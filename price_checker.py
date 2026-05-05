"""价格真实性检测 + 跨平台比价。

解决两个核心问题：
1. 商家先涨后降：声称原价¥200现价¥50，但该商品日常价就是¥50
2. 跨平台比价：京东打完折¥18，但拼多多同款日常¥15 — 不是真优惠
"""

import re
import logging
from collections import defaultdict
from typing import Optional

from data_sources.base import Deal

logger = logging.getLogger(__name__)


class PriceChecker:
    """Validates deal authenticity and performs cross-platform comparison."""

    def __init__(self, deal_history, config: dict):
        self.history = deal_history
        self.config = config

    def check_authenticity(self, deal: Deal) -> tuple[bool, str]:
        """Check if a deal's claimed discount is authentic.

        Returns (is_authentic, reason).
        Red flags:
        - No original price data available
        - Original price much higher than historical "original prices" for this product
        - Historical price data shows this "deal" price is actually normal
        """
        if deal.original_price <= 0:
            return True, ""  # No claimed discount, can't detect fake

        # Get price history from our DB
        price_records = self.history.get_price_history(deal.source_id)
        if len(price_records) < 3:
            return True, ""  # Not enough data to judge

        # Check if current price is actually abnormal (higher than historical)
        historical_prices = [r["price"] for r in price_records[:-1]]  # exclude current
        if historical_prices:
            min_hist = min(historical_prices)
            avg_hist = sum(historical_prices) / len(historical_prices)

            # If current "deal" price is HIGHER than historical minimum,
            # the discount is fake (price went up then "discounted" back)
            if deal.price > min_hist * 1.05:
                return False, f"疑似假折扣：历史最低¥{min_hist:.1f}，现价¥{deal.price:.1f}"

            # If current price is close to historical average, discount is misleading
            if abs(deal.price - avg_hist) / avg_hist < 0.10:
                return False, f"疑似假折扣：价格接近历史均价¥{avg_hist:.1f}"

        return True, ""

    def cross_platform_check(self, deal: Deal, all_deals: list[Deal]) -> tuple[bool, str]:
        """Compare this deal's price against the same product on other platforms.

        Returns (is_best_price, reason).
        """
        normalized = self._normalize_name(deal.title)

        # Find same product on other platforms
        same_product: dict[str, list[Deal]] = defaultdict(list)
        for d in all_deals:
            if d.source_id == deal.source_id:
                continue
            d_norm = self._normalize_name(d.title)
            if self._is_same_product(normalized, d_norm):
                same_product[d.platform].append(d)

        if not same_product:
            return True, ""  # No comparison data available

        # Compare prices
        cheaper_platforms = []
        for platform, pdeals in same_product.items():
            min_price = min(d.price for d in pdeals)
            if min_price < deal.price * 0.95:  # 5%+ cheaper elsewhere
                cheaper_platforms.append((platform, min_price))

        if cheaper_platforms:
            info = "; ".join(f"{p}¥{pr:.1f}" for p, pr in cheaper_platforms[:3])
            return False, f"其他平台更便宜：{info}"

        return True, ""

    def find_platform_comparisons(self, matched_deals: list[Deal], all_deals: list[Deal]) -> list[dict]:
        """For matched deals, find same-product listings on other platforms.

        Returns list of comparison dicts for the alert message.
        """
        comparisons = []

        # Build index: normalized_name → list of (platform, price, title)
        product_index: dict[str, list[dict]] = defaultdict(list)
        for d in all_deals:
            norm = self._normalize_name(d.title)
            if norm:
                product_index[norm].append({
                    "platform": d.platform or "未知",
                    "price": d.price,
                    "title": d.title[:60],
                    "url": d.url,
                })

        # For each matched deal, find cross-platform entries
        for deal in matched_deals:
            norm = self._normalize_name(deal.title)
            all_listings = product_index.get(norm, [])
            # Separate current platform vs others
            same_platform = [l for l in all_listings if l["platform"] == deal.platform]
            other_platforms = [l for l in all_listings if l["platform"] != deal.platform]

            if other_platforms:
                comparisons.append({
                    "deal": deal,
                    "same_platform_count": len(same_platform),
                    "other_listings": other_platforms,
                    "cheapest_other": min(other_platforms, key=lambda x: x["price"]),
                    "is_truly_cheapest": deal.price <= min(l["price"] for l in other_platforms) * 1.05,
                })

        return comparisons

    @staticmethod
    def _normalize_name(title: str) -> str:
        """Normalize product title to extract core product identity.

        Removes platform prefixes (京东百亿补贴, 88VIP, etc.), coupon notes,
        and keeps brand + product type.
        """
        # Remove common prefixes
        prefixes = [
            "移动端、", "京东百亿补贴：", "百亿补贴：", "PLUS会员：",
            "88VIP：", "移动端：", "有券的上：", "国家补贴：",
            "今日必买：", "值友专享、", "值友专享：", "限地区：",
            "京东百亿补贴、限地区：",
        ]
        name = title
        for p in prefixes:
            if name.startswith(p):
                name = name[len(p):]

        # Remove quantity/spec info in parentheses
        name = re.sub(r"[（(][^)）]*[)）]", "", name)
        # Remove "多款任选" "多色任选" etc.
        name = re.sub(r"多[款色码].*", "", name)
        # Normalize spaces
        name = re.sub(r"\s+", " ", name).strip()

        # Extract key tokens for fuzzy matching (first 6 chars after cleanup)
        if len(name) >= 4:
            return name[:20]  # First 20 chars captures brand + product
        return name

    @staticmethod
    def _is_same_product(name1: str, name2: str) -> bool:
        """Check if two normalized names refer to the same product."""
        if not name1 or not name2:
            return False
        # Exact match on normalized name
        if name1 == name2:
            return True
        # One contained in the other
        if len(name1) >= 6 and len(name2) >= 6:
            if name1[:10] == name2[:10]:
                return True
        return False

    def get_price_history_insight(self, deal: Deal) -> str:
        """Get price history insight for alert display."""
        records = self.history.get_price_history(deal.source_id)
        if len(records) < 2:
            return "首次监控，暂无历史数据"

        prices = [r["price"] for r in records]
        min_p = min(prices)
        max_p = max(prices)
        avg_p = sum(prices) / len(prices)
        current = prices[-1]

        if current <= min_p * 1.02:
            return f"✅ 历史最低价（监控{len(records)}次，最低¥{min_p:.1f}）"
        elif current <= avg_p:
            return f"📉 低于均价¥{avg_p:.1f}（历史¥{min_p:.1f}~¥{max_p:.1f}）"
        else:
            return f"⚠️ 高于历史最低¥{min_p:.1f}，可能不是真优惠"


def compute_platform_stats(deals: list[Deal]) -> dict:
    """Compute platform distribution statistics."""
    stats: dict[str, dict] = defaultdict(lambda: {"count": 0, "total_price": 0.0, "deals": []})
    for d in deals:
        p = d.platform or "未知"
        stats[p]["count"] += 1
        stats[p]["total_price"] += d.price
        stats[p]["deals"].append(d)
    return dict(stats)
