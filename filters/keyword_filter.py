"""关键词 + 价格阈值 + 折扣力度 + 值率过滤引擎."""

import logging
from data_sources.base import Deal

logger = logging.getLogger(__name__)


class KeywordFilter:
    """Filters deals by keywords, price thresholds, discount, and community votes."""

    def __init__(self, config: dict):
        fc = config.get("filters", {})
        pt = config.get("price_thresholds", {})

        self.keywords: list[str] = [kw.lower() for kw in fc.get("keywords", [])]
        self.min_worthy_ratio: float = float(fc.get("min_worthy_ratio", 60))
        self.min_worthy_count: int = int(fc.get("min_worthy_count", 5))
        self.global_max: float = float(pt.get("global_max", 30))
        self.min_discount_pct: float = float(fc.get("min_discount_pct", 30))

        # Per-keyword price caps (yuan), keyed by lowercase keyword
        self.keyword_prices: dict[str, float] = {
            kw.lower(): float(price)
            for kw, price in pt.get("keywords", {}).items()
        }

    def should_alert(self, deal: Deal) -> tuple[bool, str]:
        """Returns (should_alert, reason_text)."""
        # Must pass community worthiness
        if not self._passes_worthy(deal):
            return False, "值率不足"

        matched_kw = self._match_keyword(deal)
        below_global = deal.price < self.global_max

        # Discount check
        discount = self._get_discount(deal)
        is_big_discount = discount >= self.min_discount_pct

        # Decision logic:
        # 1. Keyword match + big discount → alert
        # 2. Keyword match + below global → alert
        # 3. Below global + big discount → alert
        # 4. Big discount alone (no keyword, not below global) → alert (rare large discount)
        # 5. Below global alone → alert

        if matched_kw:
            kw_lower = matched_kw.lower()
            if kw_lower in self.keyword_prices and deal.price > self.keyword_prices[kw_lower]:
                return False, f"超过「{matched_kw}」价格上限 ¥{self.keyword_prices[kw_lower]}"
            if is_big_discount:
                return True, f"关键词「{matched_kw}」+ 折扣{discount:.0f}%"
            if below_global:
                return True, f"关键词「{matched_kw}」+ 低于¥{self.global_max:.0f}"
            return False, f"关键词匹配但折扣不够({discount:.0f}%<{self.min_discount_pct:.0f}%)"

        if below_global and is_big_discount:
            return True, f"低于¥{self.global_max:.0f} + 折扣{discount:.0f}%"

        if is_big_discount:
            deal_price = deal.original_price if deal.original_price > 0 else deal.price * 2
            return True, f"大额折扣 {discount:.0f}%（¥{deal.price:.0f}/¥{deal_price:.0f}）"

        # Only push "just cheap" items if we can't calculate discount (no original price data)
        if below_global and deal.original_price == 0:
            return True, f"低价商品 ¥{deal.price:.0f}"

        return False, f"折扣{discount:.0f}%不够（需>{self.min_discount_pct:.0f}%）"

    def _match_keyword(self, deal: Deal) -> str:
        """Return the first matching keyword, or empty string."""
        text = (deal.title + " " + deal.description).lower()
        for kw in self.keywords:
            if kw in text:
                return kw
        return ""

    def _passes_worthy(self, deal: Deal) -> bool:
        """Check community vote thresholds.

        If there are no votes yet, pass by default (new deal, no data).
        Only filter when there are enough votes to be meaningful.
        """
        total = deal.worthy_count + deal.unworthy_count
        if total == 0:
            return True  # No votes yet, give it a chance
        if total < 3:
            return True  # Too few votes to be statistically meaningful
        if self.min_worthy_ratio > 0 and deal.worthy_ratio < self.min_worthy_ratio:
            return False
        return True

    def _get_discount(self, deal: Deal) -> float:
        """Calculate discount percentage for this deal."""
        if deal.original_price > 0 and deal.original_price > deal.price:
            return round((deal.original_price - deal.price) / deal.original_price * 100, 1)
        return 0.0

    def match_info(self, deal: Deal) -> str:
        """Get a human-readable match reason."""
        matched_kw = self._match_keyword(deal)
        below_global = deal.price < self.global_max
        discount = self._get_discount(deal)

        parts = []
        if matched_kw:
            parts.append(f"关键词「{matched_kw}」")
        if discount >= self.min_discount_pct:
            parts.append(f"折扣{discount:.0f}%")
        if below_global:
            parts.append(f"低于 ¥{self.global_max:.0f}")
        return ", ".join(parts) if parts else "不符合规则"
