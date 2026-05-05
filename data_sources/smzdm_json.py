"""什么值得买 好价抓取器 — 使用 Playwright + 系统 Edge 浏览器穿透反爬。

解析 youhui/baicai 页面的 HTML DOM，提取 Deal 数据，包括：
- 商品标题、现价、原价
- 折扣百分比（从描述文本提取原始售价计算）
- 平台、值得率、链接
"""

import re
import logging
import time
from datetime import datetime
from typing import Optional

from .base import BaseFetcher, Deal

logger = logging.getLogger(__name__)

CHANNEL_URLS = {
    "youhui": "https://www.smzdm.com/youhui/",
    "baicai": "https://www.smzdm.com/baicai/",
}


class SMZDMJsonFetcher(BaseFetcher):
    """Fetches deals from SMZDM using Playwright browser + DOM parsing."""

    def __init__(self, config: dict):
        self.config = config
        self.scrolls = config.get("scrolls", 5)
        self.delay = config.get("request_delay", 2.0)
        self.channels = config.get("channels", ["youhui", "baicai"])
        self._pw = None
        self._browser = None
        self._context = None

    def is_available(self) -> bool:
        try:
            from playwright.sync_api import sync_playwright
            from playwright_stealth import Stealth
            return True
        except ImportError:
            return False

    def open_browser(self):
        """Open a persistent browser session for reuse across operations."""
        if self._context is not None:
            return
        from playwright.sync_api import sync_playwright
        from playwright_stealth import Stealth

        stealth = Stealth()
        self._pw = sync_playwright().start()
        try:
            self._browser = self._pw.chromium.launch(channel="msedge", headless=True)
        except Exception:
            logger.warning("Edge not found, trying default Chromium")
            self._browser = self._pw.chromium.launch(headless=True)

        self._context = self._browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="zh-CN",
            viewport={"width": 1920, "height": 1080},
        )
        stealth.apply_stealth_sync(self._context)
        logger.info("Browser session opened")

    def close_browser(self):
        """Close the persistent browser session."""
        if self._context:
            try:
                self._context.close()
            except Exception:
                pass
            self._context = None
        if self._browser:
            try:
                self._browser.close()
            except Exception:
                pass
            self._browser = None
        if self._pw:
            try:
                self._pw.stop()
            except Exception:
                pass
            self._pw = None
        logger.info("Browser session closed")

    def fetch(self) -> list[Deal]:
        try:
            own_browser = self._context is None
            if own_browser:
                self.open_browser()
            deals = self._fetch_channels(self._context)
            if own_browser:
                self.close_browser()
            return deals
        except Exception:
            logger.exception("Playwright fetch failed")
            return []

    def _fetch_channels(self, context) -> list[Deal]:
        """Fetch deals from all configured channels using an existing context."""
        deals: list[Deal] = []
        seen_ids: set[str] = set()

        for channel in self.channels:
            channel_deals = self._fetch_channel(context, channel)
            for deal in channel_deals:
                if deal.source_id not in seen_ids:
                    seen_ids.add(deal.source_id)
                    deals.append(deal)
            time.sleep(self.delay)

        logger.info("SMZDM fetched %d deals from channels %s", len(deals), self.channels)
        return deals

    def fetch_cross_platform(self, product_name: str, context=None) -> list[Deal]:
        """Search SMZDM for same product across all platforms.

        Args:
            product_name: Search keywords
            context: Optional existing browser context (defaults to self._context)
        """
        try:
            ctx = context or self._context
            return self._search_product(product_name, ctx)
        except Exception:
            logger.exception("Cross-platform search failed for: %s", product_name)
            return []

    def fetch_platform_deals(self, platform: str) -> list[Deal]:
        """Search SMZDM for deals from a specific platform to increase diversity.

        Uses the platform name (e.g. '拼多多', '淘宝') as a search term
        to find deals that mention this platform in their description.
        """
        ctx = self._context
        if not ctx:
            return []
        try:
            import urllib.parse
            search_url = (
                "https://search.smzdm.com/?c=youhui&s="
                + urllib.parse.quote(platform)
                + "&v=b&order=time"
            )
            page = ctx.new_page()
            try:
                page.goto(search_url, wait_until="networkidle", timeout=15000)
                page.wait_for_timeout(1000)
                deals = self._parse_feed_blocks(page, f"平台-{platform}")
            finally:
                page.close()
            logger.debug("Platform '%s': %d deals", platform, len(deals))
            return deals
        except Exception as e:
            logger.debug("Platform search '%s' failed: %s", platform, e)
            return []

    def _search_product(self, query: str, reuse_context=None) -> list[Deal]:
        from playwright.sync_api import sync_playwright
        from playwright_stealth import Stealth
        import urllib.parse

        search_url = f"https://search.smzdm.com/?c=youhui&s={urllib.parse.quote(query)}&v=b"
        deals: list[Deal] = []

        ctx = reuse_context or self._context
        if ctx:
            page = ctx.new_page()
            try:
                page.goto(search_url, wait_until="networkidle", timeout=15000)
                page.wait_for_timeout(1000)
                deals = self._parse_feed_blocks(page, "搜索")
            except Exception as e:
                logger.debug("Search '%s' failed: %s", query[:20], e)
            finally:
                page.close()
            return deals

        # Slow path: new browser (fallback)
        stealth = Stealth()
        with sync_playwright() as pw:
            try:
                browser = pw.chromium.launch(channel="msedge", headless=True)
            except Exception:
                browser = pw.chromium.launch(headless=True)

            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                locale="zh-CN",
                viewport={"width": 1920, "height": 1080},
            )
            stealth.apply_stealth_sync(context)
            page = context.new_page()
            try:
                page.goto(search_url, wait_until="networkidle", timeout=15000)
                page.wait_for_timeout(1000)
                deals = self._parse_feed_blocks(page, "搜索")
            except Exception as e:
                logger.debug("Search '%s' failed: %s", query[:20], e)
            finally:
                page.close()
                context.close()
                browser.close()

        return deals

    def _fetch_channel(self, context, channel: str) -> list[Deal]:
        """Navigate to a channel page, scroll to load deals, extract from DOM."""
        from playwright.sync_api import Error as PlaywrightError

        url = CHANNEL_URLS.get(channel, CHANNEL_URLS["youhui"])
        page = context.new_page()

        deals: list[Deal] = []
        try:
            page.goto(url, wait_until="networkidle", timeout=30000)
            page.wait_for_timeout(2000)

            for i in range(self.scrolls):
                new_deals = self._parse_feed_blocks(page, channel)
                added = 0
                for d in new_deals:
                    if d.source_id not in {x.source_id for x in deals}:
                        deals.append(d)
                        added += 1
                if added == 0 and i > 2:
                    break  # no more new content
                # Scroll to load more
                page.evaluate("window.scrollBy(0, 2000)")
                page.wait_for_timeout(1500)
        except PlaywrightError as e:
            logger.warning("Page load issue for %s: %s", channel, e)
        finally:
            page.close()

        logger.debug("Channel '%s': %d deals", channel, len(deals))
        return deals

    def _parse_feed_blocks(self, page, channel: str) -> list[Deal]:
        """Extract deals from .feed-block elements on the current page."""
        blocks = page.query_selector_all(".feed-block")
        deals = []
        for block in blocks:
            deal = self._parse_block(block, channel)
            if deal:
                deals.append(deal)
        return deals

    def _parse_block(self, block, channel: str) -> Optional[Deal]:
        """Parse a single .feed-block DOM element into a Deal."""
        try:
            text = block.inner_text()
        except Exception:
            return None

        # ── Title & URL ──
        p_links = block.query_selector_all("a[href*='/p/']")
        if not p_links:
            return None

        title = ""
        url = ""
        price_pattern = re.compile(r"^\d+\.?\d*\s*元")

        for link in p_links:
            link_text = link.inner_text().strip()
            link_url = link.get_attribute("href") or ""
            if not link_text:
                continue
            if price_pattern.match(link_text):
                continue
            if link_text in ("阅读全文", "去购买", "直达链接"):
                continue
            if len(link_text) > len(title):
                title = link_text
                url = link_url

        if not title or len(title) < 3:
            return None

        # Skip coupon/red-packet ad posts (not real product deals)
        _skip_prefixes = (
            "淘宝 每天", "今日好券", "周一好券", "周二好券", "周三好券",
            "周四好券", "周五好券", "周六好券", "周日好券",
            "淘宝 领券", "淘宝 88VIP", "淘宝-首页",
            "淘宝闪购", "京东 领券",
        )
        if title.startswith(_skip_prefixes):
            return None

        match = re.search(r"/p/(\d+)", url)
        if not match:
            return None
        source_id = match.group(1)
        url = f"https://www.smzdm.com/p/{source_id}/"

        # ── Current price ──
        price = self._extract_price_from_text(text)
        if price <= 0:
            return None

        # ── Original price ──
        # 1) DOM: strikethrough / old-price / market-price elements
        original_price = 0.0
        del_elements = block.query_selector_all(
            "del, s, strike, "
            "[class*='original'], [class*='old-price'], [class*='oldPrice'], "
            "[class*='list-price'], [class*='market'], [class*='retail'], "
            "[class*='origPrice'], [class*='prime-cost']"
        )
        for el in del_elements:
            el_text = el.inner_text().strip()
            m = re.search(r"(\d+\.?\d*)\s*元?", el_text)
            if m:
                op = float(m.group(1))
                if op > price:
                    original_price = op
                    break

        # 2) Text patterns + heuristic fallback
        if original_price <= 0:
            original_price = self._extract_original_price(text, price)

        # ── Discount percentage ──
        discount_pct = 0.0
        if original_price > 0 and original_price > price:
            discount_pct = round((original_price - price) / original_price * 100, 1)
        else:
            # Try to read discount from text: "低XX%", "比上次发布低XX%"
            discount_pct = self._extract_discount_pct(text)

        # ── Discount tags ──
        tags = []
        if "低于常卖价" in text:
            tags.append("低于常卖价")
        if "新低" in text:
            m = re.search(r"(\d+)天新低", text)
            if m:
                tags.append(f"{m.group(1)}天新低")
            else:
                tags.append("历史新低")
        if "百亿补贴" in text:
            tags.append("百亿补贴")
        if "比上次发布低" in text:
            m = re.search(r"比上次发布低(\d+%)", text)
            if m:
                tags.append(f"比上次低{m.group(1)}")

        # ── Platform ──
        mall_link = block.query_selector("a[href*='/mall/']")
        platform = mall_link.inner_text().strip() if mall_link else ""

        # Fallback: extract platform from description text (needed for search result pages)
        if not platform:
            platform = self._extract_platform_from_text(text, title)

        # ── Worthy/Unworthy ──
        worthy = 0
        unworthy = 0
        vote_spans = block.query_selector_all("[class*='vote'], [class*='worth'], [class*='unworth']")
        if vote_spans:
            for span in vote_spans:
                t = span.inner_text().strip()
                if t.isdigit():
                    worthy = int(t)
                    break

        vote_match = re.findall(r"(\d+)\s+(\d+)\s+(\d+)", text)
        if vote_match and not worthy:
            w, uw, _ = vote_match[0]
            worthy = int(w)
            unworthy = int(uw)

        # ── Description ──
        description = ", ".join(tags) if tags else ""

        channel_name = {"youhui": "好价", "baicai": "白菜"}.get(channel, channel)

        return Deal(
            source_id=source_id,
            title=title,
            price=round(price, 2),
            original_price=round(original_price, 2),
            platform=platform,
            url=url,
            worthy_count=worthy,
            unworthy_count=unworthy,
            timestamp=datetime.now(),
            channel=channel_name,
            description=description,
        )

    # ── Price Extraction Helpers ──

    @staticmethod
    def _extract_price_from_text(text: str) -> float:
        """Extract the deal price from block text — the lowest real price before '阅读全文'."""
        if not text:
            return 0.0

        boundary = text.find("阅读全文")
        if boundary > 0:
            text = text[:boundary]

        coupon_keywords = ["立减", "满减", "满\\d+减", "享\\d+折", "返\\d+", "打\\d+折"]

        line_prices: list[float] = []
        for line in text.split("\n"):
            line = line.strip()
            if not line:
                continue
            if any(re.search(kw, line) for kw in coupon_keywords):
                continue
            m = re.search(r"(\d+\.?\d*)\s*元", line)
            if m:
                p = float(m.group(1))
                if p >= 0.5:
                    line_prices.append(p)

        return min(line_prices) if line_prices else 0.0

    @staticmethod
    def _extract_original_price(text: str, deal_price: float = 0.0) -> float:
        """Extract the original/list price from deal description.

        Tries text patterns first, then falls back to the highest price
        mentioned in text that is plausibly the original (higher than deal).
        """
        patterns = [
            r"(?:目前|当前)?(?:活动)?售[价价]\s*[:：]?\s*(\d+\.?\d*)\s*元",
            r"日常价\s*(\d+\.?\d*)\s*元",
            r"原价\s*[:：]?\s*(\d+\.?\d*)\s*元",
            r"平常价\s*(\d+\.?\d*)\s*元",
            r"市场价\s*(\d+\.?\d*)\s*元",
            r"参考价\s*(\d+\.?\d*)\s*元",
            r"建议零售价\s*(\d+\.?\d*)\s*元",
            r"指导价\s*(\d+\.?\d*)\s*元",
            r"标价\s*(\d+\.?\d*)\s*元",
            r"吊牌价\s*(\d+\.?\d*)\s*元",
            r"官方价\s*(\d+\.?\d*)\s*元",
        ]
        for pattern in patterns:
            m = re.search(pattern, text)
            if m:
                return float(m.group(1))

        # Heuristic fallback: scan all price-like numbers, pick the highest
        # that is > deal_price but ≤ 10× deal_price (sanity cap)
        if deal_price > 0:
            all_prices = re.findall(r"(\d+\.?\d*)\s*元", text)
            candidates = []
            for p_str in all_prices:
                p = float(p_str)
                if p > deal_price and p <= deal_price * 10:
                    candidates.append(p)
            if candidates:
                return max(candidates)

        return 0.0

    @staticmethod
    def _extract_discount_pct(text: str) -> float:
        """Extract discount percentage from text tags like '低26%', '比上次发布低30%'."""
        patterns = [
            r"比上次发布低(\d+)%",
            r"低(\d+)%",
        ]
        for pattern in patterns:
            m = re.search(pattern, text)
            if m:
                return float(m.group(1))
        return 0.0

    @staticmethod
    def _extract_platform_from_text(text: str, title: str = "") -> str:
        """Extract platform name from text when DOM mall link is missing."""
        combined = title + " " + text
        platform_map = [
            (r"拼多多", "拼多多"),
            (r"京东", "京东"),
            (r"天猫精选", "天猫精选"),
            (r"天猫超市", "天猫超市"),
            (r"天猫", "天猫"),
            (r"淘宝", "淘宝"),
            (r"抖音", "抖音"),
            (r"美团", "美团"),
            (r"得物", "得物"),
            (r"苏宁易购", "苏宁易购"),
            (r"苏宁", "苏宁"),
        ]
        for pattern, name in platform_map:
            if pattern in combined:
                return name
        return ""
