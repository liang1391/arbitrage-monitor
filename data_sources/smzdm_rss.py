"""什么值得买 RSS 抓取器 — 回退数据源。

当 JSON API 不可用时，通过 RSS feed 获取数据。
RSS URL: https://www.smzdm.com/feed
"""

import re
import logging
from datetime import datetime
from typing import Optional

import feedparser

from .base import BaseFetcher, Deal

logger = logging.getLogger(__name__)


class SMZDMRssFetcher(BaseFetcher):
    """Fetches deals from SMZDM RSS feed (fallback)."""

    RSS_URL = "https://www.smzdm.com/feed"

    def __init__(self, config: dict):
        self.config = config
        self.url = config.get("rss_url", self.RSS_URL)

    def is_available(self) -> bool:
        try:
            feed = feedparser.parse(self.url)
            return len(feed.entries) > 0
        except Exception:
            return False

    def fetch(self) -> list[Deal]:
        try:
            feed = feedparser.parse(self.url, agent=self._user_agent())
            if not feed.entries:
                logger.warning("SMZDM RSS: no entries found")
                return []
            deals = []
            for entry in feed.entries:
                deal = self._parse_entry(entry)
                if deal:
                    deals.append(deal)
            logger.info("SMZDM RSS: fetched %d deals", len(deals))
            return deals
        except Exception:
            logger.error("SMZDM RSS fetch failed", exc_info=True)
            return []

    def _parse_entry(self, entry) -> Optional[Deal]:
        """Parse an RSS entry into a Deal."""
        eid = entry.get("id", "")
        if not eid:
            return None

        title = entry.get("title", "").strip()
        if not title:
            return None

        # Extract price from title (e.g. "... ¥199 ...", "... 29.9元 ...")
        price = self._extract_price(title)
        if price <= 0:
            return None

        # Try to find platform in the summary
        summary = entry.get("summary", "")
        platform = self._extract_platform(summary)

        link = entry.get("link", "")

        # Parse published date
        published = entry.get("published", "")
        try:
            ts = datetime(*entry.get("published_parsed", datetime.now().timetuple())[:6])
        except Exception:
            ts = datetime.now()

        return Deal(
            source_id=eid,
            title=title,
            price=price,
            original_price=0.0,
            platform=platform,
            url=link,
            timestamp=ts,
            channel="好价",
            description=self._strip_html(summary)[:200],
        )

    @staticmethod
    def _extract_price(text: str) -> float:
        """Extract price from text like '... ¥199 ...' or '... 29.9元 ...'."""
        patterns = [
            r"[¥￥]\s*(\d+\.?\d*)",     # ¥199 or ￥199
            r"(\d+\.?\d*)\s*元",        # 29.9元
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return float(match.group(1))
        return 0.0

    @staticmethod
    def _extract_platform(text: str) -> str:
        """Try to identify the e-commerce platform from text."""
        platforms = ["京东", "天猫", "淘宝", "拼多多", "苏宁", "国美", "唯品会", "亚马逊"]
        for p in platforms:
            if p in text:
                return p
        return ""

    @staticmethod
    def _strip_html(text: str) -> str:
        """Remove HTML tags from text."""
        return re.sub(r"<[^>]+>", "", text)

    @staticmethod
    def _user_agent() -> str:
        return (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
