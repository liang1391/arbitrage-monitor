"""SQLite 去重 + 价格历史追踪."""

import sqlite3
import logging
import os
from datetime import datetime, timedelta
from data_sources.base import Deal

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS seen_deals (
    source_id TEXT PRIMARY KEY,
    title TEXT,
    price REAL,
    platform TEXT,
    url TEXT,
    worthy_count INTEGER DEFAULT 0,
    unworthy_count INTEGER DEFAULT 0,
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    lowest_price REAL,
    alert_count INTEGER DEFAULT 0,
    last_alerted TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS price_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id TEXT,
    price REAL,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (source_id) REFERENCES seen_deals(source_id)
);

CREATE INDEX IF NOT EXISTS idx_seen_last_alerted ON seen_deals(last_alerted);
CREATE INDEX IF NOT EXISTS idx_price_source ON price_history(source_id);
"""


class DealHistory:
    """Manages deal deduplication and price history in SQLite."""

    def __init__(self, config: dict):
        dc = config.get("dedup", {})
        self.db_path: str = dc.get("db_path", "data/deals.db")
        self.cooldown_sec: int = int(dc.get("cooldown_sec", 86400))
        self.price_drop_pct: float = float(dc.get("price_drop_pct", 5))

        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        self._init_db()

    def _init_db(self):
        with self._connect() as conn:
            conn.executescript(SCHEMA)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def should_alert(self, deal: Deal) -> tuple[bool, str]:
        """Returns (should_alert, reason).

        - New deal → alert
        - Price dropped >= price_drop_pct vs last_alerted → alert
        - Cooldown expired → alert
        - Otherwise → skip
        """
        with self._connect() as conn:
            row = conn.execute(
                "SELECT price, lowest_price, last_alerted, alert_count FROM seen_deals WHERE source_id = ?",
                (deal.source_id,),
            ).fetchone()

            if row is None:
                return True, "新品上线"

            prev_price, lowest_price, last_alerted_str, alert_count = row

            # Parse last_alerted
            if last_alerted_str:
                try:
                    last_alerted = datetime.fromisoformat(last_alerted_str)
                except ValueError:
                    last_alerted = datetime.min
            else:
                last_alerted = datetime.min

            # Price drop check (relative to lowest recorded price)
            if lowest_price and lowest_price > 0:
                drop = (lowest_price - deal.price) / lowest_price * 100
                if drop >= self.price_drop_pct:
                    return True, f"价格创新低 (跌 {drop:.0f}%)"

            # Cooldown check
            if datetime.now() - last_alerted > timedelta(seconds=self.cooldown_sec):
                return True, "冷却期已过"

            return False, "已推送过"

    def record_alert(self, deal: Deal):
        """Record that we alerted for this deal, and save price history."""
        with self._connect() as conn:
            now = datetime.now().isoformat()
            existing = conn.execute(
                "SELECT lowest_price, alert_count FROM seen_deals WHERE source_id = ?",
                (deal.source_id,),
            ).fetchone()

            if existing:
                new_lowest = min(existing[0] or deal.price, deal.price)
                new_count = existing[1] + 1
                conn.execute(
                    """UPDATE seen_deals SET
                        title=?, price=?, platform=?, url=?,
                        worthy_count=?, unworthy_count=?,
                        last_seen=?, lowest_price=?, alert_count=?, last_alerted=?
                    WHERE source_id=?""",
                    (
                        deal.title, deal.price, deal.platform, deal.url,
                        deal.worthy_count, deal.unworthy_count,
                        now, new_lowest, new_count, now,
                        deal.source_id,
                    ),
                )
            else:
                conn.execute(
                    """INSERT INTO seen_deals
                        (source_id, title, price, platform, url,
                         worthy_count, unworthy_count, first_seen, last_seen,
                         lowest_price, alert_count, last_alerted)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        deal.source_id, deal.title, deal.price, deal.platform, deal.url,
                        deal.worthy_count, deal.unworthy_count, now, now,
                        deal.price, 1, now,
                    ),
                )

            # Always record price history
            conn.execute(
                "INSERT INTO price_history (source_id, price) VALUES (?, ?)",
                (deal.source_id, deal.price),
            )

    def get_price_history(self, source_id: str, limit: int = 10) -> list[dict]:
        """Return recent price history for a deal."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT price, recorded_at FROM price_history WHERE source_id = ? "
                "ORDER BY recorded_at DESC LIMIT ?",
                (source_id, limit),
            ).fetchall()
            return [
                {"price": row[0], "recorded_at": row[1]} for row in reversed(rows)
            ]

    def record_seen(self, deal: Deal):
        """Record a deal was seen (without alerting)."""
        with self._connect() as conn:
            now = datetime.now().isoformat()
            existing = conn.execute(
                "SELECT source_id FROM seen_deals WHERE source_id = ?",
                (deal.source_id,),
            ).fetchone()
            if existing:
                conn.execute(
                    """UPDATE seen_deals SET
                        price=?, last_seen=?, worthy_count=?, unworthy_count=?
                    WHERE source_id=?""",
                    (deal.price, now, deal.worthy_count, deal.unworthy_count, deal.source_id),
                )
            else:
                conn.execute(
                    """INSERT INTO seen_deals
                        (source_id, title, price, platform, url,
                         worthy_count, unworthy_count, first_seen, last_seen, lowest_price)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        deal.source_id, deal.title, deal.price, deal.platform, deal.url,
                        deal.worthy_count, deal.unworthy_count, now, now, deal.price,
                    ),
                )
            conn.execute(
                "INSERT INTO price_history (source_id, price) VALUES (?, ?)",
                (deal.source_id, deal.price),
            )
