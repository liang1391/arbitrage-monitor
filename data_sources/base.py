"""Deal dataclass and abstract base fetcher."""

from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional


@dataclass
class Deal:
    """Normalized deal/price data from any source."""
    source_id: str
    title: str
    price: float
    original_price: float = 0.0
    platform: str = ""
    url: str = ""
    worthy_count: int = 0
    unworthy_count: int = 0
    timestamp: datetime = field(default_factory=datetime.now)
    channel: str = ""
    description: str = ""

    @property
    def worthy_ratio(self) -> float:
        total = self.worthy_count + self.unworthy_count
        if total == 0:
            return 0.0
        return (self.worthy_count / total) * 100

    def summary(self) -> str:
        return (
            f"[{self.channel or '好价'}] {self.title}  "
            f"¥{self.price:.1f}"
            f"{f' (原价¥{self.original_price:.1f})' if self.original_price else ''}"
            f" — {self.platform}"
            f"{f' [{self.worthy_ratio:.0f}%值/{self.worthy_count}票]' if self.worthy_count else ''}"
        )


class BaseFetcher(ABC):
    """Abstract base for deal data fetchers."""

    @abstractmethod
    def fetch(self) -> list[Deal]:
        """Fetch and return normalized deals."""

    @abstractmethod
    def is_available(self) -> bool:
        """Check if the source is reachable."""
