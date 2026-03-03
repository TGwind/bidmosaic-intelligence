"""Intelligence Item schema - 所有管线的统一数据格式契约."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any


@dataclass
class MarketData:
    symbols: list[str] = field(default_factory=list)
    price_change: dict[str, Any] = field(default_factory=dict)
    indicators: dict[str, Any] = field(default_factory=dict)


@dataclass
class IntelligenceItem:
    # 采集管线填充
    source_pipeline: str  # rss_tech | stock_analysis | hedge_analysis
    raw_title: str
    raw_content: str
    source_url: str
    source_name: str
    data_type: str = "news"  # news | report | market_data | analysis | opinion
    domain: str = ""  # tech | finance | ecommerce | policy | crypto | ai
    collected_at: str = ""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    # AI 处理层填充
    generated_title: str = ""
    generated_summary: str = ""
    generated_analysis: str = ""
    tags: list[str] = field(default_factory=list)
    importance_score: int = 0
    processed_at: str = ""

    # 领域专有（可选）
    market_data: MarketData = field(default_factory=MarketData)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.collected_at:
            self.collected_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> IntelligenceItem:
        md = data.pop("market_data", {})
        if isinstance(md, dict):
            md = MarketData(**md)
        return cls(market_data=md, **data)
