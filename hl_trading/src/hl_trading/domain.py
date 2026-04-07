"""Core domain types — strategy-agnostic order intents and portfolio view."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

Side = Literal["buy", "sell"]


@dataclass(frozen=True, slots=True)
class LimitOrderIntent:
    """Portable limit order request (mapped to Hyperliquid `Exchange.order`)."""

    coin: str
    side: Side
    size: float
    limit_px: float
    reduce_only: bool = False
    client_order_id_hex: str | None = None
    tif: Literal["Gtc", "Ioc", "Alo"] = "Gtc"


@dataclass(slots=True)
class PortfolioView:
    """Reduced account state for risk checks and strategy inputs."""

    account_address: str
    margin_summary: dict[str, Any] = field(default_factory=dict)
    positions: dict[str, float] = field(default_factory=dict)
    open_orders_count: int = 0
    raw: dict[str, Any] | None = None
