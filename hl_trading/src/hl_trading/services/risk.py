"""Pre-trade gates — keep deterministic and fast (no I/O)."""

from __future__ import annotations

from typing import Any, Protocol

from hl_trading.config import Settings
from hl_trading.domain import LimitOrderIntent, PortfolioView


class RiskGate(Protocol):
    def check_new_order(self, p: PortfolioView, intent: LimitOrderIntent, mid_px: float | None) -> None: ...


class NotionalLimitRisk:
    """Blocks orders when caps would be exceeded (conservative USD approx using limit_px or mid)."""

    def __init__(self, settings: Settings) -> None:
        self._max_pos = settings.max_position_usd_per_coin
        self._max_order = settings.max_order_notional_usd

    def _open_order_signed_size(self, raw_order: Any, coin: str) -> float:
        if not isinstance(raw_order, dict):
            return 0.0
        order = raw_order.get("order")
        if isinstance(order, dict):
            raw_order = order
        if str(raw_order.get("coin", "")) != coin:
            return 0.0
        if bool(raw_order.get("reduceOnly")):
            return 0.0
        side_raw = str(raw_order.get("side", "")).lower()
        try:
            size = abs(float(raw_order.get("sz", 0.0)))
        except (TypeError, ValueError):
            return 0.0
        # Hyperliquid open orders use "B" / "A" (see Info.open_orders docs)
        if side_raw in ("buy", "b"):
            return size
        if side_raw in ("sell", "a", "ask"):
            return -size
        return 0.0

    def _pending_position_delta(self, p: PortfolioView, coin: str) -> float:
        raw = p.raw if isinstance(p.raw, dict) else {}
        open_orders = raw.get("openOrders")
        if not isinstance(open_orders, list):
            return 0.0
        return sum(self._open_order_signed_size(row, coin) for row in open_orders)

    def check_new_order(self, p: PortfolioView, intent: LimitOrderIntent, mid_px: float | None) -> None:
        px = intent.limit_px if intent.limit_px > 0 else (mid_px or 0.0)
        if px <= 0:
            raise RiskViolation("cannot evaluate notional: no price")
        order_usd = abs(intent.size) * px
        if self._max_order is not None and order_usd > self._max_order:
            raise RiskViolation(f"order notional {order_usd:.2f} > max_order_notional_usd {self._max_order}")
        if self._max_pos is None:
            return
        cur = float(p.positions.get(intent.coin, 0.0))
        pending_delta = self._pending_position_delta(p, intent.coin)
        signed_delta = intent.size if intent.side == "buy" else -intent.size
        new_pos = cur + pending_delta + signed_delta
        projected_usd = abs(new_pos) * px
        if projected_usd > self._max_pos:
            raise RiskViolation(
                f"position notional after trade ~{projected_usd:.2f} > max_position_usd_per_coin {self._max_pos}"
            )


class RiskViolation(Exception):
    pass
