"""Portfolio snapshot from REST `user_state` — extend with WS fills later."""

from __future__ import annotations

import logging
from typing import Any

from hyperliquid.info import Info

from hl_trading.domain import PortfolioView

logger = logging.getLogger(__name__)


def _extract_positions(user_state: dict[str, Any]) -> dict[str, float]:
    out: dict[str, float] = {}
    for row in user_state.get("assetPositions") or []:
        pos = row.get("position") or {}
        coin = pos.get("coin")
        if not coin:
            continue
        try:
            out[str(coin)] = float(pos.get("szi") or 0.0)
        except (TypeError, ValueError):
            logger.warning("skip position row: %s", row)
    return out


def fetch_portfolio_view(info: Info, account_address: str, dex: str = "") -> PortfolioView:
    """Merge ``clearinghouseState`` (user_state) with the dedicated ``openOrders`` info call.

    Hyperliquid's ``user_state`` / clearinghouse payload often **does not** include resting
    orders; strategy dedup and risk need ``raw[\"openOrders\"]`` from ``open_orders()``."""
    raw_in = info.user_state(account_address, dex=dex)
    raw: dict[str, Any] = dict(raw_in) if isinstance(raw_in, dict) else {}

    try:
        oo = info.open_orders(account_address, dex=dex)
        if isinstance(oo, list):
            raw["openOrders"] = oo
    except Exception:
        logger.exception("open_orders failed; keeping any openOrders from user_state only")

    positions = _extract_positions(raw)
    margin = raw.get("marginSummary") or {}
    orders = raw.get("openOrders") or []
    return PortfolioView(
        account_address=account_address.lower(),
        margin_summary=dict(margin) if isinstance(margin, dict) else {},
        positions=positions,
        open_orders_count=len(orders) if isinstance(orders, list) else 0,
        raw=raw,
    )
