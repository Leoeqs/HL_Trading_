"""Portfolio snapshot from REST — perp clearinghouse, spot clearinghouse (unified accounts), open orders."""

from __future__ import annotations

import logging
from typing import Any

from hyperliquid.info import Info

from hl_trading.domain import PortfolioView

logger = logging.getLogger(__name__)


def _margin_account_value_usd(margin: dict[str, Any]) -> float:
    for key in ("accountValue", "account_value"):
        v = margin.get(key)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                continue
    return 0.0


def _spot_usdc_equity_usd(spot: dict[str, Any]) -> float:
    """Unified / portfolio-margin accounts hold collateral in spot clearinghouse (HL docs)."""
    balances = spot.get("balances") or []
    for row in balances:
        if not isinstance(row, dict):
            continue
        if str(row.get("coin", "")).upper() != "USDC":
            continue
        try:
            return float(row.get("total", 0) or 0)
        except (TypeError, ValueError):
            continue
    return 0.0


def account_equity_usd(portfolio: PortfolioView) -> float:
    """Best-effort USD equity for sizing and checks.

    Standard accounts: ``marginSummary`` / ``crossMarginSummary`` from perp ``clearinghouseState``.

    **Unified accounts:** perp ``accountValue`` can be a **subset** of total collateral while most
    USDC still shows under ``spotClearinghouseState``. Taking only the first positive margin value
    understates equity and makes ``pos_pct`` look ~100% with a small position — we use
    ``max(perp, spot_usdc)`` when both are present.
    """
    raw = portfolio.raw if isinstance(portfolio.raw, dict) else {}
    perp = 0.0
    for key in ("marginSummary", "crossMarginSummary"):
        m = raw.get(key)
        if isinstance(m, dict):
            perp = max(perp, _margin_account_value_usd(m))
    spot_v = 0.0
    spot = raw.get("spotClearinghouseState")
    if isinstance(spot, dict):
        spot_v = _spot_usdc_equity_usd(spot)
    if perp > 0.0 and spot_v > 0.0:
        return max(perp, spot_v)
    if perp > 0.0:
        return perp
    if spot_v > 0.0:
        return spot_v
    return 0.0


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
    """Merge perp ``clearinghouseState``, ``spotClearinghouseState`` (unified accounts), and ``open_orders``.

    Hyperliquid's ``user_state`` often omits resting orders — we attach ``open_orders()``.

    For **unified accounts**, balances used as perp collateral appear in **spot** clearinghouse;
    perp ``marginSummary`` may be zeros — see ``account_equity_usd``."""
    raw_in = info.user_state(account_address, dex=dex)
    raw: dict[str, Any] = dict(raw_in) if isinstance(raw_in, dict) else {}

    try:
        spot_raw = info.spot_user_state(account_address)
        if isinstance(spot_raw, dict):
            raw["spotClearinghouseState"] = spot_raw
    except Exception:
        logger.exception("spot_user_state failed")

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
