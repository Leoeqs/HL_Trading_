"""LIT and HYPE with the same two-sided depth front-run mechanics as SOL.

For **each** coin (separate thresholds): large **bids** → opening **buy** one tick above the wall;
large **asks** → opening **sell** one tick below the wall. Default opening clips are **5% of equity**
per order (buy and sell). By default **no 50% position throttle**: cap is set very high and pause flags
are off so the bot keeps placing opening orders even when the position is large (subject to exchange / risk limits).

**LIT** default: bid wall ≥ ``LIT_DEPTH_THRESHOLD`` (1000 LIT). **HYPE** default: wall ≥ ``HYPE_DEPTH_THRESHOLD`` (500 HYPE).

Requires ``WATCH_COINS`` to include both ``LIT`` and ``HYPE`` (and ``SUBSCRIBE_L2=true``).

Defaults use Hyperliquid meta: LIT ``szDecimals=0``, tick 0.0001; HYPE ``szDecimals=2``, tick 0.001.

Env (optional; defaults: 5% clips, both sides on, no practical position cap)::

    LIT_DEPTH_BUY_PCT=0.05
    LIT_DEPTH_POSITION_CAP_PCT=100
    LIT_DEPTH_PAUSE_BUYS_WHEN_OVER_CAP=false
    LIT_DEPTH_PAUSE_SELLS_WHEN_OVER_CAP=false

    HYPE_DEPTH_BUY_PCT=0.05
    (same pattern for ``HYPE_DEPTH_*``)
"""

from __future__ import annotations

import os
from typing import Any

from hl_trading.book.l2 import PerpL2Book
from hl_trading.domain import LimitOrderIntent, PortfolioView
from hl_trading.strategies.depth_front_run import DepthFrontRun, DepthFrontRunConfig

LIT = "LIT"
HYPE = "HYPE"


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _lit_config() -> DepthFrontRunConfig:
    th = _env_float("LIT_DEPTH_THRESHOLD", 1000.0)
    buy_pct = _env_float("LIT_DEPTH_BUY_PCT", 0.05)
    sell_raw = os.environ.get("LIT_DEPTH_SELL_PCT")
    sell_pct = float(sell_raw) if sell_raw is not None and sell_raw.strip() != "" else buy_pct
    return DepthFrontRunConfig(
        coin=LIT,
        log_name="LitDepthStrategy",
        threshold=th,
        reduce_threshold=_env_float("LIT_DEPTH_REDUCE_THRESHOLD", th),
        near_mid_usd=_env_float("LIT_DEPTH_NEAR_MID_USD", 0.1),
        tick=_env_float("LIT_DEPTH_TICK", 0.0001),
        sz_decimals=_env_int("LIT_DEPTH_SZ_DECIMALS", 0),
        buy_pct=buy_pct,
        sell_pct=sell_pct,
        sell_pos_pct=_env_float("LIT_DEPTH_SELL_POS_PCT", 0.01),
        pos_cap_pct=_env_float("LIT_DEPTH_POSITION_CAP_PCT", 100.0),
        pause_buys_when_over_cap=_env_bool("LIT_DEPTH_PAUSE_BUYS_WHEN_OVER_CAP", False),
        pause_sells_when_over_cap=_env_bool("LIT_DEPTH_PAUSE_SELLS_WHEN_OVER_CAP", False),
        max_orders=_env_int("LIT_DEPTH_MAX_ORDERS_PER_BOOK", 12),
        min_notional=_env_float("LIT_DEPTH_MIN_NOTIONAL_USD", 2.0),
        enable_opening_buys=True,
        enable_opening_sells=_env_bool("LIT_DEPTH_OPENING_SELLS", True),
        debug=_env_bool("LIT_DEPTH_DEBUG", False),
    )


def _hype_config() -> DepthFrontRunConfig:
    th = _env_float("HYPE_DEPTH_THRESHOLD", 500.0)
    buy_pct = _env_float("HYPE_DEPTH_BUY_PCT", 0.05)
    sell_raw = os.environ.get("HYPE_DEPTH_SELL_PCT")
    sell_pct = float(sell_raw) if sell_raw is not None and sell_raw.strip() != "" else buy_pct
    return DepthFrontRunConfig(
        coin=HYPE,
        log_name="HypeDepthStrategy",
        threshold=th,
        reduce_threshold=_env_float("HYPE_DEPTH_REDUCE_THRESHOLD", th),
        near_mid_usd=_env_float("HYPE_DEPTH_NEAR_MID_USD", 0.1),
        tick=_env_float("HYPE_DEPTH_TICK", 0.001),
        sz_decimals=_env_int("HYPE_DEPTH_SZ_DECIMALS", 2),
        buy_pct=buy_pct,
        sell_pct=sell_pct,
        sell_pos_pct=_env_float("HYPE_DEPTH_SELL_POS_PCT", 0.01),
        pos_cap_pct=_env_float("HYPE_DEPTH_POSITION_CAP_PCT", 100.0),
        pause_buys_when_over_cap=_env_bool("HYPE_DEPTH_PAUSE_BUYS_WHEN_OVER_CAP", False),
        pause_sells_when_over_cap=_env_bool("HYPE_DEPTH_PAUSE_SELLS_WHEN_OVER_CAP", False),
        max_orders=_env_int("HYPE_DEPTH_MAX_ORDERS_PER_BOOK", 12),
        min_notional=_env_float("HYPE_DEPTH_MIN_NOTIONAL_USD", 2.0),
        enable_opening_buys=_env_bool("HYPE_DEPTH_OPENING_BUYS", True),
        enable_opening_sells=True,
        debug=_env_bool("HYPE_DEPTH_DEBUG", False),
    )


class LitHypeDepthStrategy:
    """Runs two-sided depth front-run for LIT and HYPE (same pattern as ``SolDepthStrategy``)."""

    def __init__(self) -> None:
        self._lit = DepthFrontRun(_lit_config())
        self._hype = DepthFrontRun(_hype_config())

    def on_bbo(self, coin: str, msg: Any, portfolio: PortfolioView) -> list[LimitOrderIntent]:
        return []

    def on_user_event(self, msg: Any, portfolio: PortfolioView) -> list[LimitOrderIntent]:
        return []

    def on_l2_book(self, coin: str, book: PerpL2Book, portfolio: PortfolioView) -> list[LimitOrderIntent]:
        u = str(coin).strip().upper()
        if u == LIT:
            return self._lit.on_l2_book(coin, book, portfolio)
        if u == HYPE:
            return self._hype.on_l2_book(coin, book, portfolio)
        return []
