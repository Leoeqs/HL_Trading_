"""SOL perp: front-run large resting liquidity (≥1000 SOL) within $0.10 of mid.

See module docstring on ``hl_trading.strategies.depth_front_run`` / env vars below.

Hyperliquid coin symbol: ``SOL``. Defaults: tick 0.001, sz 2 dp.

Tune via environment variables (optional)::

    SOL_DEPTH_THRESHOLD_SOL=1000
    SOL_DEPTH_NEAR_MID_USD=0.1
    SOL_DEPTH_TICK=0.001
    SOL_DEPTH_SZ_DECIMALS=2
    SOL_DEPTH_BUY_PCT=0.05
    SOL_DEPTH_SELL_PCT=0.05
    SOL_DEPTH_SELL_POS_PCT=0.01
    SOL_DEPTH_POSITION_CAP_PCT=0.50
    SOL_DEPTH_PAUSE_BUYS_WHEN_OVER_CAP=true
    SOL_DEPTH_PAUSE_SELLS_WHEN_OVER_CAP=true
    SOL_DEPTH_MAX_ORDERS_PER_BOOK=12
    SOL_DEPTH_MIN_NOTIONAL_USD=2.0
    SOL_DEPTH_DEBUG=0
"""

from __future__ import annotations

import os
from typing import Any

from hl_trading.book.l2 import PerpL2Book
from hl_trading.domain import LimitOrderIntent, PortfolioView
from hl_trading.strategies.depth_front_run import DepthFrontRun, DepthFrontRunConfig

COIN = "SOL"


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


def _sol_config() -> DepthFrontRunConfig:
    th = _env_float("SOL_DEPTH_THRESHOLD_SOL", 1000.0)
    buy_pct = _env_float("SOL_DEPTH_BUY_PCT", 0.05)
    sell_raw = os.environ.get("SOL_DEPTH_SELL_PCT")
    sell_pct = float(sell_raw) if sell_raw is not None and sell_raw.strip() != "" else buy_pct
    return DepthFrontRunConfig(
        coin=COIN,
        log_name="SolDepthStrategy",
        threshold=th,
        reduce_threshold=_env_float("SOL_DEPTH_REDUCE_THRESHOLD_SOL", th),
        near_mid_usd=_env_float("SOL_DEPTH_NEAR_MID_USD", 0.1),
        tick=_env_float("SOL_DEPTH_TICK", 0.001),
        sz_decimals=_env_int("SOL_DEPTH_SZ_DECIMALS", 2),
        buy_pct=buy_pct,
        sell_pct=sell_pct,
        sell_pos_pct=_env_float("SOL_DEPTH_SELL_POS_PCT", 0.01),
        pos_cap_pct=_env_float("SOL_DEPTH_POSITION_CAP_PCT", 0.50),
        pause_buys_when_over_cap=_env_bool("SOL_DEPTH_PAUSE_BUYS_WHEN_OVER_CAP", True),
        pause_sells_when_over_cap=_env_bool("SOL_DEPTH_PAUSE_SELLS_WHEN_OVER_CAP", True),
        max_orders=_env_int("SOL_DEPTH_MAX_ORDERS_PER_BOOK", 12),
        min_notional=_env_float("SOL_DEPTH_MIN_NOTIONAL_USD", 2.0),
        enable_opening_buys=True,
        enable_opening_sells=True,
        debug=_env_bool("SOL_DEPTH_DEBUG", False),
    )


class SolDepthStrategy:
    """Requires ``WATCH_COINS`` to include ``SOL`` so the engine subscribes to SOL L2."""

    def __init__(self) -> None:
        self._impl = DepthFrontRun(_sol_config())

    def on_bbo(self, coin: str, msg: Any, portfolio: PortfolioView) -> list[LimitOrderIntent]:
        return []

    def on_user_event(self, msg: Any, portfolio: PortfolioView) -> list[LimitOrderIntent]:
        return []

    def on_l2_book(self, coin: str, book: PerpL2Book, portfolio: PortfolioView) -> list[LimitOrderIntent]:
        return self._impl.on_l2_book(coin, book, portfolio)
