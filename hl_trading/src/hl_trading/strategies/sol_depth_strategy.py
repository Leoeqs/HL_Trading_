"""SOL perp: front-run large resting liquidity (≥1000 SOL) within $0.10 of mid.

Accumulate (position ≤10% of account): for each bid level with size ≥ threshold, place a buy
one tick above the wall; size = fraction of account equity (USD) per order.

Reduce (position >10% of account): for each ask level with size ≥ threshold, place a sell
one tick below the wall; size = fraction of long position (SOL); ``reduce_only=True``.

Hyperliquid coin symbol: ``SOL``. Tick/size rounding are fixed for SOL meta (tick 0.001, sz 2 dp).

Tune via environment variables (optional)::

    SOL_DEPTH_THRESHOLD_SOL=1000
    SOL_DEPTH_NEAR_MID_USD=0.1
    SOL_DEPTH_TICK=0.001
    SOL_DEPTH_SZ_DECIMALS=2
    SOL_DEPTH_BUY_PCT=0.01
    SOL_DEPTH_SELL_POS_PCT=0.01
    SOL_DEPTH_POSITION_CAP_PCT=0.10
    SOL_DEPTH_MAX_ORDERS_PER_BOOK=12
    SOL_DEPTH_MIN_NOTIONAL_USD=5.0
    SOL_DEPTH_DEBUG=0   # if 1, log throttled snapshot (av, szi, reduce_mode, intents)
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

from hl_trading.book.l2 import PerpL2Book
from hl_trading.domain import LimitOrderIntent, PortfolioView

logger = logging.getLogger(__name__)

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


def _account_value_usd(margin: dict[str, Any]) -> float:
    for key in ("accountValue", "account_value"):
        v = margin.get(key)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                continue
    return 0.0


def _account_value_from_portfolio(portfolio: PortfolioView) -> float:
    """Prefer ``marginSummary``; fall back to ``crossMarginSummary`` (HL often fills one or the other)."""
    av = _account_value_usd(portfolio.margin_summary)
    if av > 0:
        return av
    raw = portfolio.raw if isinstance(portfolio.raw, dict) else {}
    for key in ("crossMarginSummary", "marginSummary"):
        m = raw.get(key)
        if isinstance(m, dict):
            av = max(av, _account_value_usd(m))
    return av


def _norm_side(s: Any) -> str:
    x = str(s).strip().lower()
    if x in ("b", "buy"):
        return "buy"
    if x in ("a", "ask", "sell"):
        return "sell"
    return x


def _order_dict(row: dict[str, Any]) -> dict[str, Any]:
    inner = row.get("order")
    return inner if isinstance(inner, dict) else row


def _has_open_limit(
    portfolio: PortfolioView,
    coin: str,
    side: str,
    limit_px: float,
    *,
    tick: float,
) -> bool:
    raw = portfolio.raw if isinstance(portfolio.raw, dict) else {}
    for row in raw.get("openOrders") or []:
        if not isinstance(row, dict):
            continue
        o = _order_dict(row)
        if str(o.get("coin", "")) != coin:
            continue
        if _norm_side(o.get("side")) != side:
            continue
        try:
            lp = float(o.get("limitPx", 0.0))
        except (TypeError, ValueError):
            continue
        if abs(lp - limit_px) <= tick * 0.5:
            return True
    return False


def _round_px(px: float, tick: float) -> float:
    if tick <= 0:
        return px
    steps = round(px / tick)
    return round(steps * tick, 8)


def _round_sz(sz: float, decimals: int) -> float:
    return round(sz + 1e-12, decimals)


class SolDepthStrategy:
    """Requires ``WATCH_COINS`` to include ``SOL`` so the engine subscribes to SOL L2."""

    def __init__(self) -> None:
        self._threshold_sol = _env_float("SOL_DEPTH_THRESHOLD_SOL", 1000.0)
        self._near_mid_usd = _env_float("SOL_DEPTH_NEAR_MID_USD", 0.1)
        self._tick = _env_float("SOL_DEPTH_TICK", 0.001)
        self._sz_decimals = _env_int("SOL_DEPTH_SZ_DECIMALS", 2)
        self._buy_pct = _env_float("SOL_DEPTH_BUY_PCT", 0.01)
        self._sell_pos_pct = _env_float("SOL_DEPTH_SELL_POS_PCT", 0.01)
        self._pos_cap_pct = _env_float("SOL_DEPTH_POSITION_CAP_PCT", 0.10)
        self._max_orders = _env_int("SOL_DEPTH_MAX_ORDERS_PER_BOOK", 12)
        self._min_notional = _env_float("SOL_DEPTH_MIN_NOTIONAL_USD", 5.0)
        self._warned_reduce_mode = False
        self._warned_low_account = False
        self._warned_buy_clip_too_small = False
        self._debug = _env_bool("SOL_DEPTH_DEBUG", False)
        self._last_debug_log_m: float = 0.0

    def on_bbo(self, coin: str, msg: Any, portfolio: PortfolioView) -> list[LimitOrderIntent]:
        return []

    def on_user_event(self, msg: Any, portfolio: PortfolioView) -> list[LimitOrderIntent]:
        return []

    def on_l2_book(self, coin: str, book: PerpL2Book, portfolio: PortfolioView) -> list[LimitOrderIntent]:
        if coin != COIN:
            return []

        mid = book.mid()
        if mid is None:
            return []

        av = _account_value_from_portfolio(portfolio)
        if av <= self._min_notional:
            if not self._warned_low_account:
                logger.warning(
                    "SolDepthStrategy: account value unreadable or <= %.2f USD — no orders (check marginSummary / crossMarginSummary)",
                    self._min_notional,
                )
                self._warned_low_account = True
            return []

        szi = float(portfolio.positions.get(COIN, 0.0))
        pos_usd = abs(szi) * mid
        pos_pct = pos_usd / av if av > 0 else 0.0
        reduce_mode = pos_pct > self._pos_cap_pct and szi > 0

        out: list[LimitOrderIntent] = []

        if reduce_mode:
            if not self._warned_reduce_mode:
                logger.info(
                    "SolDepthStrategy: position ~%.1f%% of account (>%d%% cap) — only **sell** intents (ask side); "
                    "bid walls are ignored until size drops",
                    pos_pct * 100,
                    int(self._pos_cap_pct * 100),
                )
                self._warned_reduce_mode = True
            for lvl in book.asks_asc():
                if len(out) >= self._max_orders:
                    break
                if lvl.sz < self._threshold_sol:
                    continue
                if abs(lvl.px - mid) > self._near_mid_usd + 1e-9:
                    continue
                limit_px = _round_px(lvl.px - self._tick, self._tick)
                if limit_px <= 0:
                    continue
                size = _round_sz(self._sell_pos_pct * abs(szi), self._sz_decimals)
                if size <= 0:
                    continue
                if size * limit_px < self._min_notional:
                    continue
                if _has_open_limit(portfolio, COIN, "sell", limit_px, tick=self._tick):
                    continue
                out.append(
                    LimitOrderIntent(
                        coin=COIN,
                        side="sell",
                        size=size,
                        limit_px=limit_px,
                        reduce_only=True,
                        tif="Gtc",
                    )
                )
            return out

        buy_clip_usd = self._buy_pct * av
        if buy_clip_usd < self._min_notional:
            if not self._warned_buy_clip_too_small:
                logger.warning(
                    "SolDepthStrategy: each buy is %.2f%% of account ≈ %.2f USD, below SOL_DEPTH_MIN_NOTIONAL_USD=%.2f — "
                    "no buys until equity is higher (~%.0f USD+ at 1%%/5 USD min) or lower SOL_DEPTH_MIN_NOTIONAL_USD / raise SOL_DEPTH_BUY_PCT",
                    self._buy_pct * 100,
                    buy_clip_usd,
                    self._min_notional,
                    self._min_notional / self._buy_pct if self._buy_pct > 0 else 0.0,
                )
                self._warned_buy_clip_too_small = True
            return []

        for lvl in book.bids_desc():
            if len(out) >= self._max_orders:
                break
            if lvl.sz < self._threshold_sol:
                continue
            if abs(lvl.px - mid) > self._near_mid_usd + 1e-9:
                continue
            limit_px = _round_px(lvl.px + self._tick, self._tick)
            if limit_px <= 0:
                continue
            notion = self._buy_pct * av
            size = _round_sz(notion / limit_px, self._sz_decimals)
            if size <= 0:
                continue
            if size * limit_px < self._min_notional:
                continue
            if _has_open_limit(portfolio, COIN, "buy", limit_px, tick=self._tick):
                continue
            out.append(
                LimitOrderIntent(
                    coin=COIN,
                    side="buy",
                    size=size,
                    limit_px=limit_px,
                    reduce_only=False,
                    tif="Gtc",
                )
            )

        if self._debug:
            now = time.monotonic()
            if now - self._last_debug_log_m >= 5.0:
                self._last_debug_log_m = now
                bb, ba = book.best_bid(), book.best_ask()
                logger.info(
                    "SolDepthStrategy debug: av=%.2f szi=%s reduce=%s mid=%s bb=%s ba=%s intents=%d",
                    av,
                    szi,
                    reduce_mode,
                    mid,
                    (bb.px, bb.sz) if bb else None,
                    (ba.px, ba.sz) if ba else None,
                    len(out),
                )

        return out
