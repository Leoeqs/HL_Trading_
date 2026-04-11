"""SOL perp: front-run large resting liquidity (≥1000 SOL) within $0.10 of mid.

**Hyperliquid L2 feed limit:** REST and websocket ``l2Book`` snapshots expose **at most ~20 bid
and 20 ask price levels**. A large wall that appears deep on the website may be **invisible** to
this bot if it is beyond those levels — lower ``SOL_DEPTH_THRESHOLD_SOL`` or widen logic if needed.

Accumulate: for each bid level with size ≥ threshold, place a buy one tick above the wall;
size = fraction of account equity (USD) per order.

Optional reduce-only sells when long and ``pos_pct > SOL_DEPTH_POSITION_CAP_PCT``: place sells
one tick below large asks (``reduce_only=True``). By default **buys are not paused** when over
that cap — only ``SOL_DEPTH_PAUSE_BUYS_WHEN_OVER_CAP=true`` restores the old “sell-only” behavior.
Use ``MAX_POSITION_USD_PER_COIN`` (risk) to cap total exposure instead.

Hyperliquid coin symbol: ``SOL``. Tick/size rounding are fixed for SOL meta (tick 0.001, sz 2 dp).

Tune via environment variables (optional)::

    SOL_DEPTH_THRESHOLD_SOL=1000
    SOL_DEPTH_NEAR_MID_USD=0.1
    SOL_DEPTH_TICK=0.001
    SOL_DEPTH_SZ_DECIMALS=2
    SOL_DEPTH_BUY_PCT=0.05
    SOL_DEPTH_SELL_POS_PCT=0.01
    SOL_DEPTH_POSITION_CAP_PCT=0.50
    SOL_DEPTH_PAUSE_BUYS_WHEN_OVER_CAP=false
    SOL_DEPTH_MAX_ORDERS_PER_BOOK=12
    SOL_DEPTH_MIN_NOTIONAL_USD=2.0
    SOL_DEPTH_DEBUG=0   # if 1, log throttled snapshot (av, szi, reduce_mode, intents)
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

from hl_trading.book.l2 import PerpL2Book
from hl_trading.domain import LimitOrderIntent, PortfolioView
from hl_trading.services.portfolio import account_equity_usd

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


def _qualifying_bids_in_band(
    book: PerpL2Book, mid: float, *, threshold_sol: float, near_mid_usd: float
) -> tuple[int, float]:
    """Count bid levels in band with sz >= threshold; return (count, max_sz among them)."""
    n = 0
    mx = 0.0
    for lvl in book.bids_desc():
        if abs(lvl.px - mid) > near_mid_usd + 1e-9:
            continue
        if lvl.sz >= threshold_sol:
            n += 1
            mx = max(mx, lvl.sz)
    return n, mx


class SolDepthStrategy:
    """Requires ``WATCH_COINS`` to include ``SOL`` so the engine subscribes to SOL L2."""

    def __init__(self) -> None:
        self._threshold_sol = _env_float("SOL_DEPTH_THRESHOLD_SOL", 1000.0)
        self._near_mid_usd = _env_float("SOL_DEPTH_NEAR_MID_USD", 0.1)
        self._tick = _env_float("SOL_DEPTH_TICK", 0.001)
        self._sz_decimals = _env_int("SOL_DEPTH_SZ_DECIMALS", 2)
        self._buy_pct = _env_float("SOL_DEPTH_BUY_PCT", 0.05)
        self._sell_pos_pct = _env_float("SOL_DEPTH_SELL_POS_PCT", 0.01)
        self._pos_cap_pct = _env_float("SOL_DEPTH_POSITION_CAP_PCT", 0.50)
        self._pause_buys_when_over_cap = _env_bool("SOL_DEPTH_PAUSE_BUYS_WHEN_OVER_CAP", False)
        self._max_orders = _env_int("SOL_DEPTH_MAX_ORDERS_PER_BOOK", 12)
        self._min_notional = _env_float("SOL_DEPTH_MIN_NOTIONAL_USD", 2.0)
        self._warned_reduce_mode = False
        self._warned_low_account = False
        self._warned_buy_clip_too_small = False
        self._debug = _env_bool("SOL_DEPTH_DEBUG", False)
        self._last_debug_log_m: float = 0.0
        self._last_strategy_diag_m: float = 0.0

    def on_bbo(self, coin: str, msg: Any, portfolio: PortfolioView) -> list[LimitOrderIntent]:
        return []

    def on_user_event(self, msg: Any, portfolio: PortfolioView) -> list[LimitOrderIntent]:
        return []

    def on_l2_book(self, coin: str, book: PerpL2Book, portfolio: PortfolioView) -> list[LimitOrderIntent]:
        if str(coin).strip().upper() != COIN:
            return []

        mid = book.mid()
        if mid is None:
            return []

        av = account_equity_usd(portfolio)
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
                if self._pause_buys_when_over_cap:
                    logger.info(
                        "SolDepthStrategy: position ~%.1f%% of account (>%d%% cap) — **sell-only** "
                        "(SOL_DEPTH_PAUSE_BUYS_WHEN_OVER_CAP=true); bid walls skipped until size drops",
                        pos_pct * 100,
                        int(self._pos_cap_pct * 100),
                    )
                else:
                    logger.info(
                        "SolDepthStrategy: position ~%.1f%% of account (>%d%% cap) — emitting reduce-only sells "
                        "where asks qualify; **buys still enabled** (pause buys: set "
                        "SOL_DEPTH_PAUSE_BUYS_WHEN_OVER_CAP=true)",
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
            if self._pause_buys_when_over_cap:
                return out

        buy_clip_usd = self._buy_pct * av
        if buy_clip_usd < self._min_notional:
            if not self._warned_buy_clip_too_small:
                logger.warning(
                    "SolDepthStrategy: each buy is %.2f%% of account ≈ %.2f USD, below SOL_DEPTH_MIN_NOTIONAL_USD=%.2f — "
                    "no buys until equity is higher (~%.0f USD+ for this buy_pct vs min notional) or lower SOL_DEPTH_MIN_NOTIONAL_USD / raise SOL_DEPTH_BUY_PCT",
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

        now = time.monotonic()
        if now - self._last_strategy_diag_m >= 30.0:
            self._last_strategy_diag_m = now
            db, da = book.depth_levels()
            n_qual, mx_qual = _qualifying_bids_in_band(
                book, mid, threshold_sol=self._threshold_sol, near_mid_usd=self._near_mid_usd
            )
            logger.info(
                "SolDepthStrategy: HL L2 depth ~%d bids / %d asks (API cap ~20/side); "
                "±%.2f USD of mid: %d bid level(s) ≥%.0f SOL (max sz %.1f); equity ~%.2f USD; intents=%d",
                db,
                da,
                self._near_mid_usd,
                n_qual,
                self._threshold_sol,
                mx_qual,
                av,
                len(out),
            )

        if self._debug:
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
