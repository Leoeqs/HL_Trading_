"""SOL perp: front-run large resting liquidity (≥1000 SOL) within $0.10 of mid.

**Hyperliquid L2 feed limit:** REST and websocket ``l2Book`` snapshots expose **at most ~20 bid
and 20 ask price levels**. A large wall that appears deep on the website may be **invisible** to
this bot if it is beyond those levels — lower ``SOL_DEPTH_THRESHOLD_SOL`` or widen logic if needed.

Accumulate **buys**: for each bid level with size ≥ threshold, place a buy **one tick above** the wall;
size = ``SOL_DEPTH_BUY_PCT`` × account equity (USD) / price.

Accumulate **sells** (mirror): for each ask level with size ≥ threshold, place a sell **one tick below**
the wall; size = ``SOL_DEPTH_SELL_PCT`` × equity / price (same pattern as buys).

When **long** and ``pos_pct > SOL_DEPTH_POSITION_CAP_PCT`` (default **50%**): emit reduce-only sells on
large asks. By default **new buys are paused** (``SOL_DEPTH_PAUSE_BUYS_WHEN_OVER_CAP``, default **true**).

When **short** and over the same cap: emit reduce-only **buys** on large bids. By default **new sells are
paused** (``SOL_DEPTH_PAUSE_SELLS_WHEN_OVER_CAP``, default **true**).

**Reduce vs opening (why sells can look “stuck”):** opening buys size off **equity** (large USD clip);
reduce sells size off **``SOL_DEPTH_SELL_POS_PCT`` × position** (default 1%) — that notional can fall
below ``SOL_DEPTH_MIN_NOTIONAL_USD`` unless we bump the clip. Reduce-only paths use the same wall-size
threshold as opening (**``SOL_DEPTH_THRESHOLD_SOL``**, default **1000**), unless you set
``SOL_DEPTH_REDUCE_THRESHOLD_SOL`` explicitly.

With both sides active, ``SOL_DEPTH_MAX_ORDERS_PER_BOOK`` is split **50/50** between bid and ask intents
(when not in a reduce-only regime).

Hyperliquid coin symbol: ``SOL``. Tick/size rounding are fixed for SOL meta (tick 0.001, sz 2 dp).

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


def _qualifying_asks_in_band(
    book: PerpL2Book, mid: float, *, threshold_sol: float, near_mid_usd: float
) -> tuple[int, float]:
    """Count ask levels in band with sz >= threshold; return (count, max_sz among them)."""
    n = 0
    mx = 0.0
    for lvl in book.asks_asc():
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
        self._reduce_threshold_sol = _env_float("SOL_DEPTH_REDUCE_THRESHOLD_SOL", self._threshold_sol)
        self._near_mid_usd = _env_float("SOL_DEPTH_NEAR_MID_USD", 0.1)
        self._tick = _env_float("SOL_DEPTH_TICK", 0.001)
        self._sz_decimals = _env_int("SOL_DEPTH_SZ_DECIMALS", 2)
        self._buy_pct = _env_float("SOL_DEPTH_BUY_PCT", 0.05)
        self._sell_pct = _env_float("SOL_DEPTH_SELL_PCT", self._buy_pct)
        self._sell_pos_pct = _env_float("SOL_DEPTH_SELL_POS_PCT", 0.01)
        self._pos_cap_pct = _env_float("SOL_DEPTH_POSITION_CAP_PCT", 0.50)
        self._pause_buys_when_over_cap = _env_bool("SOL_DEPTH_PAUSE_BUYS_WHEN_OVER_CAP", True)
        self._pause_sells_when_over_cap = _env_bool("SOL_DEPTH_PAUSE_SELLS_WHEN_OVER_CAP", True)
        self._max_orders = _env_int("SOL_DEPTH_MAX_ORDERS_PER_BOOK", 12)
        self._min_notional = _env_float("SOL_DEPTH_MIN_NOTIONAL_USD", 2.0)
        self._warned_reduce_mode_long = False
        self._warned_reduce_mode_short = False
        self._warned_reduce_no_liquidity_long = False
        self._warned_reduce_no_liquidity_short = False
        self._warned_low_account = False
        self._warned_buy_clip_too_small = False
        self._warned_sell_clip_too_small = False
        self._debug = _env_bool("SOL_DEPTH_DEBUG", False)
        self._last_debug_log_m: float = 0.0
        self._last_strategy_diag_m: float = 0.0

    def _reduce_clip_sol(self, szi: float, limit_px: float) -> float:
        """SOL size for reduce-only orders; may exceed ``SOL_DEPTH_SELL_POS_PCT`` to satisfy min notional."""
        cap = abs(szi)
        if cap <= 0.0 or limit_px <= 0.0:
            return 0.0
        sz = _round_sz(self._sell_pos_pct * cap, self._sz_decimals)
        if sz * limit_px < self._min_notional:
            need_sol = self._min_notional / limit_px
            sz = _round_sz(min(need_sol, cap), self._sz_decimals)
        if sz <= 0.0 or sz * limit_px < self._min_notional - 1e-9:
            return 0.0
        return sz

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
        long_reduce = pos_pct > self._pos_cap_pct and szi > 0
        short_reduce = pos_pct > self._pos_cap_pct and szi < 0

        out: list[LimitOrderIntent] = []

        # --- Short over cap: chip away with reduce-only buys in front of large bid walls ---
        if short_reduce:
            if not self._warned_reduce_mode_short:
                if self._pause_sells_when_over_cap:
                    logger.info(
                        "SolDepthStrategy: short ~%.1f%% of account (>%d%% cap) — **buy-only** (reduce); "
                        "ask walls skipped until size drops (set SOL_DEPTH_PAUSE_SELLS_WHEN_OVER_CAP=false to keep selling)",
                        pos_pct * 100,
                        int(self._pos_cap_pct * 100),
                    )
                else:
                    logger.info(
                        "SolDepthStrategy: short ~%.1f%% of account (>%d%% cap) — reduce-only buys where bids qualify; "
                        "**opening sells still enabled** (SOL_DEPTH_PAUSE_SELLS_WHEN_OVER_CAP=false)",
                        pos_pct * 100,
                        int(self._pos_cap_pct * 100),
                    )
                self._warned_reduce_mode_short = True
            n_short0 = len(out)
            for lvl in book.bids_desc():
                if len(out) >= self._max_orders:
                    break
                if lvl.sz < self._reduce_threshold_sol:
                    continue
                if abs(lvl.px - mid) > self._near_mid_usd + 1e-9:
                    continue
                limit_px = _round_px(lvl.px + self._tick, self._tick)
                if limit_px <= 0:
                    continue
                size = self._reduce_clip_sol(szi, limit_px)
                if size <= 0:
                    continue
                if _has_open_limit(portfolio, COIN, "buy", limit_px, tick=self._tick):
                    continue
                out.append(
                    LimitOrderIntent(
                        coin=COIN,
                        side="buy",
                        size=size,
                        limit_px=limit_px,
                        reduce_only=True,
                        tif="Gtc",
                    )
                )
            if self._pause_sells_when_over_cap:
                if len(out) == n_short0 and not self._warned_reduce_no_liquidity_short:
                    logger.warning(
                        "SolDepthStrategy: short reduce — **0 reduce-buy intents** (no bid ≥%.0f SOL within ±%.2f USD "
                        "of mid, clips dust vs min notional, or resting orders at those prices). "
                        "Try SOL_DEPTH_REDUCE_THRESHOLD_SOL lower, wider SOL_DEPTH_NEAR_MID_USD, or raise SOL_DEPTH_SELL_POS_PCT.",
                        self._reduce_threshold_sol,
                        self._near_mid_usd,
                    )
                    self._warned_reduce_no_liquidity_short = True
                return out

        # --- Long over cap: chip away with reduce-only sells in front of large ask walls ---
        if long_reduce:
            if not self._warned_reduce_mode_long:
                if self._pause_buys_when_over_cap:
                    logger.info(
                        "SolDepthStrategy: position ~%.1f%% of account (>%d%% cap) — **sell-only** (reduce); "
                        "bid walls skipped until size drops (set SOL_DEPTH_PAUSE_BUYS_WHEN_OVER_CAP=false to keep buying)",
                        pos_pct * 100,
                        int(self._pos_cap_pct * 100),
                    )
                else:
                    logger.info(
                        "SolDepthStrategy: position ~%.1f%% of account (>%d%% cap) — reduce-only sells where asks qualify; "
                        "**buys still enabled** (SOL_DEPTH_PAUSE_BUYS_WHEN_OVER_CAP=false)",
                        pos_pct * 100,
                        int(self._pos_cap_pct * 100),
                    )
                self._warned_reduce_mode_long = True
            n_long0 = len(out)
            for lvl in book.asks_asc():
                if len(out) >= self._max_orders:
                    break
                if lvl.sz < self._reduce_threshold_sol:
                    continue
                if abs(lvl.px - mid) > self._near_mid_usd + 1e-9:
                    continue
                limit_px = _round_px(lvl.px - self._tick, self._tick)
                if limit_px <= 0:
                    continue
                size = self._reduce_clip_sol(szi, limit_px)
                if size <= 0:
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
                if len(out) == n_long0 and not self._warned_reduce_no_liquidity_long:
                    logger.warning(
                        "SolDepthStrategy: long reduce — **0 reduce-sell intents** (no ask ≥%.0f SOL within ±%.2f USD "
                        "of mid, clips dust vs min notional, or resting orders at those prices). "
                        "Try SOL_DEPTH_REDUCE_THRESHOLD_SOL lower, wider SOL_DEPTH_NEAR_MID_USD, or raise SOL_DEPTH_SELL_POS_PCT.",
                        self._reduce_threshold_sol,
                        self._near_mid_usd,
                    )
                    self._warned_reduce_no_liquidity_long = True
                return out

        buy_clip_usd = self._buy_pct * av
        sell_clip_usd = self._sell_pct * av
        buy_clip_ok = buy_clip_usd >= self._min_notional
        sell_clip_ok = sell_clip_usd >= self._min_notional

        if not buy_clip_ok and not self._warned_buy_clip_too_small:
            logger.warning(
                "SolDepthStrategy: each buy is %.2f%% of account ≈ %.2f USD, below SOL_DEPTH_MIN_NOTIONAL_USD=%.2f — "
                "skipping **opening** buys (~%.0f USD+ equity for this buy_pct) or raise SOL_DEPTH_BUY_PCT / lower min notional",
                self._buy_pct * 100,
                buy_clip_usd,
                self._min_notional,
                self._min_notional / self._buy_pct if self._buy_pct > 0 else 0.0,
            )
            self._warned_buy_clip_too_small = True

        if not sell_clip_ok and not self._warned_sell_clip_too_small:
            logger.warning(
                "SolDepthStrategy: each sell is %.2f%% of account ≈ %.2f USD, below SOL_DEPTH_MIN_NOTIONAL_USD=%.2f — "
                "skipping **opening** sells (~%.0f USD+ equity for this sell_pct) or raise SOL_DEPTH_SELL_PCT / lower min notional",
                self._sell_pct * 100,
                sell_clip_usd,
                self._min_notional,
                self._min_notional / self._sell_pct if self._sell_pct > 0 else 0.0,
            )
            self._warned_sell_clip_too_small = True

        rem = self._max_orders - len(out)
        cap_bids = (rem + 1) // 2
        cap_asks = rem // 2
        n_bid = 0
        n_ask = 0

        if buy_clip_ok:
            for lvl in book.bids_desc():
                if len(out) >= self._max_orders or n_bid >= cap_bids:
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
                n_bid += 1

        if sell_clip_ok:
            for lvl in book.asks_asc():
                if len(out) >= self._max_orders or n_ask >= cap_asks:
                    break
                if lvl.sz < self._threshold_sol:
                    continue
                if abs(lvl.px - mid) > self._near_mid_usd + 1e-9:
                    continue
                limit_px = _round_px(lvl.px - self._tick, self._tick)
                if limit_px <= 0:
                    continue
                notion = self._sell_pct * av
                size = _round_sz(notion / limit_px, self._sz_decimals)
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
                        reduce_only=False,
                        tif="Gtc",
                    )
                )
                n_ask += 1

        now = time.monotonic()
        if now - self._last_strategy_diag_m >= 30.0:
            self._last_strategy_diag_m = now
            db, da = book.depth_levels()
            n_qual_b, mx_qual_b = _qualifying_bids_in_band(
                book, mid, threshold_sol=self._threshold_sol, near_mid_usd=self._near_mid_usd
            )
            n_qual_a, mx_qual_a = _qualifying_asks_in_band(
                book, mid, threshold_sol=self._threshold_sol, near_mid_usd=self._near_mid_usd
            )
            logger.debug(
                "SolDepthStrategy: HL L2 depth ~%d bids / %d asks (API cap ~20/side); "
                "±%.2f USD of mid: %d bid / %d ask level(s) ≥%.0f SOL (max sz bid %.1f / ask %.1f); equity ~%.2f USD; intents=%d",
                db,
                da,
                self._near_mid_usd,
                n_qual_b,
                n_qual_a,
                self._threshold_sol,
                mx_qual_b,
                mx_qual_a,
                av,
                len(out),
            )

        if self._debug:
            if now - self._last_debug_log_m >= 5.0:
                self._last_debug_log_m = now
                bb, ba = book.best_bid(), book.best_ask()
                logger.info(
                    "SolDepthStrategy debug: av=%.2f szi=%s long_reduce=%s short_reduce=%s mid=%s bb=%s ba=%s intents=%d",
                    av,
                    szi,
                    long_reduce,
                    short_reduce,
                    mid,
                    (bb.px, bb.sz) if bb else None,
                    (ba.px, ba.sz) if ba else None,
                    len(out),
                )

        return out
