"""Shared depth front-run logic: quote one tick better than large resting walls near mid."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

from hl_trading.book.l2 import PerpL2Book
from hl_trading.domain import LimitOrderIntent, PortfolioView
from hl_trading.services.portfolio import account_equity_usd

logger = logging.getLogger(__name__)


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


def has_open_limit(
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


def round_px(px: float, tick: float) -> float:
    if tick <= 0:
        return px
    steps = round(px / tick)
    return round(steps * tick, 8)


def round_sz(sz: float, decimals: int) -> float:
    return round(sz + 1e-12, decimals)


def qualifying_bids_in_band(
    book: PerpL2Book, mid: float, *, threshold: float, near_mid_usd: float
) -> tuple[int, float]:
    n = 0
    mx = 0.0
    for lvl in book.bids_desc():
        if abs(lvl.px - mid) > near_mid_usd + 1e-9:
            continue
        if lvl.sz >= threshold:
            n += 1
            mx = max(mx, lvl.sz)
    return n, mx


def qualifying_asks_in_band(
    book: PerpL2Book, mid: float, *, threshold: float, near_mid_usd: float
) -> tuple[int, float]:
    n = 0
    mx = 0.0
    for lvl in book.asks_asc():
        if abs(lvl.px - mid) > near_mid_usd + 1e-9:
            continue
        if lvl.sz >= threshold:
            n += 1
            mx = max(mx, lvl.sz)
    return n, mx


@dataclass(frozen=True, slots=True)
class DepthFrontRunConfig:
    coin: str
    log_name: str
    threshold: float
    reduce_threshold: float
    near_mid_usd: float
    tick: float
    sz_decimals: int
    buy_pct: float
    sell_pct: float
    sell_pos_pct: float
    pos_cap_pct: float
    pause_buys_when_over_cap: bool
    pause_sells_when_over_cap: bool
    max_orders: int
    min_notional: float
    enable_opening_buys: bool = True
    enable_opening_sells: bool = True
    debug: bool = False


class DepthFrontRun:
    """One coin: bids ≥ threshold → buy one tick above; asks ≥ threshold → sell one tick below (when enabled)."""

    def __init__(self, cfg: DepthFrontRunConfig) -> None:
        self._cfg = cfg
        self._warned_reduce_mode_long = False
        self._warned_reduce_mode_short = False
        self._warned_reduce_no_liquidity_long = False
        self._warned_reduce_no_liquidity_short = False
        self._warned_low_account = False
        self._warned_buy_clip_too_small = False
        self._warned_sell_clip_too_small = False
        self._last_debug_log_m: float = 0.0
        self._last_strategy_diag_m: float = 0.0

    def _reduce_clip(self, szi: float, limit_px: float) -> float:
        cap = abs(szi)
        if cap <= 0.0 or limit_px <= 0.0:
            return 0.0
        c = self._cfg
        sz = round_sz(c.sell_pos_pct * cap, c.sz_decimals)
        if sz * limit_px < c.min_notional:
            need = c.min_notional / limit_px
            sz = round_sz(min(need, cap), c.sz_decimals)
        if sz <= 0.0 or sz * limit_px < c.min_notional - 1e-9:
            return 0.0
        return sz

    def on_l2_book(self, coin: str, book: PerpL2Book, portfolio: PortfolioView) -> list[LimitOrderIntent]:
        c = self._cfg
        if str(coin).strip().upper() != c.coin:
            return []

        mid = book.mid()
        if mid is None:
            return []

        av = account_equity_usd(portfolio)
        if av <= c.min_notional:
            if not self._warned_low_account:
                logger.warning(
                    "%s: account value unreadable or <= %.2f USD — no orders (check marginSummary / crossMarginSummary)",
                    c.log_name,
                    c.min_notional,
                )
                self._warned_low_account = True
            return []

        szi = float(portfolio.positions.get(c.coin, 0.0))
        pos_usd = abs(szi) * mid
        pos_pct = pos_usd / av if av > 0 else 0.0
        long_reduce = pos_pct > c.pos_cap_pct and szi > 0
        short_reduce = pos_pct > c.pos_cap_pct and szi < 0

        out: list[LimitOrderIntent] = []

        if short_reduce:
            if not self._warned_reduce_mode_short:
                if c.pause_sells_when_over_cap:
                    logger.info(
                        "%s: short ~%.1f%% of account (>%d%% cap) — **buy-only** (reduce); "
                        "ask walls skipped until size drops (set pause_sells_when_over_cap=false to keep opening sells)",
                        c.log_name,
                        pos_pct * 100,
                        int(c.pos_cap_pct * 100),
                    )
                else:
                    logger.info(
                        "%s: short ~%.1f%% of account (>%d%% cap) — reduce-only buys; opening sells still allowed",
                        c.log_name,
                        pos_pct * 100,
                        int(c.pos_cap_pct * 100),
                    )
                self._warned_reduce_mode_short = True
            n_short0 = len(out)
            for lvl in book.bids_desc():
                if len(out) >= c.max_orders:
                    break
                if lvl.sz < c.reduce_threshold:
                    continue
                if abs(lvl.px - mid) > c.near_mid_usd + 1e-9:
                    continue
                limit_px = round_px(lvl.px + c.tick, c.tick)
                if limit_px <= 0:
                    continue
                size = self._reduce_clip(szi, limit_px)
                if size <= 0:
                    continue
                if has_open_limit(portfolio, c.coin, "buy", limit_px, tick=c.tick):
                    continue
                out.append(
                    LimitOrderIntent(
                        coin=c.coin,
                        side="buy",
                        size=size,
                        limit_px=limit_px,
                        reduce_only=True,
                        tif="Gtc",
                    )
                )
            if c.pause_sells_when_over_cap:
                if len(out) == n_short0 and not self._warned_reduce_no_liquidity_short:
                    logger.warning(
                        "%s: short reduce — **0 reduce-buy intents** (no bid ≥%.4g within ±%.2f USD of mid, …)",
                        c.log_name,
                        c.reduce_threshold,
                        c.near_mid_usd,
                    )
                    self._warned_reduce_no_liquidity_short = True
                return out

        if long_reduce:
            if not self._warned_reduce_mode_long:
                if c.pause_buys_when_over_cap:
                    logger.info(
                        "%s: long ~%.1f%% of account (>%d%% cap) — **sell-only** (reduce); "
                        "bid walls skipped until size drops (set pause_buys_when_over_cap=false to keep opening buys)",
                        c.log_name,
                        pos_pct * 100,
                        int(c.pos_cap_pct * 100),
                    )
                else:
                    logger.info(
                        "%s: long ~%.1f%% of account (>%d%% cap) — reduce-only sells; opening buys still allowed",
                        c.log_name,
                        pos_pct * 100,
                        int(c.pos_cap_pct * 100),
                    )
                self._warned_reduce_mode_long = True
            n_long0 = len(out)
            for lvl in book.asks_asc():
                if len(out) >= c.max_orders:
                    break
                if lvl.sz < c.reduce_threshold:
                    continue
                if abs(lvl.px - mid) > c.near_mid_usd + 1e-9:
                    continue
                limit_px = round_px(lvl.px - c.tick, c.tick)
                if limit_px <= 0:
                    continue
                size = self._reduce_clip(szi, limit_px)
                if size <= 0:
                    continue
                if has_open_limit(portfolio, c.coin, "sell", limit_px, tick=c.tick):
                    continue
                out.append(
                    LimitOrderIntent(
                        coin=c.coin,
                        side="sell",
                        size=size,
                        limit_px=limit_px,
                        reduce_only=True,
                        tif="Gtc",
                    )
                )
            if c.pause_buys_when_over_cap:
                if len(out) == n_long0 and not self._warned_reduce_no_liquidity_long:
                    logger.warning(
                        "%s: long reduce — **0 reduce-sell intents** (no ask ≥%.4g within ±%.2f USD of mid, …)",
                        c.log_name,
                        c.reduce_threshold,
                        c.near_mid_usd,
                    )
                    self._warned_reduce_no_liquidity_long = True
                return out

        buy_clip_usd = c.buy_pct * av
        sell_clip_usd = c.sell_pct * av
        buy_clip_ok = c.enable_opening_buys and buy_clip_usd >= c.min_notional
        sell_clip_ok = c.enable_opening_sells and sell_clip_usd >= c.min_notional

        if c.enable_opening_buys and not buy_clip_ok and not self._warned_buy_clip_too_small:
            logger.warning(
                "%s: each buy is %.2f%% of account (~%.2f USD), below min_notional=%.2f — skipping **opening** buys",
                c.log_name,
                c.buy_pct * 100,
                buy_clip_usd,
                c.min_notional,
            )
            self._warned_buy_clip_too_small = True

        if c.enable_opening_sells and not sell_clip_ok and not self._warned_sell_clip_too_small:
            logger.warning(
                "%s: each sell is %.2f%% of account (~%.2f USD), below min_notional=%.2f — skipping **opening** sells",
                c.log_name,
                c.sell_pct * 100,
                sell_clip_usd,
                c.min_notional,
            )
            self._warned_sell_clip_too_small = True

        rem = c.max_orders - len(out)
        if c.enable_opening_buys and c.enable_opening_sells:
            cap_bids = (rem + 1) // 2
            cap_asks = rem // 2
        elif c.enable_opening_buys:
            cap_bids = rem
            cap_asks = 0
        elif c.enable_opening_sells:
            cap_bids = 0
            cap_asks = rem
        else:
            cap_bids = 0
            cap_asks = 0

        n_bid = 0
        n_ask = 0

        if buy_clip_ok:
            for lvl in book.bids_desc():
                if len(out) >= c.max_orders or n_bid >= cap_bids:
                    break
                if lvl.sz < c.threshold:
                    continue
                if abs(lvl.px - mid) > c.near_mid_usd + 1e-9:
                    continue
                limit_px = round_px(lvl.px + c.tick, c.tick)
                if limit_px <= 0:
                    continue
                notion = c.buy_pct * av
                size = round_sz(notion / limit_px, c.sz_decimals)
                if size <= 0:
                    continue
                if size * limit_px < c.min_notional:
                    continue
                if has_open_limit(portfolio, c.coin, "buy", limit_px, tick=c.tick):
                    continue
                out.append(
                    LimitOrderIntent(
                        coin=c.coin,
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
                if len(out) >= c.max_orders or n_ask >= cap_asks:
                    break
                if lvl.sz < c.threshold:
                    continue
                if abs(lvl.px - mid) > c.near_mid_usd + 1e-9:
                    continue
                limit_px = round_px(lvl.px - c.tick, c.tick)
                if limit_px <= 0:
                    continue
                notion = c.sell_pct * av
                size = round_sz(notion / limit_px, c.sz_decimals)
                if size <= 0:
                    continue
                if size * limit_px < c.min_notional:
                    continue
                if has_open_limit(portfolio, c.coin, "sell", limit_px, tick=c.tick):
                    continue
                out.append(
                    LimitOrderIntent(
                        coin=c.coin,
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
            n_qual_b, mx_qual_b = qualifying_bids_in_band(
                book, mid, threshold=c.threshold, near_mid_usd=c.near_mid_usd
            )
            n_qual_a, mx_qual_a = qualifying_asks_in_band(
                book, mid, threshold=c.threshold, near_mid_usd=c.near_mid_usd
            )
            logger.debug(
                "%s: HL L2 depth ~%d bids / %d asks; ±%.2f USD of mid: %d bid / %d ask level(s) ≥%.4g "
                "(max sz bid %.4g / ask %.4g); equity ~%.2f USD; intents=%d",
                c.log_name,
                db,
                da,
                c.near_mid_usd,
                n_qual_b,
                n_qual_a,
                c.threshold,
                mx_qual_b,
                mx_qual_a,
                av,
                len(out),
            )

        if c.debug:
            if now - self._last_debug_log_m >= 5.0:
                self._last_debug_log_m = now
                bb, ba = book.best_bid(), book.best_ask()
                logger.info(
                    "%s debug: av=%.2f szi=%s long_reduce=%s short_reduce=%s mid=%s bb=%s ba=%s intents=%d",
                    c.log_name,
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
