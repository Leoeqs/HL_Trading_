"""Large-trade and wallet behavior watcher for public Hyperliquid data.

The public ``trades`` feed does not identify wallets in current SDK messages.
This module still keeps the pipeline wallet-first: large trades are recorded by
hash, explicitly tracked wallets are snapshotted, and any future/enriched trade
payloads containing wallet fields are automatically folded into tracking.
"""

from __future__ import annotations

import json
import logging
import queue
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from hyperliquid.info import Info

from hl_trading.services.market_data import MarketDataService
from hl_trading.services.portfolio import fetch_portfolio_view

logger = logging.getLogger(__name__)


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass(frozen=True, slots=True)
class LargeTradeEvent:
    coin: str
    side: str
    px: float
    sz: float
    notional_usd: float
    exchange_time_ms: int
    hash: str
    raw: dict[str, Any]
    wallets: tuple[str, ...] = ()

    def to_record(self) -> dict[str, Any]:
        return {
            "type": "large_trade",
            "coin": self.coin,
            "side": self.side,
            "px": self.px,
            "sz": self.sz,
            "notional_usd": self.notional_usd,
            "exchange_time_ms": self.exchange_time_ms,
            "hash": self.hash,
            "wallets": list(self.wallets),
            "raw": self.raw,
        }


@dataclass(frozen=True, slots=True)
class OpenOrderView:
    oid: int
    coin: str
    side: str
    limit_px: float
    sz: float
    timestamp_ms: int | None

    @property
    def notional_usd(self) -> float:
        return abs(self.limit_px * self.sz)

    def to_record(self) -> dict[str, Any]:
        return {
            "oid": self.oid,
            "coin": self.coin,
            "side": self.side,
            "limit_px": self.limit_px,
            "sz": self.sz,
            "timestamp_ms": self.timestamp_ms,
            "notional_usd": self.notional_usd,
        }


@dataclass(frozen=True, slots=True)
class WalletSnapshot:
    account: str
    observed_at_ms: int
    positions: dict[str, float]
    open_orders: tuple[OpenOrderView, ...]
    margin_summary: dict[str, Any] = field(default_factory=dict)

    def to_record(self) -> dict[str, Any]:
        return {
            "type": "wallet_snapshot",
            "account": self.account,
            "observed_at_ms": self.observed_at_ms,
            "positions": self.positions,
            "open_orders": [o.to_record() for o in self.open_orders],
            "margin_summary": self.margin_summary,
        }


@dataclass(frozen=True, slots=True)
class WalletBehaviorFeatures:
    account: str
    observed_at_ms: int
    open_order_count: int
    bid_order_count: int
    ask_order_count: int
    bid_notional_usd: float
    ask_notional_usd: float
    added_order_count: int
    removed_order_count: int
    possible_replace_count: int
    avg_order_age_ms: float | None
    size_repetition_score: float
    quote_refresh_score: float

    def to_record(self) -> dict[str, Any]:
        return {
            "type": "wallet_behavior_features",
            "account": self.account,
            "observed_at_ms": self.observed_at_ms,
            "open_order_count": self.open_order_count,
            "bid_order_count": self.bid_order_count,
            "ask_order_count": self.ask_order_count,
            "bid_notional_usd": self.bid_notional_usd,
            "ask_notional_usd": self.ask_notional_usd,
            "added_order_count": self.added_order_count,
            "removed_order_count": self.removed_order_count,
            "possible_replace_count": self.possible_replace_count,
            "avg_order_age_ms": self.avg_order_age_ms,
            "size_repetition_score": self.size_repetition_score,
            "quote_refresh_score": self.quote_refresh_score,
        }


class NdjsonSink:
    def __init__(self, path: str | None) -> None:
        self._path = Path(path) if path else None
        self._fh = None

    def __enter__(self) -> NdjsonSink:
        if self._path is not None:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._fh = self._path.open("a", encoding="utf-8")
        return self

    def __exit__(self, *_exc: object) -> None:
        if self._fh is not None:
            self._fh.close()

    def write(self, record: dict[str, Any]) -> None:
        line = json.dumps(record, sort_keys=True, separators=(",", ":"))
        if self._fh is None:
            print(line, flush=True)
            return
        self._fh.write(line + "\n")
        self._fh.flush()


class LargeTradeActorWatcher:
    def __init__(
        self,
        info: Info,
        *,
        coins: list[str],
        min_notional_usd: float,
        tracked_wallets: list[str] | None = None,
        wallet_poll_interval_s: float = 5.0,
        auto_track_trade_wallets: bool = True,
        max_tracked_wallets: int = 100,
        output_path: str | None = None,
        dex: str = "",
    ) -> None:
        self._info = info
        self._md = MarketDataService(info)
        self._coins = [c.strip().upper() for c in coins if c.strip()]
        self._min_notional_usd = min_notional_usd
        self._tracked_wallets = {w.lower() for w in tracked_wallets or [] if w}
        self._wallet_poll_interval_s = wallet_poll_interval_s
        self._auto_track_trade_wallets = auto_track_trade_wallets
        self._max_tracked_wallets = max_tracked_wallets
        self._output_path = output_path
        self._dex = dex
        self._trade_q: queue.Queue[LargeTradeEvent] = queue.Queue(maxsize=10_000)
        self._previous_snapshots: dict[str, WalletSnapshot] = {}

    def run(self, *, duration_s: float | None = None) -> None:
        logger.info(
            "actor watcher starting coins=%s min_notional=%.2f tracked_wallets=%d",
            ",".join(self._coins),
            self._min_notional_usd,
            len(self._tracked_wallets),
        )
        for coin in self._coins:
            self._md.subscribe_trades(coin, self._on_trades_message)

        stop_at = time.monotonic() + duration_s if duration_s is not None else None
        next_wallet_poll = 0.0
        with NdjsonSink(self._output_path) as sink:
            while True:
                now = time.monotonic()
                if stop_at is not None and now >= stop_at:
                    return

                self._drain_large_trades(sink)

                if self._tracked_wallets and now >= next_wallet_poll:
                    self._poll_tracked_wallets(sink)
                    next_wallet_poll = now + self._wallet_poll_interval_s

                time.sleep(0.1)

    def _on_trades_message(self, ws_msg: Any) -> None:
        if not isinstance(ws_msg, dict) or ws_msg.get("channel") != "trades":
            return
        for trade in ws_msg.get("data") or []:
            if not isinstance(trade, dict):
                continue
            event = self._parse_large_trade(trade)
            if event is None:
                continue
            try:
                self._trade_q.put_nowait(event)
            except queue.Full:
                logger.error("large trade queue full; dropping hash=%s", event.hash)

    def _parse_large_trade(self, trade: dict[str, Any]) -> LargeTradeEvent | None:
        px = _as_float(trade.get("px"))
        sz = _as_float(trade.get("sz"))
        notional = abs(px * sz)
        if notional < self._min_notional_usd:
            return None
        coin = str(trade.get("coin", "")).upper()
        if self._coins and coin not in self._coins:
            return None
        return LargeTradeEvent(
            coin=coin,
            side=str(trade.get("side", "")),
            px=px,
            sz=sz,
            notional_usd=notional,
            exchange_time_ms=_as_int(trade.get("time")),
            hash=str(trade.get("hash", "")),
            wallets=tuple(_extract_wallets_from_trade(trade)),
            raw=dict(trade),
        )

    def _drain_large_trades(self, sink: NdjsonSink) -> None:
        while True:
            try:
                event = self._trade_q.get_nowait()
            except queue.Empty:
                return
            sink.write(event.to_record())
            if self._auto_track_trade_wallets:
                for wallet in event.wallets:
                    self._track_wallet(wallet)
            logger.info(
                "large trade coin=%s side=%s notional=%.0f px=%s sz=%s wallets=%d hash=%s",
                event.coin,
                event.side,
                event.notional_usd,
                event.px,
                event.sz,
                len(event.wallets),
                event.hash,
            )

    def _track_wallet(self, wallet: str) -> None:
        normalized = wallet.lower()
        if normalized in self._tracked_wallets:
            return
        if self._max_tracked_wallets > 0 and len(self._tracked_wallets) >= self._max_tracked_wallets:
            logger.warning("max tracked wallets reached; ignoring account=%s", normalized)
            return
        self._tracked_wallets.add(normalized)

    def _poll_tracked_wallets(self, sink: NdjsonSink) -> None:
        for wallet in sorted(self._tracked_wallets):
            try:
                snapshot = fetch_wallet_snapshot(self._info, wallet, dex=self._dex)
            except Exception:
                logger.exception("wallet snapshot failed account=%s", wallet)
                continue
            sink.write(snapshot.to_record())
            features = compute_wallet_behavior_features(snapshot, self._previous_snapshots.get(wallet))
            sink.write(features.to_record())
            self._previous_snapshots[wallet] = snapshot


def fetch_wallet_snapshot(info: Info, account: str, *, dex: str = "") -> WalletSnapshot:
    view = fetch_portfolio_view(info, account, dex=dex)
    raw = view.raw or {}
    return WalletSnapshot(
        account=account.lower(),
        observed_at_ms=_now_ms(),
        positions=dict(view.positions),
        open_orders=tuple(_parse_open_order(o) for o in raw.get("openOrders") or [] if isinstance(o, dict)),
        margin_summary=dict(view.margin_summary),
    )


def compute_wallet_behavior_features(
    snapshot: WalletSnapshot,
    previous: WalletSnapshot | None = None,
) -> WalletBehaviorFeatures:
    orders = snapshot.open_orders
    bids = [o for o in orders if _is_bid(o.side)]
    asks = [o for o in orders if _is_ask(o.side)]
    current_oids = {o.oid for o in orders if o.oid > 0}
    previous_oids = {o.oid for o in previous.open_orders if o.oid > 0} if previous else set()
    added = current_oids - previous_oids
    removed = previous_oids - current_oids
    ages = [snapshot.observed_at_ms - o.timestamp_ms for o in orders if o.timestamp_ms]
    possible_replaces = min(len(added), len(removed))
    return WalletBehaviorFeatures(
        account=snapshot.account,
        observed_at_ms=snapshot.observed_at_ms,
        open_order_count=len(orders),
        bid_order_count=len(bids),
        ask_order_count=len(asks),
        bid_notional_usd=sum(o.notional_usd for o in bids),
        ask_notional_usd=sum(o.notional_usd for o in asks),
        added_order_count=len(added),
        removed_order_count=len(removed),
        possible_replace_count=possible_replaces,
        avg_order_age_ms=sum(ages) / len(ages) if ages else None,
        size_repetition_score=_size_repetition_score(orders),
        quote_refresh_score=possible_replaces / max(len(orders), 1),
    )


def _parse_open_order(order: dict[str, Any]) -> OpenOrderView:
    ts = order.get("timestamp", order.get("time"))
    return OpenOrderView(
        oid=_as_int(order.get("oid")),
        coin=str(order.get("coin", "")).upper(),
        side=str(order.get("side", "")),
        limit_px=_as_float(order.get("limitPx", order.get("px"))),
        sz=_as_float(order.get("sz")),
        timestamp_ms=_as_int(ts) if ts is not None else None,
    )


def _extract_wallets_from_trade(trade: dict[str, Any]) -> list[str]:
    wallets: list[str] = []
    for key in ("user", "buyer", "seller", "maker", "taker", "makerUser", "takerUser"):
        value = trade.get(key)
        if isinstance(value, str) and value.startswith("0x"):
            wallets.append(value.lower())
    users = trade.get("users")
    if isinstance(users, list):
        wallets.extend(str(u).lower() for u in users if isinstance(u, str) and u.startswith("0x"))
    return sorted(set(wallets))


def _is_bid(side: str) -> bool:
    return side.lower() in {"b", "bid", "buy"}


def _is_ask(side: str) -> bool:
    return side.lower() in {"a", "ask", "sell"}


def _size_repetition_score(orders: tuple[OpenOrderView, ...]) -> float:
    if not orders:
        return 0.0
    counts: dict[float, int] = {}
    for order in orders:
        bucket = round(order.sz, 8)
        counts[bucket] = counts.get(bucket, 0) + 1
    return max(counts.values()) / len(orders)
