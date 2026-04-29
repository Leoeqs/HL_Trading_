"""Live directional-wallet signal daemon.

This module keeps wallet state in memory, detects position deltas as wallet
snapshots change, and emits trade/watch/skip decisions. It intentionally does
not place orders; execution should be wired only after the signal loop is
stable in dry-run.
"""

from __future__ import annotations

import json
import logging
import time
from collections import deque
from pathlib import Path
from typing import Any

from hyperliquid.info import Info

from hl_trading.services.actor_watch import fetch_wallet_snapshot
from hl_trading.services.market_data import MarketDataService
from hl_trading.services.wallet_signals import (
    CoinSignal,
    DecisionAction,
    DecisionSide,
    PositionEvent,
    SignalDecision,
    _fmt_usd,
    _position_event,
    _short_wallet,
    add_position_event_to_signal,
    decide_coin_signal,
)

logger = logging.getLogger(__name__)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class LiveWalletSignalDaemon:
    def __init__(
        self,
        info: Info,
        *,
        coins: list[str],
        wallets: list[str],
        poll_interval_s: float = 120.0,
        lookback_minutes: float = 120.0,
        min_delta_notional: float = 1_000.0,
        min_follow_notional: float = 100_000.0,
        min_follow_wallets: int = 2,
        min_imbalance: float = 0.75,
        max_opposite_ratio: float = 0.35,
        max_adverse_fade_ratio: float = 0.50,
        output_path: str | None = None,
    ) -> None:
        self._info = info
        self._md = MarketDataService(info)
        self._coins = [c.strip() for c in coins if c.strip()]
        self._coin_filter = {c.upper() for c in self._coins}
        self._wallets = sorted({w.strip().lower() for w in wallets if w.strip()})
        self._poll_interval_s = poll_interval_s
        self._lookback_ms = int(lookback_minutes * 60_000)
        self._min_delta_notional = min_delta_notional
        self._min_follow_notional = min_follow_notional
        self._min_follow_wallets = min_follow_wallets
        self._min_imbalance = min_imbalance
        self._max_opposite_ratio = max_opposite_ratio
        self._max_adverse_fade_ratio = max_adverse_fade_ratio
        self._output_path = Path(output_path) if output_path else None
        self._last_px: dict[str, float] = {}
        self._positions: dict[str, dict[str, float]] = {}
        self._events: deque[PositionEvent] = deque()
        self._last_decisions: dict[str, tuple[DecisionAction, DecisionSide | None, str]] = {}

    def run(self, *, duration_s: float | None = None) -> None:
        if not self._wallets:
            raise ValueError("at least one wallet is required")
        self._seed_prices()
        for coin in self._coins:
            self._md.subscribe_trades(coin, self._on_trades_message)
        stop_at = time.monotonic() + duration_s if duration_s is not None else None
        next_poll = 0.0
        logger.info(
            "live wallet signals starting coins=%s wallets=%d poll=%.1fs lookback=%.1fm",
            ",".join(self._coins),
            len(self._wallets),
            self._poll_interval_s,
            self._lookback_ms / 60_000,
        )

        with _JsonlWriter(self._output_path) as writer:
            while True:
                now = time.monotonic()
                if stop_at is not None and now >= stop_at:
                    return
                if now >= next_poll:
                    self._poll_wallets(writer)
                    self._emit_decisions(writer)
                    next_poll = now + self._poll_interval_s
                time.sleep(0.25)

    def _seed_prices(self) -> None:
        try:
            mids = self._info.all_mids()
        except Exception:
            logger.exception("all_mids failed; waiting for trade prices")
            return
        if not isinstance(mids, dict):
            return
        for coin in self._coins:
            px = _as_float(mids.get(coin))
            if px > 0:
                self._last_px[coin.upper()] = px

    def _on_trades_message(self, ws_msg: Any) -> None:
        if not isinstance(ws_msg, dict) or ws_msg.get("channel") != "trades":
            return
        for trade in ws_msg.get("data") or []:
            if not isinstance(trade, dict):
                continue
            coin = str(trade.get("coin", ""))
            if coin.upper() not in self._coin_filter:
                continue
            px = _as_float(trade.get("px"))
            if px > 0:
                self._last_px[coin.upper()] = px

    def _poll_wallets(self, writer: _JsonlWriter) -> None:
        for wallet in self._wallets:
            try:
                snapshot = fetch_wallet_snapshot(self._info, wallet)
            except Exception:
                logger.exception("live wallet snapshot failed account=%s", wallet)
                continue
            positions = {str(k).upper(): float(v) for k, v in snapshot.positions.items()}
            prior = self._positions.get(wallet)
            if prior is None:
                self._positions[wallet] = positions
                writer.write(
                    {
                        "type": "live_wallet_signal_baseline",
                        "observed_at_ms": snapshot.observed_at_ms,
                        "account": wallet,
                        "positions": {coin: positions.get(coin, 0.0) for coin in self._coin_filter},
                    }
                )
                continue

            for coin in self._coin_filter:
                prev_size = prior.get(coin, 0.0)
                new_size = positions.get(coin, 0.0)
                if new_size == prev_size:
                    continue
                event = _position_event(
                    account=wallet,
                    coin=coin,
                    observed_at_ms=snapshot.observed_at_ms,
                    prior_size=prev_size,
                    new_size=new_size,
                    approx_px=self._last_px.get(coin),
                )
                prior[coin] = new_size
                if event.approx_delta_notional < self._min_delta_notional:
                    continue
                self._events.append(event)
                writer.write(_event_record(event))
                logger.info(
                    "wallet event %s %s %s %s->%s notional=%s follow=%s fade=%s",
                    event.coin,
                    _short_wallet(event.account),
                    event.kind,
                    event.prior_size,
                    event.new_size,
                    _fmt_usd(event.approx_delta_notional),
                    event.follow_side or "-",
                    event.fade_side or "-",
                )
            self._positions[wallet] = positions
        self._prune_events()

    def _prune_events(self) -> None:
        cutoff = _now_ms() - self._lookback_ms
        while self._events and self._events[0].observed_at_ms < cutoff:
            self._events.popleft()

    def _emit_decisions(self, writer: _JsonlWriter) -> None:
        signals = {coin.upper(): CoinSignal(coin=coin.upper()) for coin in self._coins}
        for event in self._events:
            signal = signals.get(event.coin.upper())
            if signal is None:
                continue
            add_position_event_to_signal(signal, event)

        for signal in signals.values():
            decision = self._decision_for(signal)
            writer.write(decision.to_record(record_type="live_wallet_signal_decision"))
            key = (decision.action, decision.side, decision.reason)
            if self._last_decisions.get(signal.coin) != key:
                self._last_decisions[signal.coin] = key
                logger.info(
                    "decision %s %s %s reason=%s follow=%s opp=%s fade=%s wallets=%d imbalance=%.2f events=%d",
                    decision.coin,
                    decision.action,
                    decision.side or "-",
                    decision.reason,
                    _fmt_usd(decision.follow_notional),
                    _fmt_usd(decision.opposite_notional),
                    _fmt_usd(decision.adverse_fade_notional),
                    decision.follow_wallet_count,
                    decision.imbalance,
                    decision.recent_event_count,
                )

    def _decision_for(self, signal: CoinSignal) -> SignalDecision:
        return decide_coin_signal(
            signal,
            observed_at_ms=_now_ms(),
            min_follow_notional=self._min_follow_notional,
            min_follow_wallets=self._min_follow_wallets,
            min_imbalance=self._min_imbalance,
            max_opposite_ratio=self._max_opposite_ratio,
            max_adverse_fade_ratio=self._max_adverse_fade_ratio,
        )


class _JsonlWriter:
    def __init__(self, path: Path | None) -> None:
        self._path = path
        self._fh = None

    def __enter__(self) -> _JsonlWriter:
        if self._path is not None:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._fh = self._path.open("a", encoding="utf-8")
        return self

    def __exit__(self, *_exc: object) -> None:
        if self._fh is not None:
            self._fh.close()

    def write(self, record: dict[str, Any]) -> None:
        if self._fh is None:
            return
        self._fh.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
        self._fh.flush()


def _event_record(event: PositionEvent) -> dict[str, Any]:
    return {
        "type": "live_wallet_position_event",
        "account": event.account,
        "coin": event.coin,
        "observed_at_ms": event.observed_at_ms,
        "kind": event.kind,
        "prior_size": event.prior_size,
        "new_size": event.new_size,
        "delta_size": event.delta_size,
        "approx_px": event.approx_px,
        "approx_delta_notional": event.approx_delta_notional,
        "follow_side": event.follow_side,
        "fade_side": event.fade_side,
    }
