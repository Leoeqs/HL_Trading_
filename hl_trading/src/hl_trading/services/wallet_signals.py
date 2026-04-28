"""Directional wallet signal research from actor-watch NDJSON."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from hl_trading.services.actor_analysis import analyze_actor_ndjson

SignalSide = Literal["long", "short"]


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


@dataclass(frozen=True, slots=True)
class PositionEvent:
    account: str
    coin: str
    observed_at_ms: int
    kind: str
    prior_size: float
    new_size: float
    delta_size: float
    approx_px: float | None
    approx_delta_notional: float
    follow_side: SignalSide | None
    fade_side: SignalSide | None


@dataclass(slots=True)
class CoinSignal:
    coin: str
    follow_long_notional: float = 0.0
    follow_short_notional: float = 0.0
    fade_long_notional: float = 0.0
    fade_short_notional: float = 0.0
    follow_long_wallets: set[str] = field(default_factory=set)
    follow_short_wallets: set[str] = field(default_factory=set)
    fade_long_wallets: set[str] = field(default_factory=set)
    fade_short_wallets: set[str] = field(default_factory=set)
    events: list[PositionEvent] = field(default_factory=list)

    @property
    def best_follow_side(self) -> SignalSide | None:
        if self.follow_long_notional <= 0 and self.follow_short_notional <= 0:
            return None
        return "long" if self.follow_long_notional >= self.follow_short_notional else "short"

    @property
    def follow_imbalance(self) -> float:
        total = self.follow_long_notional + self.follow_short_notional
        if total <= 0.0:
            return 0.0
        return abs(self.follow_long_notional - self.follow_short_notional) / total

    def to_record(self) -> dict[str, Any]:
        return {
            "coin": self.coin,
            "best_follow_side": self.best_follow_side,
            "follow_imbalance": self.follow_imbalance,
            "follow_long_notional": self.follow_long_notional,
            "follow_short_notional": self.follow_short_notional,
            "fade_long_notional": self.fade_long_notional,
            "fade_short_notional": self.fade_short_notional,
            "follow_long_wallet_count": len(self.follow_long_wallets),
            "follow_short_wallet_count": len(self.follow_short_wallets),
            "fade_long_wallet_count": len(self.fade_long_wallets),
            "fade_short_wallet_count": len(self.fade_short_wallets),
            "event_count": len(self.events),
        }


@dataclass(frozen=True, slots=True)
class WalletSignalReport:
    path: str
    target_coins: tuple[str, ...]
    directional_wallet_count: int
    event_count: int
    lookback_minutes: float
    min_delta_notional: float
    latest_observed_at_ms: int
    signals: tuple[CoinSignal, ...]

    def to_record(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "target_coins": list(self.target_coins),
            "directional_wallet_count": self.directional_wallet_count,
            "event_count": self.event_count,
            "lookback_minutes": self.lookback_minutes,
            "min_delta_notional": self.min_delta_notional,
            "latest_observed_at_ms": self.latest_observed_at_ms,
            "signals": [s.to_record() for s in self.signals],
        }


def build_wallet_signal_report(
    path: str | Path,
    *,
    target_coins: list[str],
    lookback_minutes: float = 120.0,
    min_delta_notional: float = 1_000.0,
) -> WalletSignalReport:
    path_obj = Path(path)
    targets = tuple(c.strip().upper() for c in target_coins if c.strip())
    target_set = set(targets)
    analysis = analyze_actor_ndjson(path_obj)
    directional_accounts = {
        w.account
        for w in analysis.wallets
        if w.behavior_subtype
        in {
            "directional_position_holder",
            "directional_flow_only",
            "directional_with_passive_orders",
            "directional_unenriched",
        }
    }

    last_px: dict[str, float] = {}
    prior_positions: dict[str, dict[str, float]] = {}
    events: list[PositionEvent] = []
    latest_observed_at_ms = 0

    with path_obj.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(record, dict):
                continue
            typ = record.get("type")
            if typ == "large_trade":
                coin = str(record.get("coin", "")).upper()
                px = _as_float(record.get("px"))
                if coin and px > 0:
                    last_px[coin] = px
                continue
            if typ != "wallet_snapshot":
                continue

            account = str(record.get("account", "")).lower()
            if account not in directional_accounts:
                continue
            observed_at_ms = _as_int(record.get("observed_at_ms"))
            latest_observed_at_ms = max(latest_observed_at_ms, observed_at_ms)
            positions_raw = record.get("positions")
            if not isinstance(positions_raw, dict):
                continue
            positions = {str(k).upper(): _as_float(v) for k, v in positions_raw.items()}
            prior = prior_positions.setdefault(account, {})
            for coin in target_set:
                prev_size = prior.get(coin, 0.0)
                new_size = positions.get(coin, 0.0)
                if new_size == prev_size:
                    continue
                event = _position_event(
                    account=account,
                    coin=coin,
                    observed_at_ms=observed_at_ms,
                    prior_size=prev_size,
                    new_size=new_size,
                    approx_px=last_px.get(coin),
                )
                if event.approx_delta_notional >= min_delta_notional:
                    events.append(event)
                prior[coin] = new_size

    cutoff_ms = latest_observed_at_ms - int(lookback_minutes * 60_000)
    recent_events = [e for e in events if e.observed_at_ms >= cutoff_ms]
    signals_by_coin = {coin: CoinSignal(coin=coin) for coin in targets}
    for event in recent_events:
        signal = signals_by_coin[event.coin]
        signal.events.append(event)
        if event.follow_side == "long":
            signal.follow_long_notional += event.approx_delta_notional
            signal.follow_long_wallets.add(event.account)
        elif event.follow_side == "short":
            signal.follow_short_notional += event.approx_delta_notional
            signal.follow_short_wallets.add(event.account)
        if event.fade_side == "long":
            signal.fade_long_notional += event.approx_delta_notional
            signal.fade_long_wallets.add(event.account)
        elif event.fade_side == "short":
            signal.fade_short_notional += event.approx_delta_notional
            signal.fade_short_wallets.add(event.account)

    signals = tuple(sorted(signals_by_coin.values(), key=lambda s: len(s.events), reverse=True))
    return WalletSignalReport(
        path=str(path_obj),
        target_coins=targets,
        directional_wallet_count=len(directional_accounts),
        event_count=len(events),
        lookback_minutes=lookback_minutes,
        min_delta_notional=min_delta_notional,
        latest_observed_at_ms=latest_observed_at_ms,
        signals=signals,
    )


def format_wallet_signal_report(report: WalletSignalReport, *, top_events: int = 8) -> str:
    lines = [
        f"Wallet signal report: {report.path}",
        (
            f"targets={','.join(report.target_coins)} directional_wallets={report.directional_wallet_count:,} "
            f"events={report.event_count:,} lookback_min={report.lookback_minutes:g} "
            f"min_delta_notional={_fmt_usd(report.min_delta_notional)}"
        ),
        "",
        "Coin Signals",
    ]
    for signal in report.signals:
        lines.append(
            f"- {signal.coin}: follow={signal.best_follow_side or '-'} "
            f"imbalance={signal.follow_imbalance:.2f} "
            f"follow_long={_fmt_usd(signal.follow_long_notional)} ({len(signal.follow_long_wallets)}w) "
            f"follow_short={_fmt_usd(signal.follow_short_notional)} ({len(signal.follow_short_wallets)}w) "
            f"fade_long={_fmt_usd(signal.fade_long_notional)} ({len(signal.fade_long_wallets)}w) "
            f"fade_short={_fmt_usd(signal.fade_short_notional)} ({len(signal.fade_short_wallets)}w)"
        )
        for event in sorted(signal.events, key=lambda e: e.observed_at_ms, reverse=True)[:top_events]:
            lines.append(
                "    "
                f"{_short_wallet(event.account)} {event.kind} "
                f"{event.prior_size:g}->{event.new_size:g} "
                f"delta={event.delta_size:g} "
                f"notional={_fmt_usd(event.approx_delta_notional)} "
                f"follow={event.follow_side or '-'} fade={event.fade_side or '-'}"
            )
    lines.extend(
        [
            "",
            "Trading Interpretation",
            "- Follow open/increase/flip events when one side dominates and contradictions are small.",
            "- Treat reduce/exit as exit or fade candidates, especially when the original follow thesis disappears.",
            "- This report is a research layer; run in dry-run before placing live orders.",
        ]
    )
    return "\n".join(lines)


def _position_event(
    *,
    account: str,
    coin: str,
    observed_at_ms: int,
    prior_size: float,
    new_size: float,
    approx_px: float | None,
) -> PositionEvent:
    delta = new_size - prior_size
    kind = _event_kind(prior_size, new_size)
    follow_side: SignalSide | None = None
    fade_side: SignalSide | None = None
    if kind in {"open_long", "increase_long", "flip_to_long"}:
        follow_side = "long"
    elif kind in {"open_short", "increase_short", "flip_to_short"}:
        follow_side = "short"
    elif kind in {"reduce_long", "exit_long"}:
        fade_side = "short"
    elif kind in {"reduce_short", "exit_short"}:
        fade_side = "long"
    px = approx_px if approx_px and approx_px > 0 else None
    notional = abs(delta) * px if px is not None else abs(delta)
    return PositionEvent(
        account=account,
        coin=coin,
        observed_at_ms=observed_at_ms,
        kind=kind,
        prior_size=prior_size,
        new_size=new_size,
        delta_size=delta,
        approx_px=px,
        approx_delta_notional=notional,
        follow_side=follow_side,
        fade_side=fade_side,
    )


def _event_kind(prior_size: float, new_size: float) -> str:
    if prior_size == 0.0 and new_size > 0.0:
        return "open_long"
    if prior_size == 0.0 and new_size < 0.0:
        return "open_short"
    if prior_size > 0.0 and new_size == 0.0:
        return "exit_long"
    if prior_size < 0.0 and new_size == 0.0:
        return "exit_short"
    if prior_size > 0.0 and new_size < 0.0:
        return "flip_to_short"
    if prior_size < 0.0 and new_size > 0.0:
        return "flip_to_long"
    if new_size > prior_size > 0.0:
        return "increase_long"
    if 0.0 < new_size < prior_size:
        return "reduce_long"
    if new_size < prior_size < 0.0:
        return "increase_short"
    if prior_size < new_size < 0.0:
        return "reduce_short"
    return "position_change"


def _short_wallet(account: str) -> str:
    if len(account) <= 12:
        return account
    return f"{account[:8]}...{account[-6:]}"


def _fmt_usd(value: float) -> str:
    abs_v = abs(value)
    sign = "-" if value < 0 else ""
    if abs_v >= 1_000_000:
        return f"{sign}${abs_v / 1_000_000:.2f}M"
    if abs_v >= 1_000:
        return f"{sign}${abs_v / 1_000:.1f}K"
    return f"{sign}${abs_v:.0f}"
