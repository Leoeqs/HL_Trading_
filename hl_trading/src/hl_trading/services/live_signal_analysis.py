"""Analysis utilities for live wallet signal NDJSON output."""

from __future__ import annotations

import json
from bisect import bisect_left
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any

from hl_trading.services.wallet_signals import _fmt_usd, _short_wallet


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


@dataclass(slots=True)
class WalletLiveStats:
    account: str
    event_count: int = 0
    total_notional: float = 0.0
    follow_long_notional: float = 0.0
    follow_short_notional: float = 0.0
    fade_long_notional: float = 0.0
    fade_short_notional: float = 0.0
    coins: set[str] = field(default_factory=set)
    kind_counts: Counter[str] = field(default_factory=Counter)
    first_observed_at_ms: int = 0
    latest_observed_at_ms: int = 0

    def observe(self, record: dict[str, Any]) -> None:
        coin = str(record.get("coin", "")).upper()
        ts = _as_int(record.get("observed_at_ms"))
        notional = _as_float(record.get("approx_delta_notional"))
        kind = str(record.get("kind", "unknown"))
        follow_side = record.get("follow_side")
        fade_side = record.get("fade_side")
        self.event_count += 1
        self.total_notional += notional
        if coin:
            self.coins.add(coin)
        self.kind_counts[kind] += 1
        if ts > 0:
            self.first_observed_at_ms = ts if self.first_observed_at_ms == 0 else min(self.first_observed_at_ms, ts)
            self.latest_observed_at_ms = max(self.latest_observed_at_ms, ts)
        if follow_side == "long":
            self.follow_long_notional += notional
        elif follow_side == "short":
            self.follow_short_notional += notional
        if fade_side == "long":
            self.fade_long_notional += notional
        elif fade_side == "short":
            self.fade_short_notional += notional

    @property
    def dominant_side(self) -> str:
        if self.follow_long_notional <= 0 and self.follow_short_notional <= 0:
            return "-"
        return "long" if self.follow_long_notional >= self.follow_short_notional else "short"

    @property
    def dominant_notional(self) -> float:
        return max(self.follow_long_notional, self.follow_short_notional)

    def to_record(self) -> dict[str, Any]:
        return {
            "account": self.account,
            "event_count": self.event_count,
            "total_notional": self.total_notional,
            "dominant_side": self.dominant_side,
            "dominant_notional": self.dominant_notional,
            "follow_long_notional": self.follow_long_notional,
            "follow_short_notional": self.follow_short_notional,
            "fade_long_notional": self.fade_long_notional,
            "fade_short_notional": self.fade_short_notional,
            "coins": sorted(self.coins),
            "kind_counts": dict(self.kind_counts),
            "first_observed_at_ms": self.first_observed_at_ms,
            "latest_observed_at_ms": self.latest_observed_at_ms,
        }


@dataclass(slots=True)
class CoinLiveStats:
    coin: str
    event_count: int = 0
    total_event_notional: float = 0.0
    follow_long_notional: float = 0.0
    follow_short_notional: float = 0.0
    fade_long_notional: float = 0.0
    fade_short_notional: float = 0.0
    follow_long_wallets: set[str] = field(default_factory=set)
    follow_short_wallets: set[str] = field(default_factory=set)
    fade_long_wallets: set[str] = field(default_factory=set)
    fade_short_wallets: set[str] = field(default_factory=set)
    kind_counts: Counter[str] = field(default_factory=Counter)
    decision_counts: Counter[str] = field(default_factory=Counter)
    transition_counts: Counter[str] = field(default_factory=Counter)
    reason_counts: Counter[str] = field(default_factory=Counter)
    latest_decision: dict[str, Any] | None = None
    latest_event_at_ms: int = 0
    latest_decision_at_ms: int = 0
    first_observed_at_ms: int = 0
    latest_observed_at_ms: int = 0

    def observe_event(self, record: dict[str, Any]) -> None:
        ts = _as_int(record.get("observed_at_ms"))
        account = str(record.get("account", "")).lower()
        notional = _as_float(record.get("approx_delta_notional"))
        kind = str(record.get("kind", "unknown"))
        follow_side = record.get("follow_side")
        fade_side = record.get("fade_side")
        self.event_count += 1
        self.total_event_notional += notional
        self.kind_counts[kind] += 1
        if ts > 0:
            self.latest_event_at_ms = max(self.latest_event_at_ms, ts)
            self._observe_ts(ts)
        if follow_side == "long":
            self.follow_long_notional += notional
            if account:
                self.follow_long_wallets.add(account)
        elif follow_side == "short":
            self.follow_short_notional += notional
            if account:
                self.follow_short_wallets.add(account)
        if fade_side == "long":
            self.fade_long_notional += notional
            if account:
                self.fade_long_wallets.add(account)
        elif fade_side == "short":
            self.fade_short_notional += notional
            if account:
                self.fade_short_wallets.add(account)

    def observe_decision(self, record: dict[str, Any], *, is_transition: bool) -> None:
        ts = _as_int(record.get("observed_at_ms"))
        action = str(record.get("action", "UNKNOWN"))
        reason = str(record.get("reason", "unknown"))
        side = str(record.get("side") or "-")
        key = f"{action}:{side}"
        self.decision_counts[key] += 1
        for part in reason.split(","):
            if part:
                self.reason_counts[part] += 1
        if is_transition:
            self.transition_counts[key] += 1
        self.latest_decision = record
        if ts > 0:
            self.latest_decision_at_ms = max(self.latest_decision_at_ms, ts)
            self._observe_ts(ts)

    @property
    def best_follow_side(self) -> str:
        if self.follow_long_notional <= 0 and self.follow_short_notional <= 0:
            return "-"
        return "long" if self.follow_long_notional >= self.follow_short_notional else "short"

    @property
    def follow_imbalance(self) -> float:
        total = self.follow_long_notional + self.follow_short_notional
        if total <= 0:
            return 0.0
        return abs(self.follow_long_notional - self.follow_short_notional) / total

    def _observe_ts(self, ts: int) -> None:
        self.first_observed_at_ms = ts if self.first_observed_at_ms == 0 else min(self.first_observed_at_ms, ts)
        self.latest_observed_at_ms = max(self.latest_observed_at_ms, ts)

    def to_record(self) -> dict[str, Any]:
        latest = self.latest_decision or {}
        return {
            "coin": self.coin,
            "event_count": self.event_count,
            "total_event_notional": self.total_event_notional,
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
            "kind_counts": dict(self.kind_counts),
            "decision_counts": dict(self.decision_counts),
            "transition_counts": dict(self.transition_counts),
            "reason_counts": dict(self.reason_counts),
            "latest_decision": latest,
            "latest_event_at_ms": self.latest_event_at_ms,
            "latest_decision_at_ms": self.latest_decision_at_ms,
        }


@dataclass(frozen=True, slots=True)
class TradeEntry:
    coin: str
    side: str
    observed_at_ms: int
    entry_px: float | None
    action_reason: str
    follow_notional: float
    opposite_notional: float
    adverse_fade_notional: float
    follow_wallet_count: int
    imbalance: float
    recent_event_count: int

    def to_record(self) -> dict[str, Any]:
        return {
            "coin": self.coin,
            "side": self.side,
            "observed_at_ms": self.observed_at_ms,
            "entry_px": self.entry_px,
            "action_reason": self.action_reason,
            "follow_notional": self.follow_notional,
            "opposite_notional": self.opposite_notional,
            "adverse_fade_notional": self.adverse_fade_notional,
            "follow_wallet_count": self.follow_wallet_count,
            "imbalance": self.imbalance,
            "recent_event_count": self.recent_event_count,
        }


@dataclass(frozen=True, slots=True)
class HorizonPerformance:
    horizon_minutes: float
    evaluated_count: int
    missing_count: int
    win_count: int
    loss_count: int
    avg_return_bps: float | None
    median_return_bps: float | None
    best_return_bps: float | None
    worst_return_bps: float | None

    def to_record(self) -> dict[str, Any]:
        return {
            "horizon_minutes": self.horizon_minutes,
            "evaluated_count": self.evaluated_count,
            "missing_count": self.missing_count,
            "win_count": self.win_count,
            "loss_count": self.loss_count,
            "avg_return_bps": self.avg_return_bps,
            "median_return_bps": self.median_return_bps,
            "best_return_bps": self.best_return_bps,
            "worst_return_bps": self.worst_return_bps,
        }


@dataclass(frozen=True, slots=True)
class ActionableEvent:
    account: str
    coin: str
    observed_at_ms: int
    kind: str
    tactic: str
    side: str
    entry_px: float
    notional: float

    def to_record(self) -> dict[str, Any]:
        return {
            "account": self.account,
            "coin": self.coin,
            "observed_at_ms": self.observed_at_ms,
            "kind": self.kind,
            "tactic": self.tactic,
            "side": self.side,
            "entry_px": self.entry_px,
            "notional": self.notional,
        }


@dataclass(slots=True)
class CalibrationBucket:
    key_type: str
    key: str
    label: str
    event_count: int = 0
    evaluated_count: int = 0
    missing_count: int = 0
    win_count: int = 0
    loss_count: int = 0
    total_notional: float = 0.0
    returns_bps: list[float] = field(default_factory=list)
    coins: Counter[str] = field(default_factory=Counter)
    tactics: Counter[str] = field(default_factory=Counter)
    sides: Counter[str] = field(default_factory=Counter)
    kinds: Counter[str] = field(default_factory=Counter)

    def observe(self, event: ActionableEvent, return_bps: float | None) -> None:
        self.event_count += 1
        self.total_notional += event.notional
        self.coins[event.coin] += 1
        self.tactics[event.tactic] += 1
        self.sides[event.side] += 1
        self.kinds[event.kind] += 1
        if return_bps is None:
            self.missing_count += 1
            return
        self.evaluated_count += 1
        self.returns_bps.append(return_bps)
        if return_bps > 0:
            self.win_count += 1
        elif return_bps < 0:
            self.loss_count += 1

    @property
    def win_rate(self) -> float | None:
        if self.evaluated_count <= 0:
            return None
        return self.win_count / self.evaluated_count

    @property
    def avg_return_bps(self) -> float | None:
        if not self.returns_bps:
            return None
        return sum(self.returns_bps) / len(self.returns_bps)

    @property
    def median_return_bps(self) -> float | None:
        if not self.returns_bps:
            return None
        return median(self.returns_bps)

    @property
    def best_return_bps(self) -> float | None:
        if not self.returns_bps:
            return None
        return max(self.returns_bps)

    @property
    def worst_return_bps(self) -> float | None:
        if not self.returns_bps:
            return None
        return min(self.returns_bps)

    @property
    def weighted_edge_bps(self) -> float | None:
        avg = self.avg_return_bps
        if avg is None:
            return None
        # Penalize tiny samples so a one-off lucky wallet is not ranked as durable edge.
        confidence_weight = min(1.0, self.evaluated_count / 20)
        return avg * confidence_weight

    def to_record(self) -> dict[str, Any]:
        return {
            "key_type": self.key_type,
            "key": self.key,
            "label": self.label,
            "event_count": self.event_count,
            "evaluated_count": self.evaluated_count,
            "missing_count": self.missing_count,
            "win_count": self.win_count,
            "loss_count": self.loss_count,
            "win_rate": self.win_rate,
            "total_notional": self.total_notional,
            "avg_return_bps": self.avg_return_bps,
            "median_return_bps": self.median_return_bps,
            "best_return_bps": self.best_return_bps,
            "worst_return_bps": self.worst_return_bps,
            "weighted_edge_bps": self.weighted_edge_bps,
            "coins": dict(self.coins),
            "tactics": dict(self.tactics),
            "sides": dict(self.sides),
            "kinds": dict(self.kinds),
        }


@dataclass(frozen=True, slots=True)
class LiveSignalAnalysis:
    path: str
    target_coins: tuple[str, ...]
    record_count: int
    invalid_record_count: int
    baseline_wallet_count: int
    position_event_count: int
    decision_count: int
    trade_decision_count: int
    watch_decision_count: int
    skip_decision_count: int
    decision_transition_count: int
    trade_entry_count: int
    first_observed_at_ms: int
    latest_observed_at_ms: int
    coin_stats: tuple[CoinLiveStats, ...]
    top_wallets: tuple[WalletLiveStats, ...]
    trade_entries: tuple[TradeEntry, ...]
    performance: tuple[HorizonPerformance, ...]
    price_observation_counts: dict[str, int]
    decision_price_coverage: float
    calibration_horizon_minutes: float
    min_calibration_events: int
    wallet_calibration: tuple[CalibrationBucket, ...]
    pattern_calibration: tuple[CalibrationBucket, ...]

    def to_record(self, *, top_wallets: int = 20, top_entries: int = 20) -> dict[str, Any]:
        return {
            "path": self.path,
            "target_coins": list(self.target_coins),
            "record_count": self.record_count,
            "invalid_record_count": self.invalid_record_count,
            "baseline_wallet_count": self.baseline_wallet_count,
            "position_event_count": self.position_event_count,
            "decision_count": self.decision_count,
            "trade_decision_count": self.trade_decision_count,
            "watch_decision_count": self.watch_decision_count,
            "skip_decision_count": self.skip_decision_count,
            "decision_transition_count": self.decision_transition_count,
            "trade_entry_count": self.trade_entry_count,
            "first_observed_at_ms": self.first_observed_at_ms,
            "latest_observed_at_ms": self.latest_observed_at_ms,
            "decision_price_coverage": self.decision_price_coverage,
            "price_observation_counts": self.price_observation_counts,
            "coins": [s.to_record() for s in self.coin_stats],
            "top_wallets": [w.to_record() for w in self.top_wallets[:top_wallets]],
            "trade_entries": [e.to_record() for e in self.trade_entries[:top_entries]],
            "performance": [p.to_record() for p in self.performance],
            "calibration_horizon_minutes": self.calibration_horizon_minutes,
            "min_calibration_events": self.min_calibration_events,
            "wallet_calibration": [b.to_record() for b in self.wallet_calibration],
            "pattern_calibration": [b.to_record() for b in self.pattern_calibration],
        }


def analyze_live_signal_ndjson(
    path: str | Path,
    *,
    target_coins: tuple[str, ...] = (),
    horizons_minutes: tuple[float, ...] = (5.0, 15.0, 30.0, 60.0),
    calibration_horizon_minutes: float = 15.0,
    min_calibration_events: int = 5,
    top_wallets: int = 20,
) -> LiveSignalAnalysis:
    path_obj = Path(path)
    coin_filter = {coin.strip().upper() for coin in target_coins if coin.strip()}
    targets = tuple(sorted(coin_filter))
    record_count = 0
    invalid_record_count = 0
    baseline_wallets: set[str] = set()
    position_event_count = 0
    decision_count = 0
    trade_decision_count = 0
    watch_decision_count = 0
    skip_decision_count = 0
    decision_transition_count = 0
    decisions_with_px = 0
    first_observed_at_ms = 0
    latest_observed_at_ms = 0
    coins: dict[str, CoinLiveStats] = {}
    wallets: dict[str, WalletLiveStats] = {}
    price_points: dict[str, list[tuple[int, float]]] = defaultdict(list)
    latest_px: dict[str, float] = {}
    last_decision_key: dict[str, tuple[str, str | None, str]] = {}
    trade_entries: list[TradeEntry] = []
    actionable_events: list[ActionableEvent] = []

    def include_coin(coin: str) -> bool:
        return not coin_filter or coin.upper() in coin_filter

    def coin_stats(coin: str) -> CoinLiveStats:
        coin = coin.upper()
        stats = coins.get(coin)
        if stats is None:
            stats = CoinLiveStats(coin=coin)
            coins[coin] = stats
        return stats

    with path_obj.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            record_count += 1
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                invalid_record_count += 1
                continue
            if not isinstance(record, dict):
                invalid_record_count += 1
                continue

            ts = _as_int(record.get("observed_at_ms"))
            if ts > 0:
                first_observed_at_ms = ts if first_observed_at_ms == 0 else min(first_observed_at_ms, ts)
                latest_observed_at_ms = max(latest_observed_at_ms, ts)
            typ = record.get("type")
            if typ == "live_wallet_signal_baseline":
                account = str(record.get("account", "")).lower()
                if account:
                    baseline_wallets.add(account)
                continue

            if typ == "live_wallet_position_event":
                coin = str(record.get("coin", "")).upper()
                if not coin or not include_coin(coin):
                    continue
                position_event_count += 1
                account = str(record.get("account", "")).lower()
                px = _as_float(record.get("approx_px"))
                notional = _as_float(record.get("approx_delta_notional"))
                kind = str(record.get("kind", "unknown"))
                if coin:
                    coin_stats(coin).observe_event(record)
                    if ts > 0 and px > 0:
                        price_points[coin].append((ts, px))
                        latest_px[coin] = px
                        follow_side = record.get("follow_side")
                        fade_side = record.get("fade_side")
                        if account and follow_side in {"long", "short"}:
                            actionable_events.append(
                                ActionableEvent(account, coin, ts, kind, "follow", str(follow_side), px, notional)
                            )
                        if account and fade_side in {"long", "short"}:
                            actionable_events.append(
                                ActionableEvent(account, coin, ts, kind, "fade", str(fade_side), px, notional)
                            )
                if account:
                    stats = wallets.get(account)
                    if stats is None:
                        stats = WalletLiveStats(account=account)
                        wallets[account] = stats
                    stats.observe(record)
                continue

            if typ == "live_wallet_signal_decision":
                coin = str(record.get("coin", "")).upper()
                if not coin or not include_coin(coin):
                    continue
                decision_count += 1
                action = str(record.get("action", "UNKNOWN"))
                side = record.get("side")
                reason = str(record.get("reason", "unknown"))
                px = _as_float(record.get("approx_px"))
                if coin and px > 0:
                    decisions_with_px += 1
                    if ts > 0:
                        price_points[coin].append((ts, px))
                    latest_px[coin] = px
                elif coin and latest_px.get(coin, 0) > 0:
                    px = latest_px[coin]
                if action == "TRADE":
                    trade_decision_count += 1
                elif action == "WATCH":
                    watch_decision_count += 1
                elif action == "SKIP":
                    skip_decision_count += 1
                key = (action, str(side) if side else None, reason)
                previous_key = last_decision_key.get(coin)
                is_transition = previous_key != key
                if is_transition:
                    decision_transition_count += 1
                    if action == "TRADE" and side in {"long", "short"}:
                        trade_entries.append(
                            TradeEntry(
                                coin=coin,
                                side=str(side),
                                observed_at_ms=ts,
                                entry_px=px if px > 0 else None,
                                action_reason=reason,
                                follow_notional=_as_float(record.get("follow_notional")),
                                opposite_notional=_as_float(record.get("opposite_notional")),
                                adverse_fade_notional=_as_float(record.get("adverse_fade_notional")),
                                follow_wallet_count=_as_int(record.get("follow_wallet_count")),
                                imbalance=_as_float(record.get("imbalance")),
                                recent_event_count=_as_int(record.get("recent_event_count")),
                            )
                        )
                last_decision_key[coin] = key
                coin_stats(coin).observe_decision(record, is_transition=is_transition)

    for points in price_points.values():
        points.sort(key=lambda p: p[0])
    sorted_coins = tuple(sorted(coins.values(), key=lambda c: (c.latest_decision_at_ms, c.event_count), reverse=True))
    sorted_wallets = tuple(
        sorted(wallets.values(), key=lambda w: (w.total_notional, w.event_count), reverse=True)[:top_wallets]
    )
    performance = _evaluate_trade_entries(trade_entries, price_points, horizons_minutes)
    wallet_calibration, pattern_calibration = _calibrate_actionable_events(
        actionable_events,
        price_points,
        horizon_minutes=calibration_horizon_minutes,
        min_events=min_calibration_events,
    )
    price_observation_counts = {coin: len(points) for coin, points in sorted(price_points.items())}
    decision_price_coverage = decisions_with_px / decision_count if decision_count else 0.0
    return LiveSignalAnalysis(
        path=str(path_obj),
        target_coins=targets,
        record_count=record_count,
        invalid_record_count=invalid_record_count,
        baseline_wallet_count=len(baseline_wallets),
        position_event_count=position_event_count,
        decision_count=decision_count,
        trade_decision_count=trade_decision_count,
        watch_decision_count=watch_decision_count,
        skip_decision_count=skip_decision_count,
        decision_transition_count=decision_transition_count,
        trade_entry_count=len(trade_entries),
        first_observed_at_ms=first_observed_at_ms,
        latest_observed_at_ms=latest_observed_at_ms,
        coin_stats=sorted_coins,
        top_wallets=sorted_wallets,
        trade_entries=tuple(sorted(trade_entries, key=lambda e: e.observed_at_ms, reverse=True)),
        performance=performance,
        price_observation_counts=price_observation_counts,
        decision_price_coverage=decision_price_coverage,
        calibration_horizon_minutes=calibration_horizon_minutes,
        min_calibration_events=min_calibration_events,
        wallet_calibration=wallet_calibration,
        pattern_calibration=pattern_calibration,
    )


def format_live_signal_analysis(
    analysis: LiveSignalAnalysis,
    *,
    top_wallets: int = 15,
    top_entries: int = 15,
) -> str:
    span = _format_span(analysis.first_observed_at_ms, analysis.latest_observed_at_ms)
    targets = ",".join(analysis.target_coins) if analysis.target_coins else "ALL"
    lines = [
        f"Live signal analysis: {analysis.path}",
        (
            f"targets={targets} records={analysis.record_count:,} invalid={analysis.invalid_record_count:,} "
            f"wallets={analysis.baseline_wallet_count:,} span={span}"
        ),
        (
            f"position_events={analysis.position_event_count:,} decisions={analysis.decision_count:,} "
            f"raw_trade={analysis.trade_decision_count:,} raw_watch={analysis.watch_decision_count:,} "
            f"raw_skip={analysis.skip_decision_count:,}"
        ),
        (
            f"decision_transitions={analysis.decision_transition_count:,} "
            f"trade_entries={analysis.trade_entry_count:,} "
            f"decision_price_coverage={analysis.decision_price_coverage:.1%}"
        ),
        "",
        "Latest Coin State",
    ]
    for stats in analysis.coin_stats:
        latest = stats.latest_decision or {}
        action = latest.get("action", "-")
        side = latest.get("side") or "-"
        reason = latest.get("reason") or "-"
        follow = _as_float(latest.get("follow_notional"))
        opposite = _as_float(latest.get("opposite_notional"))
        fade = _as_float(latest.get("adverse_fade_notional"))
        wallets = _as_int(latest.get("follow_wallet_count"))
        imbalance = _as_float(latest.get("imbalance"))
        lines.append(
            f"- {stats.coin}: latest={action} {side} reason={reason} "
            f"follow={_fmt_usd(follow)} opp={_fmt_usd(opposite)} fade={_fmt_usd(fade)} "
            f"wallets={wallets} imbalance={imbalance:.2f} events={stats.event_count:,}"
        )
        lines.append(
            f"  cumulative follow long={_fmt_usd(stats.follow_long_notional)} ({len(stats.follow_long_wallets)}w) "
            f"short={_fmt_usd(stats.follow_short_notional)} ({len(stats.follow_short_wallets)}w) "
            f"fade long={_fmt_usd(stats.fade_long_notional)} short={_fmt_usd(stats.fade_short_notional)} "
            f"cum_imbalance={stats.follow_imbalance:.2f}"
        )
        decision_bits = _format_counter(stats.decision_counts, limit=4)
        transition_bits = _format_counter(stats.transition_counts, limit=4)
        reason_bits = _format_counter(stats.reason_counts, limit=4)
        lines.append(f"  decisions={decision_bits or '-'} transitions={transition_bits or '-'} reasons={reason_bits or '-'}")

    lines.extend(["", "Distinct Trade Entries"])
    if analysis.trade_entries:
        for entry in analysis.trade_entries[:top_entries]:
            px = f"{entry.entry_px:g}" if entry.entry_px else "-"
            lines.append(
                f"- {_format_ts(entry.observed_at_ms)} {entry.coin} {entry.side} px={px} "
                f"follow={_fmt_usd(entry.follow_notional)} opp={_fmt_usd(entry.opposite_notional)} "
                f"fade={_fmt_usd(entry.adverse_fade_notional)} wallets={entry.follow_wallet_count} "
                f"imbalance={entry.imbalance:.2f} events={entry.recent_event_count:,} reason={entry.action_reason}"
            )
    else:
        lines.append("- none")

    lines.extend(["", "Proxy Forward Performance"])
    if analysis.performance:
        for perf in analysis.performance:
            avg = _fmt_bps(perf.avg_return_bps)
            med = _fmt_bps(perf.median_return_bps)
            best = _fmt_bps(perf.best_return_bps)
            worst = _fmt_bps(perf.worst_return_bps)
            lines.append(
                f"- {perf.horizon_minutes:g}m: evaluated={perf.evaluated_count} missing={perf.missing_count} "
                f"wins={perf.win_count} losses={perf.loss_count} avg={avg} median={med} best={best} worst={worst}"
            )
    else:
        lines.append("- not enough trade entries/price observations yet")
    lines.append(
        "  Note: performance is proxy-based. It is strongest after new live decision records include decision-time px; "
        "older files may use sparse wallet-event prices."
    )

    lines.extend(["", f"Top Wallets By Position-Change Notional (top {top_wallets})"])
    if analysis.top_wallets:
        for wallet in analysis.top_wallets[:top_wallets]:
            top_kinds = _format_counter(wallet.kind_counts, limit=3)
            lines.append(
                f"- {_short_wallet(wallet.account)} events={wallet.event_count:,} notional={_fmt_usd(wallet.total_notional)} "
                f"dom={wallet.dominant_side} {_fmt_usd(wallet.dominant_notional)} "
                f"coins={','.join(sorted(wallet.coins)) or '-'} kinds={top_kinds or '-'}"
            )
    else:
        lines.append("- none")

    lines.extend(
        [
            "",
            (
                f"Wallet Edge Calibration ({analysis.calibration_horizon_minutes:g}m forward, "
                f"min {analysis.min_calibration_events} evaluated events)"
            ),
        ]
    )
    positive_wallets = [b for b in analysis.wallet_calibration if (b.weighted_edge_bps or 0) > 0]
    negative_wallets = [b for b in reversed(analysis.wallet_calibration) if (b.weighted_edge_bps or 0) < 0]
    lines.append("Best follow/fade wallets:")
    if positive_wallets:
        for bucket in positive_wallets[: min(top_wallets, 10)]:
            lines.append(f"- {_format_bucket(bucket)}")
    else:
        lines.append("- none with positive weighted edge yet")
    lines.append("Worst follow/fade wallets:")
    if negative_wallets:
        for bucket in negative_wallets[: min(top_wallets, 10)]:
            lines.append(f"- {_format_bucket(bucket)}")
    else:
        lines.append("- none with negative weighted edge yet")

    lines.extend(["", "Event Pattern Calibration"])
    positive_patterns = [b for b in analysis.pattern_calibration if (b.weighted_edge_bps or 0) > 0]
    negative_patterns = [b for b in reversed(analysis.pattern_calibration) if (b.weighted_edge_bps or 0) < 0]
    lines.append("Best patterns:")
    if positive_patterns:
        for bucket in positive_patterns[:10]:
            lines.append(f"- {_format_bucket(bucket)}")
    else:
        lines.append("- none with positive weighted edge yet")
    lines.append("Worst patterns:")
    if negative_patterns:
        for bucket in negative_patterns[:10]:
            lines.append(f"- {_format_bucket(bucket)}")
    else:
        lines.append("- none with negative weighted edge yet")

    lines.extend(
        [
            "",
            "Interpretation",
            "- Use latest coin state for current directional pressure, not as automatic execution.",
            "- Use distinct trade entries instead of raw TRADE counts; raw decisions repeat every polling cycle.",
            "- If adverse fade or opposite pressure is large, the signal is internally conflicted even when direction is clear.",
            "- Promote wallets/patterns with durable positive edge; fade or exclude persistent negative-edge wallets.",
        ]
    )
    return "\n".join(lines)


def _evaluate_trade_entries(
    entries: list[TradeEntry],
    price_points: dict[str, list[tuple[int, float]]],
    horizons_minutes: tuple[float, ...],
) -> tuple[HorizonPerformance, ...]:
    results: list[HorizonPerformance] = []
    for horizon in horizons_minutes:
        horizon_ms = int(horizon * 60_000)
        returns: list[float] = []
        missing = 0
        for entry in entries:
            if entry.entry_px is None or entry.entry_px <= 0:
                missing += 1
                continue
            points = price_points.get(entry.coin, [])
            exit_px = _price_at_or_after(points, entry.observed_at_ms + horizon_ms)
            if exit_px is None:
                missing += 1
                continue
            direction = 1.0 if entry.side == "long" else -1.0
            returns.append(((exit_px - entry.entry_px) / entry.entry_px) * 10_000 * direction)
        if returns:
            wins = sum(1 for value in returns if value > 0)
            losses = sum(1 for value in returns if value < 0)
            results.append(
                HorizonPerformance(
                    horizon_minutes=horizon,
                    evaluated_count=len(returns),
                    missing_count=missing,
                    win_count=wins,
                    loss_count=losses,
                    avg_return_bps=sum(returns) / len(returns),
                    median_return_bps=median(returns),
                    best_return_bps=max(returns),
                    worst_return_bps=min(returns),
                )
            )
        else:
            results.append(
                HorizonPerformance(
                    horizon_minutes=horizon,
                    evaluated_count=0,
                    missing_count=missing,
                    win_count=0,
                    loss_count=0,
                    avg_return_bps=None,
                    median_return_bps=None,
                    best_return_bps=None,
                    worst_return_bps=None,
                )
            )
    return tuple(results)


def _calibrate_actionable_events(
    events: list[ActionableEvent],
    price_points: dict[str, list[tuple[int, float]]],
    *,
    horizon_minutes: float,
    min_events: int,
) -> tuple[tuple[CalibrationBucket, ...], tuple[CalibrationBucket, ...]]:
    horizon_ms = int(horizon_minutes * 60_000)
    wallet_buckets: dict[str, CalibrationBucket] = {}
    pattern_buckets: dict[str, CalibrationBucket] = {}
    for event in events:
        exit_px = _price_at_or_after(price_points.get(event.coin, []), event.observed_at_ms + horizon_ms)
        return_bps: float | None = None
        if exit_px is not None and event.entry_px > 0:
            direction = 1.0 if event.side == "long" else -1.0
            return_bps = ((exit_px - event.entry_px) / event.entry_px) * 10_000 * direction

        wallet_bucket = wallet_buckets.get(event.account)
        if wallet_bucket is None:
            wallet_bucket = CalibrationBucket("wallet", event.account, _short_wallet(event.account))
            wallet_buckets[event.account] = wallet_bucket
        wallet_bucket.observe(event, return_bps)

        pattern_key = f"{event.coin}:{event.tactic}:{event.kind}:{event.side}"
        pattern_bucket = pattern_buckets.get(pattern_key)
        if pattern_bucket is None:
            pattern_bucket = CalibrationBucket("pattern", pattern_key, pattern_key)
            pattern_buckets[pattern_key] = pattern_bucket
        pattern_bucket.observe(event, return_bps)

    def bucket_sort_key(bucket: CalibrationBucket) -> tuple[float, int, float]:
        edge = bucket.weighted_edge_bps
        return (edge if edge is not None else -1_000_000.0, bucket.evaluated_count, bucket.total_notional)

    wallet_results = tuple(
        sorted(
            (bucket for bucket in wallet_buckets.values() if bucket.evaluated_count >= min_events),
            key=bucket_sort_key,
            reverse=True,
        )
    )
    pattern_results = tuple(
        sorted(
            (bucket for bucket in pattern_buckets.values() if bucket.evaluated_count >= min_events),
            key=bucket_sort_key,
            reverse=True,
        )
    )
    return wallet_results, pattern_results


def _price_at_or_after(points: list[tuple[int, float]], ts: int) -> float | None:
    if not points:
        return None
    idx = bisect_left(points, (ts, 0.0))
    if idx >= len(points):
        return None
    return points[idx][1]


def _format_counter(counter: Counter[str], *, limit: int) -> str:
    return ", ".join(f"{key}={value:,}" for key, value in counter.most_common(limit))


def _format_bucket(bucket: CalibrationBucket) -> str:
    edge = _fmt_bps(bucket.weighted_edge_bps)
    avg = _fmt_bps(bucket.avg_return_bps)
    med = _fmt_bps(bucket.median_return_bps)
    win_rate = _fmt_pct(bucket.win_rate)
    coins = _format_counter(bucket.coins, limit=3) or "-"
    tactics = _format_counter(bucket.tactics, limit=2) or "-"
    kinds = _format_counter(bucket.kinds, limit=3) or "-"
    return (
        f"{bucket.label} edge={edge} avg={avg} median={med} win_rate={win_rate} "
        f"eval={bucket.evaluated_count:,}/{bucket.event_count:,} notional={_fmt_usd(bucket.total_notional)} "
        f"coins={coins} tactics={tactics} kinds={kinds}"
    )


def _format_span(first_ms: int, latest_ms: int) -> str:
    if first_ms <= 0 or latest_ms <= 0:
        return "-"
    minutes = (latest_ms - first_ms) / 60_000
    return f"{_format_ts(first_ms)} -> {_format_ts(latest_ms)} ({minutes:.1f}m)"


def _format_ts(ts_ms: int) -> str:
    if ts_ms <= 0:
        return "-"
    return datetime.fromtimestamp(ts_ms / 1000, tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def _fmt_bps(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:+.1f}bps"


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.0%}"
