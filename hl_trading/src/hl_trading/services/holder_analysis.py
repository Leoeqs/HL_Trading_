"""Current holder analysis for targeted Hyperliquid perp wallets."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from hl_trading.services.actor_analysis import WalletActorSummary, analyze_actor_ndjson


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
class CoinOrderPosture:
    order_count: int = 0
    bid_count: int = 0
    ask_count: int = 0
    bid_notional_usd: float = 0.0
    ask_notional_usd: float = 0.0

    @property
    def total_notional_usd(self) -> float:
        return self.bid_notional_usd + self.ask_notional_usd

    @property
    def passive_bias(self) -> str:
        if self.bid_notional_usd <= 0 and self.ask_notional_usd <= 0:
            return "none"
        if self.bid_notional_usd >= self.ask_notional_usd * 1.5:
            return "bid_heavy"
        if self.ask_notional_usd >= self.bid_notional_usd * 1.5:
            return "ask_heavy"
        return "balanced"

    def to_record(self) -> dict[str, Any]:
        return {
            "order_count": self.order_count,
            "bid_count": self.bid_count,
            "ask_count": self.ask_count,
            "bid_notional_usd": self.bid_notional_usd,
            "ask_notional_usd": self.ask_notional_usd,
            "total_notional_usd": self.total_notional_usd,
            "passive_bias": self.passive_bias,
        }


@dataclass(frozen=True, slots=True)
class HolderPosition:
    account: str
    coin: str
    side: str
    size: float
    approx_px: float | None
    notional_usd: float
    first_size: float
    latest_size: float
    delta_size: float
    delta_notional_usd: float
    snapshot_count: int
    latest_observed_at_ms: int
    order_posture: CoinOrderPosture
    actor: WalletActorSummary | None = None

    @property
    def changed_side(self) -> str:
        if self.first_size == 0.0 and self.latest_size != 0.0:
            return "opened"
        if self.first_size * self.latest_size < 0:
            return "flipped"
        if self.latest_size == 0.0 and self.first_size != 0.0:
            return "exited"
        if abs(self.latest_size) > abs(self.first_size):
            return "increased"
        if abs(self.latest_size) < abs(self.first_size):
            return "reduced"
        return "unchanged"

    @property
    def holder_type(self) -> str:
        if self.actor is None:
            return "unknown"
        return self.actor.behavior_subtype

    @property
    def open_order_count(self) -> int:
        return self.order_posture.order_count

    @property
    def visible_liquidity_usd(self) -> float:
        return self.order_posture.total_notional_usd

    @property
    def risk_read(self) -> str:
        if self.actor is None:
            base = "unclassified"
        elif self.actor.behavior_subtype in {"directional_position_holder", "directional_flow_only"}:
            base = "directional"
        elif self.actor.archetype == "market_maker":
            base = "liquidity_actor"
        elif self.actor.archetype == "mixed":
            base = "mixed_inventory"
        else:
            base = self.actor.archetype
        if self.changed_side in {"opened", "increased", "flipped"}:
            return f"{base}_building"
        if self.changed_side in {"reduced", "exited"}:
            return f"{base}_unwinding"
        return base

    def to_record(self) -> dict[str, Any]:
        actor = self.actor
        return {
            "account": self.account,
            "coin": self.coin,
            "side": self.side,
            "size": self.size,
            "approx_px": self.approx_px,
            "notional_usd": self.notional_usd,
            "first_size": self.first_size,
            "latest_size": self.latest_size,
            "delta_size": self.delta_size,
            "delta_notional_usd": self.delta_notional_usd,
            "changed_side": self.changed_side,
            "snapshot_count": self.snapshot_count,
            "latest_observed_at_ms": self.latest_observed_at_ms,
            "order_posture": self.order_posture.to_record(),
            "holder_type": self.holder_type,
            "risk_read": self.risk_read,
            "actor": actor.to_record() if actor is not None else None,
        }


@dataclass(frozen=True, slots=True)
class CoinHolderSummary:
    coin: str
    approx_px: float | None
    long_count: int
    short_count: int
    long_notional_usd: float
    short_notional_usd: float
    net_notional_usd: float
    gross_notional_usd: float
    top_long_share: float
    top_short_share: float
    top3_long_share: float
    top3_short_share: float
    long_hhi: float
    short_hhi: float
    passive_bid_notional_usd: float
    passive_ask_notional_usd: float
    holders: tuple[HolderPosition, ...]

    @property
    def dominant_side(self) -> str:
        if self.long_notional_usd <= 0 and self.short_notional_usd <= 0:
            return "-"
        return "long" if self.long_notional_usd >= self.short_notional_usd else "short"

    @property
    def notional_imbalance(self) -> float:
        if self.gross_notional_usd <= 0:
            return 0.0
        return abs(self.long_notional_usd - self.short_notional_usd) / self.gross_notional_usd

    @property
    def concentration_read(self) -> str:
        share = max(self.top_long_share, self.top_short_share)
        if share >= 0.50:
            return "very_concentrated"
        if share >= 0.30:
            return "concentrated"
        if share >= 0.15:
            return "moderate"
        return "distributed"

    def to_record(self, *, top: int | None = None) -> dict[str, Any]:
        holders = self.holders[:top] if top is not None else self.holders
        return {
            "coin": self.coin,
            "approx_px": self.approx_px,
            "dominant_side": self.dominant_side,
            "notional_imbalance": self.notional_imbalance,
            "concentration_read": self.concentration_read,
            "long_count": self.long_count,
            "short_count": self.short_count,
            "long_notional_usd": self.long_notional_usd,
            "short_notional_usd": self.short_notional_usd,
            "net_notional_usd": self.net_notional_usd,
            "gross_notional_usd": self.gross_notional_usd,
            "top_long_share": self.top_long_share,
            "top_short_share": self.top_short_share,
            "top3_long_share": self.top3_long_share,
            "top3_short_share": self.top3_short_share,
            "long_hhi": self.long_hhi,
            "short_hhi": self.short_hhi,
            "passive_bid_notional_usd": self.passive_bid_notional_usd,
            "passive_ask_notional_usd": self.passive_ask_notional_usd,
            "holders": [h.to_record() for h in holders],
        }


@dataclass(frozen=True, slots=True)
class HolderAnalysisResult:
    path: str
    target_coins: tuple[str, ...]
    line_count: int
    invalid_line_count: int
    wallet_snapshot_count: int
    unique_wallet_count: int
    first_observed_at_ms: int
    latest_observed_at_ms: int
    min_abs_notional_usd: float
    coin_summaries: tuple[CoinHolderSummary, ...]
    recent_movers: tuple[HolderPosition, ...]

    def to_record(self, *, top: int = 25) -> dict[str, Any]:
        return {
            "path": self.path,
            "target_coins": list(self.target_coins),
            "line_count": self.line_count,
            "invalid_line_count": self.invalid_line_count,
            "wallet_snapshot_count": self.wallet_snapshot_count,
            "unique_wallet_count": self.unique_wallet_count,
            "first_observed_at_ms": self.first_observed_at_ms,
            "latest_observed_at_ms": self.latest_observed_at_ms,
            "min_abs_notional_usd": self.min_abs_notional_usd,
            "coins": [s.to_record(top=top) for s in self.coin_summaries],
            "recent_movers": [h.to_record() for h in self.recent_movers[:top]],
        }


def analyze_holder_ndjson(
    path: str | Path,
    *,
    target_coins: tuple[str, ...] = ("LIT", "HYPE"),
    min_abs_notional_usd: float = 0.0,
) -> HolderAnalysisResult:
    path_obj = Path(path)
    targets = tuple(c.strip().upper() for c in target_coins if c.strip())
    target_set = set(targets)
    actor_result = analyze_actor_ndjson(path_obj)
    actors_by_account = {w.account: w for w in actor_result.wallets}

    line_count = 0
    invalid_line_count = 0
    wallet_snapshot_count = 0
    first_observed_at_ms = 0
    latest_observed_at_ms = 0
    latest_px: dict[str, float] = {}
    snapshots: dict[str, dict[str, Any]] = {}
    history: defaultdict[tuple[str, str], list[tuple[int, float]]] = defaultdict(list)

    with path_obj.open("r", encoding="utf-8") as fh:
        for line in fh:
            line_count += 1
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                invalid_line_count += 1
                continue
            if not isinstance(record, dict):
                invalid_line_count += 1
                continue
            typ = record.get("type")
            if typ == "large_trade":
                coin = str(record.get("coin", "")).upper()
                px = _as_float(record.get("px"))
                if coin in target_set and px > 0:
                    latest_px[coin] = px
                continue
            if typ != "wallet_snapshot":
                continue
            account = str(record.get("account", "")).lower()
            if not account:
                continue
            wallet_snapshot_count += 1
            ts = _as_int(record.get("observed_at_ms"))
            if ts > 0:
                first_observed_at_ms = ts if first_observed_at_ms == 0 else min(first_observed_at_ms, ts)
                latest_observed_at_ms = max(latest_observed_at_ms, ts)
            positions = record.get("positions")
            if isinstance(positions, dict):
                for coin in targets:
                    history[(account, coin)].append((ts, _as_float(positions.get(coin))))
            previous = snapshots.get(account)
            if previous is None or ts >= _as_int(previous.get("observed_at_ms")):
                snapshots[account] = record

    holders_by_coin: dict[str, list[HolderPosition]] = {coin: [] for coin in targets}
    for account, snapshot in snapshots.items():
        positions = snapshot.get("positions")
        if not isinstance(positions, dict):
            continue
        order_postures = _order_postures(snapshot.get("open_orders"), target_set)
        latest_ts = _as_int(snapshot.get("observed_at_ms"))
        actor = actors_by_account.get(account)
        for coin in targets:
            size = _as_float(positions.get(coin))
            if abs(size) <= 0.00000001:
                continue
            px = latest_px.get(coin)
            notional = abs(size) * px if px and px > 0 else abs(size)
            if notional < min_abs_notional_usd:
                continue
            position_history = history.get((account, coin), [])
            first_size = position_history[0][1] if position_history else size
            delta_size = size - first_size
            delta_notional = abs(delta_size) * px if px and px > 0 else abs(delta_size)
            holders_by_coin[coin].append(
                HolderPosition(
                    account=account,
                    coin=coin,
                    side="long" if size > 0 else "short",
                    size=size,
                    approx_px=px,
                    notional_usd=notional,
                    first_size=first_size,
                    latest_size=size,
                    delta_size=delta_size,
                    delta_notional_usd=delta_notional,
                    snapshot_count=len(position_history),
                    latest_observed_at_ms=latest_ts,
                    order_posture=order_postures.get(coin, CoinOrderPosture()),
                    actor=actor,
                )
            )

    coin_summaries = tuple(_summarize_coin(coin, latest_px.get(coin), holders_by_coin[coin]) for coin in targets)
    recent_movers = tuple(
        sorted(
            (
                holder
                for holders in holders_by_coin.values()
                for holder in holders
                if holder.changed_side != "unchanged" and holder.delta_notional_usd > 0
            ),
            key=lambda h: h.delta_notional_usd,
            reverse=True,
        )
    )
    return HolderAnalysisResult(
        path=str(path_obj),
        target_coins=targets,
        line_count=line_count,
        invalid_line_count=invalid_line_count,
        wallet_snapshot_count=wallet_snapshot_count,
        unique_wallet_count=len(snapshots),
        first_observed_at_ms=first_observed_at_ms,
        latest_observed_at_ms=latest_observed_at_ms,
        min_abs_notional_usd=min_abs_notional_usd,
        coin_summaries=coin_summaries,
        recent_movers=recent_movers,
    )


def format_holder_analysis(result: HolderAnalysisResult, *, top: int = 25) -> str:
    lines = [
        f"Holder analysis: {result.path}",
        (
            f"targets={','.join(result.target_coins)} records={result.line_count:,} "
            f"invalid={result.invalid_line_count:,} snapshots={result.wallet_snapshot_count:,} "
            f"wallets={result.unique_wallet_count:,} span={_format_span(result.first_observed_at_ms, result.latest_observed_at_ms)}"
        ),
        f"min_abs_notional={_fmt_usd(result.min_abs_notional_usd)}",
        "",
        "Coin Holder Structure",
    ]
    for summary in result.coin_summaries:
        lines.extend(_format_coin_summary(summary, top=top))

    lines.extend(["", "Largest Recent Position Changes"])
    if result.recent_movers:
        for holder in result.recent_movers[:top]:
            lines.append(_format_holder_line(holder, include_change=True))
    else:
        lines.append("- none")

    lines.extend(
        [
            "",
            "Trading Research Read",
            "- Treat concentrated holder sides as regime context: one large holder unwinding can dominate small flow signals.",
            "- Prefer directional holders with few open orders for conviction reads; classify market makers as liquidity/inventory inputs.",
            "- Watch long/short concentration plus recent change together: static whales matter less than whales adding, reducing, or flipping.",
            "- Compare passive bid/ask posture to position side: long plus bid-heavy can indicate support; long plus ask-heavy can indicate distribution.",
        ]
    )
    return "\n".join(lines)


def _summarize_coin(coin: str, px: float | None, holders: list[HolderPosition]) -> CoinHolderSummary:
    holders = sorted(holders, key=lambda h: h.notional_usd, reverse=True)
    longs = [h for h in holders if h.side == "long"]
    shorts = [h for h in holders if h.side == "short"]
    long_notional = sum(h.notional_usd for h in longs)
    short_notional = sum(h.notional_usd for h in shorts)
    bid_notional = sum(h.order_posture.bid_notional_usd for h in holders)
    ask_notional = sum(h.order_posture.ask_notional_usd for h in holders)
    return CoinHolderSummary(
        coin=coin,
        approx_px=px,
        long_count=len(longs),
        short_count=len(shorts),
        long_notional_usd=long_notional,
        short_notional_usd=short_notional,
        net_notional_usd=long_notional - short_notional,
        gross_notional_usd=long_notional + short_notional,
        top_long_share=_top_share(longs, long_notional, top=1),
        top_short_share=_top_share(shorts, short_notional, top=1),
        top3_long_share=_top_share(longs, long_notional, top=3),
        top3_short_share=_top_share(shorts, short_notional, top=3),
        long_hhi=_hhi(longs, long_notional),
        short_hhi=_hhi(shorts, short_notional),
        passive_bid_notional_usd=bid_notional,
        passive_ask_notional_usd=ask_notional,
        holders=tuple(holders),
    )


def _order_postures(open_orders: Any, target_set: set[str]) -> dict[str, CoinOrderPosture]:
    mutable: dict[str, dict[str, float | int]] = {
        coin: {"order_count": 0, "bid_count": 0, "ask_count": 0, "bid_notional": 0.0, "ask_notional": 0.0}
        for coin in target_set
    }
    if not isinstance(open_orders, list):
        return {}
    for order in open_orders:
        if not isinstance(order, dict):
            continue
        coin = str(order.get("coin", "")).upper()
        if coin not in target_set:
            continue
        side = str(order.get("side", "")).lower()
        notional = _as_float(order.get("notional_usd"))
        item = mutable[coin]
        item["order_count"] = int(item["order_count"]) + 1
        if side in {"b", "bid", "buy"}:
            item["bid_count"] = int(item["bid_count"]) + 1
            item["bid_notional"] = float(item["bid_notional"]) + notional
        elif side in {"a", "ask", "sell"}:
            item["ask_count"] = int(item["ask_count"]) + 1
            item["ask_notional"] = float(item["ask_notional"]) + notional
    return {
        coin: CoinOrderPosture(
            order_count=int(values["order_count"]),
            bid_count=int(values["bid_count"]),
            ask_count=int(values["ask_count"]),
            bid_notional_usd=float(values["bid_notional"]),
            ask_notional_usd=float(values["ask_notional"]),
        )
        for coin, values in mutable.items()
    }


def _format_coin_summary(summary: CoinHolderSummary, *, top: int) -> list[str]:
    px = f"{summary.approx_px:g}" if summary.approx_px else "-"
    lines = [
        "",
        (
            f"{summary.coin} px={px} dominant={summary.dominant_side} imbalance={summary.notional_imbalance:.2f} "
            f"concentration={summary.concentration_read}"
        ),
        (
            f"  longs={summary.long_count} {_fmt_usd(summary.long_notional_usd)} "
            f"shorts={summary.short_count} {_fmt_usd(summary.short_notional_usd)} "
            f"net={_fmt_usd(summary.net_notional_usd)} gross={_fmt_usd(summary.gross_notional_usd)}"
        ),
        (
            f"  top1 long={summary.top_long_share:.0%} short={summary.top_short_share:.0%} "
            f"top3 long={summary.top3_long_share:.0%} short={summary.top3_short_share:.0%} "
            f"hhi long={summary.long_hhi:.2f} short={summary.short_hhi:.2f}"
        ),
        (
            f"  passive orders bid={_fmt_usd(summary.passive_bid_notional_usd)} "
            f"ask={_fmt_usd(summary.passive_ask_notional_usd)}"
        ),
        "  Top Long Holders",
    ]
    longs = [h for h in summary.holders if h.side == "long"]
    shorts = [h for h in summary.holders if h.side == "short"]
    lines.extend(_format_holder_section(longs[:top]))
    lines.append("  Top Short Holders")
    lines.extend(_format_holder_section(shorts[:top]))
    return lines


def _format_holder_section(holders: list[HolderPosition]) -> list[str]:
    if not holders:
        return ["    none"]
    return ["    " + _format_holder_line(holder) for holder in holders]


def _format_holder_line(holder: HolderPosition, *, include_change: bool = False) -> str:
    actor = holder.actor
    score = actor.attention_score if actor is not None else 0.0
    mm = actor.market_maker_score if actor is not None else 0.0
    directional = actor.directional_score if actor is not None else 0.0
    change = (
        f" change={holder.changed_side} delta={holder.delta_size:g}/{_fmt_usd(holder.delta_notional_usd)}"
        if include_change
        else f" {holder.changed_side}"
    )
    return (
        f"- {_short_wallet(holder.account)} {holder.coin} {holder.side} size={holder.size:g} "
        f"notional={_fmt_usd(holder.notional_usd)}{change} "
        f"type={holder.holder_type} read={holder.risk_read} "
        f"orders={holder.open_order_count} bid={_fmt_usd(holder.order_posture.bid_notional_usd)} "
        f"ask={_fmt_usd(holder.order_posture.ask_notional_usd)} bias={holder.order_posture.passive_bias} "
        f"score={score:.1f} mm={mm:.1f} dir={directional:.1f}"
    )


def _top_share(holders: list[HolderPosition], total: float, *, top: int) -> float:
    if total <= 0:
        return 0.0
    return sum(h.notional_usd for h in holders[:top]) / total


def _hhi(holders: list[HolderPosition], total: float) -> float:
    if total <= 0:
        return 0.0
    return sum((h.notional_usd / total) ** 2 for h in holders)


def _short_wallet(account: str) -> str:
    if len(account) <= 12:
        return account
    return f"{account[:8]}...{account[-6:]}"


def _fmt_usd(value: float) -> str:
    abs_v = abs(value)
    sign = "-" if value < 0 else ""
    if abs_v >= 1_000_000_000:
        return f"{sign}${abs_v / 1_000_000_000:.2f}B"
    if abs_v >= 1_000_000:
        return f"{sign}${abs_v / 1_000_000:.2f}M"
    if abs_v >= 1_000:
        return f"{sign}${abs_v / 1_000:.1f}K"
    return f"{sign}${abs_v:.0f}"


def _format_span(first_ms: int, latest_ms: int) -> str:
    if first_ms <= 0 or latest_ms <= 0:
        return "-"
    minutes = (latest_ms - first_ms) / 60_000
    return f"{_format_ts(first_ms)} -> {_format_ts(latest_ms)} ({minutes:.1f}m)"


def _format_ts(ts_ms: int) -> str:
    if ts_ms <= 0:
        return "-"
    return datetime.fromtimestamp(ts_ms / 1000, tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
