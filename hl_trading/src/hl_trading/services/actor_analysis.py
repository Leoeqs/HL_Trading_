"""Analyze actor-watch NDJSON into wallet-level behavior summaries."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


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
class WalletActorSummary:
    account: str
    trade_count: int = 0
    trade_notional_usd: float = 0.0
    side_counts: Counter[str] = field(default_factory=Counter)
    side_notional_usd: defaultdict[str, float] = field(default_factory=lambda: defaultdict(float))
    coins: Counter[str] = field(default_factory=Counter)
    counterparties: Counter[str] = field(default_factory=Counter)
    first_trade_time_ms: int | None = None
    last_trade_time_ms: int | None = None

    snapshot_count: int = 0
    latest_snapshot_time_ms: int | None = None
    latest_positions: dict[str, float] = field(default_factory=dict)
    latest_open_order_count: int = 0
    latest_bid_notional_usd: float = 0.0
    latest_ask_notional_usd: float = 0.0

    feature_count: int = 0
    total_added_order_count: int = 0
    total_removed_order_count: int = 0
    total_possible_replace_count: int = 0
    max_open_order_count: int = 0
    quote_refresh_score_sum: float = 0.0
    size_repetition_score_sum: float = 0.0

    @property
    def avg_quote_refresh_score(self) -> float:
        return self.quote_refresh_score_sum / self.feature_count if self.feature_count else 0.0

    @property
    def avg_size_repetition_score(self) -> float:
        return self.size_repetition_score_sum / self.feature_count if self.feature_count else 0.0

    @property
    def visible_liquidity_usd(self) -> float:
        return self.latest_bid_notional_usd + self.latest_ask_notional_usd

    @property
    def two_sided_balance(self) -> float:
        if self.visible_liquidity_usd <= 0.0:
            return 0.0
        return min(self.latest_bid_notional_usd, self.latest_ask_notional_usd) / max(
            self.latest_bid_notional_usd,
            self.latest_ask_notional_usd,
        )

    @property
    def traded_coin_abs_position(self) -> float:
        coins = set(self.coins)
        return sum(abs(size) for coin, size in self.latest_positions.items() if coin in coins)

    @property
    def market_maker_score(self) -> float:
        """Ranks wallets with broad two-sided quoting and frequent order refreshes."""
        return (
            self.max_open_order_count * 0.5
            + self.visible_liquidity_usd / 100_000.0
            + self.two_sided_balance * 25.0
            + self.total_possible_replace_count * 1.5
            + self.avg_quote_refresh_score * 20.0
        )

    @property
    def directional_score(self) -> float:
        """Ranks wallets that appear more position/flow driven than quote-inventory driven."""
        low_order_bonus = 20.0 if self.latest_open_order_count <= 50 else 0.0
        active_position_bonus = min(self.traded_coin_abs_position / 1_000.0, 100.0)
        return (
            self.trade_count * 5.0
            + self.trade_notional_usd / 1_000.0
            + active_position_bonus
            + low_order_bonus
            + self.avg_size_repetition_score * 10.0
        )

    @property
    def archetype(self) -> str:
        mm = self.market_maker_score
        directional = self.directional_score
        if mm >= 100.0 and directional >= 30.0:
            return "mixed"
        if mm >= 100.0:
            return "market_maker"
        if directional >= 30.0:
            return "directional"
        return "unknown"

    @property
    def attention_score(self) -> float:
        """Heuristic ranker for manual review, not a trading signal."""
        return max(self.market_maker_score, self.directional_score)

    def to_record(self) -> dict[str, Any]:
        return {
            "account": self.account,
            "archetype": self.archetype,
            "attention_score": self.attention_score,
            "market_maker_score": self.market_maker_score,
            "directional_score": self.directional_score,
            "trade_count": self.trade_count,
            "trade_notional_usd": self.trade_notional_usd,
            "side_counts": dict(self.side_counts),
            "side_notional_usd": dict(self.side_notional_usd),
            "coins": dict(self.coins),
            "top_counterparties": self.counterparties.most_common(5),
            "first_trade_time_ms": self.first_trade_time_ms,
            "last_trade_time_ms": self.last_trade_time_ms,
            "snapshot_count": self.snapshot_count,
            "latest_snapshot_time_ms": self.latest_snapshot_time_ms,
            "latest_positions": self.latest_positions,
            "latest_open_order_count": self.latest_open_order_count,
            "latest_bid_notional_usd": self.latest_bid_notional_usd,
            "latest_ask_notional_usd": self.latest_ask_notional_usd,
            "visible_liquidity_usd": self.visible_liquidity_usd,
            "two_sided_balance": self.two_sided_balance,
            "traded_coin_abs_position": self.traded_coin_abs_position,
            "feature_count": self.feature_count,
            "total_added_order_count": self.total_added_order_count,
            "total_removed_order_count": self.total_removed_order_count,
            "total_possible_replace_count": self.total_possible_replace_count,
            "max_open_order_count": self.max_open_order_count,
            "avg_quote_refresh_score": self.avg_quote_refresh_score,
            "avg_size_repetition_score": self.avg_size_repetition_score,
        }


@dataclass(frozen=True, slots=True)
class ActorAnalysisResult:
    path: str
    line_count: int
    invalid_line_count: int
    large_trade_count: int
    wallet_snapshot_count: int
    behavior_feature_count: int
    wallets: list[WalletActorSummary]

    def to_record(self, *, top: int | None = None) -> dict[str, Any]:
        wallets = self.wallets[:top] if top is not None else self.wallets
        return {
            "path": self.path,
            "line_count": self.line_count,
            "invalid_line_count": self.invalid_line_count,
            "large_trade_count": self.large_trade_count,
            "wallet_snapshot_count": self.wallet_snapshot_count,
            "behavior_feature_count": self.behavior_feature_count,
            "wallet_count": len(self.wallets),
            "wallets": [w.to_record() for w in wallets],
        }


def analyze_actor_ndjson(path: str | Path) -> ActorAnalysisResult:
    path_obj = Path(path)
    summaries: dict[str, WalletActorSummary] = {}
    line_count = 0
    invalid_line_count = 0
    large_trade_count = 0
    wallet_snapshot_count = 0
    behavior_feature_count = 0

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
                large_trade_count += 1
                _apply_large_trade(summaries, record)
            elif typ == "wallet_snapshot":
                wallet_snapshot_count += 1
                _apply_wallet_snapshot(summaries, record)
            elif typ == "wallet_behavior_features":
                behavior_feature_count += 1
                _apply_behavior_features(summaries, record)

    wallets = sorted(summaries.values(), key=lambda s: s.attention_score, reverse=True)
    return ActorAnalysisResult(
        path=str(path_obj),
        line_count=line_count,
        invalid_line_count=invalid_line_count,
        large_trade_count=large_trade_count,
        wallet_snapshot_count=wallet_snapshot_count,
        behavior_feature_count=behavior_feature_count,
        wallets=wallets,
    )


def format_actor_analysis(result: ActorAnalysisResult, *, top: int = 20, sort_by: str = "attention") -> str:
    wallets = _sorted_wallets(result.wallets, sort_by)
    lines = [
        f"Actor analysis: {result.path}",
        (
            f"records={result.line_count} invalid={result.invalid_line_count} "
            f"large_trades={result.large_trade_count} snapshots={result.wallet_snapshot_count} "
            f"features={result.behavior_feature_count} wallets={len(result.wallets)}"
        ),
        "",
        "rank type          score     mm    dir wallet                                     trades notional_usd open bid_usd ask_usd repl q_refresh pos",
    ]
    for idx, wallet in enumerate(wallets[:top], start=1):
        pos = _format_positions(wallet.latest_positions)
        lines.append(
            f"{idx:>4} "
            f"{wallet.archetype:<13} "
            f"{wallet.attention_score:>7.1f} "
            f"{wallet.market_maker_score:>6.1f} "
            f"{wallet.directional_score:>6.1f} "
            f"{wallet.account:<42} "
            f"{wallet.trade_count:>6} "
            f"{wallet.trade_notional_usd:>12.0f} "
            f"{wallet.latest_open_order_count:>4} "
            f"{wallet.latest_bid_notional_usd:>7.0f} "
            f"{wallet.latest_ask_notional_usd:>7.0f} "
            f"{wallet.total_possible_replace_count:>4} "
            f"{wallet.avg_quote_refresh_score:>9.2f} "
            f"{pos}"
        )
    return "\n".join(lines)


def format_actor_strategy_report(result: ActorAnalysisResult, *, top: int = 5) -> str:
    """Human-readable research report for trading review."""
    counts = Counter(w.archetype for w in result.wallets)
    lines = [
        f"Actor strategy report: {result.path}",
        (
            f"records={result.line_count:,} invalid={result.invalid_line_count:,} "
            f"large_trades={result.large_trade_count:,} snapshots={result.wallet_snapshot_count:,} "
            f"features={result.behavior_feature_count:,} wallets={len(result.wallets):,}"
        ),
        (
            "archetypes="
            + ", ".join(f"{name}:{counts.get(name, 0)}" for name in ("market_maker", "mixed", "directional", "unknown"))
        ),
        "",
        "Market Makers To Watch",
    ]
    lines.extend(_format_wallet_section(_top_wallets(result.wallets, "market_maker", "market-maker", top)))
    lines.extend(["", "Mixed Liquidity/Flow Actors"])
    lines.extend(_format_wallet_section(_top_wallets(result.wallets, "mixed", "attention", top)))
    lines.extend(["", "Directional Wallets To Watch"])
    lines.extend(_format_wallet_section(_top_wallets(result.wallets, "directional", "directional", top)))
    lines.extend(["", "Trading Read"])
    lines.extend(_format_trading_read(result.wallets))
    return "\n".join(lines)


def _sorted_wallets(wallets: list[WalletActorSummary], sort_by: str) -> list[WalletActorSummary]:
    if sort_by == "market-maker":
        key = lambda s: s.market_maker_score
    elif sort_by == "directional":
        key = lambda s: s.directional_score
    else:
        key = lambda s: s.attention_score
    return sorted(wallets, key=key, reverse=True)


def _top_wallets(
    wallets: list[WalletActorSummary],
    archetype: str,
    sort_by: str,
    top: int,
) -> list[WalletActorSummary]:
    matching = [w for w in wallets if w.archetype == archetype]
    return _sorted_wallets(matching, sort_by)[:top]


def _format_wallet_section(wallets: list[WalletActorSummary]) -> list[str]:
    if not wallets:
        return ["  none"]
    lines: list[str] = []
    for idx, wallet in enumerate(wallets, start=1):
        lines.append(
            "  "
            f"{idx}. {_short_wallet(wallet.account)} "
            f"score={wallet.attention_score:.1f} "
            f"mm={wallet.market_maker_score:.1f} "
            f"dir={wallet.directional_score:.1f} "
            f"trades={wallet.trade_count} "
            f"notional={_fmt_usd(wallet.trade_notional_usd)}"
        )
        lines.append(
            "     "
            f"open={wallet.latest_open_order_count} "
            f"bid={_fmt_usd(wallet.latest_bid_notional_usd)} "
            f"ask={_fmt_usd(wallet.latest_ask_notional_usd)} "
            f"replaces={wallet.total_possible_replace_count:,} "
            f"refresh={wallet.avg_quote_refresh_score:.2f} "
            f"balance={wallet.two_sided_balance:.2f}"
        )
        lines.append(f"     positions={_format_positions(wallet.latest_positions)}")
    return lines


def _format_trading_read(wallets: list[WalletActorSummary]) -> list[str]:
    directional = _top_wallets(wallets, "directional", "directional", 3)
    market_makers = _top_wallets(wallets, "market_maker", "market-maker", 3)
    mixed = _top_wallets(wallets, "mixed", "attention", 3)
    lines = [
        "- Treat market_maker wallets as liquidity-map inputs: watch bid/ask reloads, pulls, and imbalance changes.",
        "- Treat directional wallets as flow/position inputs: watch whether their LIT exposure grows, flips, or unwinds.",
        "- Treat mixed wallets carefully: they may be market makers with inventory, not pure directional conviction.",
    ]
    if market_makers:
        names = ", ".join(_short_wallet(w.account) for w in market_makers)
        lines.append(f"- Top liquidity actors right now: {names}.")
    if directional:
        names = ", ".join(_short_wallet(w.account) for w in directional)
        lines.append(f"- Top directional candidates right now: {names}.")
    if mixed:
        names = ", ".join(_short_wallet(w.account) for w in mixed)
        lines.append(f"- Top mixed actors right now: {names}.")
    return lines


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


def _summary_for(summaries: dict[str, WalletActorSummary], account: str) -> WalletActorSummary:
    normalized = account.lower()
    summary = summaries.get(normalized)
    if summary is None:
        summary = WalletActorSummary(account=normalized)
        summaries[normalized] = summary
    return summary


def _apply_large_trade(summaries: dict[str, WalletActorSummary], record: dict[str, Any]) -> None:
    wallets = [str(w).lower() for w in record.get("wallets") or [] if isinstance(w, str)]
    if not wallets:
        return
    coin = str(record.get("coin", "")).upper()
    side = str(record.get("side", ""))
    notional = _as_float(record.get("notional_usd"))
    exchange_time_ms = _as_int(record.get("exchange_time_ms"))

    for wallet in wallets:
        summary = _summary_for(summaries, wallet)
        summary.trade_count += 1
        summary.trade_notional_usd += notional
        summary.side_counts[side] += 1
        summary.side_notional_usd[side] += notional
        if coin:
            summary.coins[coin] += 1
        if exchange_time_ms:
            if summary.first_trade_time_ms is None or exchange_time_ms < summary.first_trade_time_ms:
                summary.first_trade_time_ms = exchange_time_ms
            if summary.last_trade_time_ms is None or exchange_time_ms > summary.last_trade_time_ms:
                summary.last_trade_time_ms = exchange_time_ms
        for other in wallets:
            if other != wallet:
                summary.counterparties[other] += 1


def _apply_wallet_snapshot(summaries: dict[str, WalletActorSummary], record: dict[str, Any]) -> None:
    account = record.get("account")
    if not isinstance(account, str):
        return
    summary = _summary_for(summaries, account)
    summary.snapshot_count += 1
    observed_at_ms = _as_int(record.get("observed_at_ms"))
    if summary.latest_snapshot_time_ms is not None and observed_at_ms < summary.latest_snapshot_time_ms:
        return
    summary.latest_snapshot_time_ms = observed_at_ms

    positions = record.get("positions")
    if isinstance(positions, dict):
        summary.latest_positions = {str(k): _as_float(v) for k, v in positions.items()}

    open_orders = record.get("open_orders") or []
    summary.latest_open_order_count = len(open_orders) if isinstance(open_orders, list) else 0
    bid_notional = 0.0
    ask_notional = 0.0
    if isinstance(open_orders, list):
        for order in open_orders:
            if not isinstance(order, dict):
                continue
            side = str(order.get("side", "")).lower()
            notional = _as_float(order.get("notional_usd"))
            if side in {"b", "bid", "buy"}:
                bid_notional += notional
            elif side in {"a", "ask", "sell"}:
                ask_notional += notional
    summary.latest_bid_notional_usd = bid_notional
    summary.latest_ask_notional_usd = ask_notional


def _apply_behavior_features(summaries: dict[str, WalletActorSummary], record: dict[str, Any]) -> None:
    account = record.get("account")
    if not isinstance(account, str):
        return
    summary = _summary_for(summaries, account)
    summary.feature_count += 1
    summary.total_added_order_count += _as_int(record.get("added_order_count"))
    summary.total_removed_order_count += _as_int(record.get("removed_order_count"))
    summary.total_possible_replace_count += _as_int(record.get("possible_replace_count"))
    summary.max_open_order_count = max(summary.max_open_order_count, _as_int(record.get("open_order_count")))
    summary.quote_refresh_score_sum += _as_float(record.get("quote_refresh_score"))
    summary.size_repetition_score_sum += _as_float(record.get("size_repetition_score"))


def _format_positions(positions: dict[str, float]) -> str:
    active = [(coin, size) for coin, size in positions.items() if abs(size) > 0.00000001]
    if not active:
        return "-"
    return ",".join(f"{coin}:{size:g}" for coin, size in sorted(active)[:3])
