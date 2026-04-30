"""Build focused wallet watchlists from coin-specific trade discovery files."""

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
class ParticipantSummary:
    account: str
    trade_count: int = 0
    notional_usd: float = 0.0
    coins: Counter[str] = field(default_factory=Counter)
    coin_notional_usd: defaultdict[str, float] = field(default_factory=lambda: defaultdict(float))
    side_counts: Counter[str] = field(default_factory=Counter)
    counterparties: Counter[str] = field(default_factory=Counter)
    first_trade_time_ms: int = 0
    latest_trade_time_ms: int = 0
    is_seed_wallet: bool = False

    @property
    def coin_breadth(self) -> int:
        return len(self.coins)

    @property
    def counterparty_breadth(self) -> int:
        return len(self.counterparties)

    @property
    def score(self) -> float:
        return (
            self.trade_count * 10.0
            + self.notional_usd / 1_000.0
            + self.coin_breadth * 50.0
            + self.counterparty_breadth * 2.0
            + (100.0 if self.is_seed_wallet else 0.0)
        )

    def to_record(self) -> dict[str, Any]:
        return {
            "account": self.account,
            "score": self.score,
            "trade_count": self.trade_count,
            "notional_usd": self.notional_usd,
            "coins": dict(self.coins),
            "coin_notional_usd": dict(self.coin_notional_usd),
            "side_counts": dict(self.side_counts),
            "counterparty_breadth": self.counterparty_breadth,
            "top_counterparties": self.counterparties.most_common(5),
            "first_trade_time_ms": self.first_trade_time_ms,
            "latest_trade_time_ms": self.latest_trade_time_ms,
            "is_seed_wallet": self.is_seed_wallet,
        }


@dataclass(frozen=True, slots=True)
class ParticipantWatchlistResult:
    path: str
    target_coins: tuple[str, ...]
    line_count: int
    invalid_line_count: int
    large_trade_count: int
    participant_count: int
    seed_wallet_count: int
    selected_wallets: tuple[str, ...]
    participants: tuple[ParticipantSummary, ...]

    def to_record(self, *, top: int = 50) -> dict[str, Any]:
        return {
            "path": self.path,
            "target_coins": list(self.target_coins),
            "line_count": self.line_count,
            "invalid_line_count": self.invalid_line_count,
            "large_trade_count": self.large_trade_count,
            "participant_count": self.participant_count,
            "seed_wallet_count": self.seed_wallet_count,
            "selected_wallet_count": len(self.selected_wallets),
            "selected_wallets": list(self.selected_wallets),
            "top_participants": [p.to_record() for p in self.participants[:top]],
        }


def build_participant_watchlist(
    path: str | Path,
    *,
    target_coins: tuple[str, ...] = ("LIT", "HYPE"),
    seed_wallet_files: tuple[str | Path, ...] = (),
    max_wallets: int = 250,
    min_trades: int = 1,
    min_notional_usd: float = 0.0,
) -> ParticipantWatchlistResult:
    path_obj = Path(path)
    targets = tuple(c.strip().upper() for c in target_coins if c.strip())
    target_set = set(targets)
    seed_wallets = _load_seed_wallets(seed_wallet_files)
    participants: dict[str, ParticipantSummary] = {
        wallet: ParticipantSummary(account=wallet, is_seed_wallet=True) for wallet in seed_wallets
    }
    line_count = 0
    invalid_line_count = 0
    large_trade_count = 0

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
            if record.get("type") != "large_trade":
                continue
            coin = str(record.get("coin", "")).upper()
            if target_set and coin not in target_set:
                continue
            wallets = [str(w).lower() for w in record.get("wallets") or [] if isinstance(w, str)]
            if not wallets:
                continue
            large_trade_count += 1
            notional = _as_float(record.get("notional_usd"))
            side = str(record.get("side", ""))
            trade_time_ms = _as_int(record.get("exchange_time_ms"))
            for wallet in wallets:
                summary = participants.get(wallet)
                if summary is None:
                    summary = ParticipantSummary(account=wallet)
                    participants[wallet] = summary
                summary.trade_count += 1
                summary.notional_usd += notional
                summary.coins[coin] += 1
                summary.coin_notional_usd[coin] += notional
                summary.side_counts[side] += 1
                if trade_time_ms > 0:
                    summary.first_trade_time_ms = (
                        trade_time_ms
                        if summary.first_trade_time_ms == 0
                        else min(summary.first_trade_time_ms, trade_time_ms)
                    )
                    summary.latest_trade_time_ms = max(summary.latest_trade_time_ms, trade_time_ms)
                for other in wallets:
                    other = other.lower()
                    if other != wallet:
                        summary.counterparties[other] += 1

    ranked = tuple(
        sorted(
            (
                p
                for p in participants.values()
                if p.is_seed_wallet or (p.trade_count >= min_trades and p.notional_usd >= min_notional_usd)
            ),
            key=lambda p: p.score,
            reverse=True,
        )
    )
    selected: list[str] = []
    seen: set[str] = set()

    def add_wallets(candidates: list[ParticipantSummary]) -> None:
        for participant in candidates:
            if max_wallets > 0 and len(selected) >= max_wallets:
                return
            if participant.account in seen:
                continue
            selected.append(participant.account)
            seen.add(participant.account)

    # Preserve known refined wallets, then layer in the most active LIT/HYPE-specific participants.
    add_wallets([p for p in ranked if p.is_seed_wallet])
    add_wallets(list(ranked))
    for coin in targets:
        add_wallets(sorted(ranked, key=lambda p: (p.coin_notional_usd.get(coin, 0.0), p.coins.get(coin, 0)), reverse=True))
        add_wallets(sorted(ranked, key=lambda p: (p.coins.get(coin, 0), p.coin_notional_usd.get(coin, 0.0)), reverse=True))

    return ParticipantWatchlistResult(
        path=str(path_obj),
        target_coins=targets,
        line_count=line_count,
        invalid_line_count=invalid_line_count,
        large_trade_count=large_trade_count,
        participant_count=len(participants),
        seed_wallet_count=len(seed_wallets),
        selected_wallets=tuple(selected),
        participants=ranked,
    )


def write_participant_watchlist(path: str | Path, wallets: tuple[str, ...]) -> None:
    path_obj = Path(path)
    path_obj.parent.mkdir(parents=True, exist_ok=True)
    with path_obj.open("w", encoding="utf-8") as fh:
        for wallet in wallets:
            fh.write(wallet + "\n")


def format_participant_watchlist(result: ParticipantWatchlistResult, *, top: int = 50) -> str:
    lines = [
        f"Participant watchlist: {result.path}",
        (
            f"targets={','.join(result.target_coins)} records={result.line_count:,} "
            f"invalid={result.invalid_line_count:,} large_trades={result.large_trade_count:,}"
        ),
        (
            f"participants={result.participant_count:,} seed_wallets={result.seed_wallet_count:,} "
            f"selected={len(result.selected_wallets):,}"
        ),
        "",
        f"Top Participants (top {top})",
    ]
    for idx, participant in enumerate(result.participants[:top], start=1):
        seed = " seed" if participant.is_seed_wallet else ""
        lines.append(
            f"{idx:>3}. {_short_wallet(participant.account)}{seed} score={participant.score:.1f} "
            f"trades={participant.trade_count:,} notional={_fmt_usd(participant.notional_usd)} "
            f"coins={_format_counter(participant.coins, 4)} sides={_format_counter(participant.side_counts, 4)} "
            f"counterparties={participant.counterparty_breadth}"
        )
        if participant.counterparties:
            counterparties = ", ".join(
                f"{_short_wallet(account)}:{count}" for account, count in participant.counterparties.most_common(3)
            )
            lines.append(f"     counterparties={counterparties}")
    lines.extend(
        [
            "",
            "Dataset Plan",
            "- Use this watchlist for slower position polling to identify current LIT/HYPE holders.",
            "- Keep the low-notional discovery stream running to capture new participants entering the market.",
            "- Rebuild this watchlist periodically so the polling set follows the current LIT/HYPE participant set.",
        ]
    )
    return "\n".join(lines)


def _load_seed_wallets(paths: tuple[str | Path, ...]) -> set[str]:
    wallets: set[str] = set()
    for path in paths:
        path_obj = Path(path)
        if not path_obj.exists():
            continue
        for line in path_obj.read_text(encoding="utf-8").splitlines():
            value = line.strip().lower()
            if not value or value.startswith("#"):
                continue
            wallets.add(value)
    return wallets


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


def _format_counter(counter: Counter[str], top: int) -> str:
    if not counter:
        return "-"
    return ", ".join(f"{key}:{value:,}" for key, value in counter.most_common(top))
