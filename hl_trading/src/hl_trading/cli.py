"""CLI — run, snapshot, replay, reconcile."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from hyperliquid.info import Info
from hyperliquid.utils import constants

from hl_trading import registry
from hl_trading.adapters.hyperliquid_factory import create_info_rest_only
from hl_trading.config import get_settings
from hl_trading.pnl.rollup import rollup_pnl_daily
from hl_trading.reconcile.reconciler import run_reconcile_once
from hl_trading.replay.replay_runner import replay_file
from hl_trading.runtime.engine import run_default_engine
from hl_trading.services.actor_analysis import (
    analyze_actor_ndjson,
    format_actor_analysis,
    format_actor_discovery_report,
    format_actor_strategy_report,
    select_discovery_watchlist_wallets,
    select_watchlist_wallets,
    write_watchlist,
)
from hl_trading.services.actor_watch import LargeTradeActorWatcher
from hl_trading.services.holder_analysis import analyze_holder_ndjson, format_holder_analysis
from hl_trading.services.live_signal_analysis import analyze_live_signal_ndjson, format_live_signal_analysis
from hl_trading.services.live_wallet_signals import LiveWalletSignalDaemon
from hl_trading.services.portfolio import fetch_portfolio_view
from hl_trading.services.wallet_signals import build_wallet_signal_report, format_wallet_signal_report
from hl_trading.strategies.loader import load_strategy


def main() -> None:
    parser = argparse.ArgumentParser(prog="hl-trade")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="Live engine (strategy from HL_STRATEGY or NullStrategy)")
    p_run.set_defaults(fn=_cmd_run)

    p_sn = sub.add_parser("snapshot", help="Print user_state JSON (REST only)")
    p_sn.set_defaults(fn=_cmd_snapshot)

    p_rp = sub.add_parser("replay", help="Replay L2 from NDJSON (sleep_s=0 for max throughput)")
    p_rp.add_argument("ndjson", type=Path, help="Path to newline-delimited JSON")
    p_rp.add_argument("--max-events", type=int, default=None)
    p_rp.add_argument("--sleep", type=float, default=0.0, help="Seconds between events (0 = fastest)")
    p_rp.add_argument("--log-every", type=int, default=0, help="Log progress every N events")
    p_rp.set_defaults(fn=_cmd_replay)

    p_rc = sub.add_parser("reconcile", help="Compare API open orders vs Postgres journal")
    p_rc.set_defaults(fn=_cmd_reconcile)

    p_pnl = sub.add_parser("pnl-rollup", help="Rebuild pnl_daily from fills (UTC days, rolling window)")
    p_pnl.add_argument("--days", type=int, default=30, help="Lookback calendar days")
    p_pnl.set_defaults(fn=_cmd_pnl_rollup)

    p_aw = sub.add_parser("watch-actors", help="Watch large trades and snapshot tracked wallets")
    p_aw.add_argument("--coins", default=None, help="Comma-separated perps; defaults to WATCH_COINS")
    p_aw.add_argument("--all-perps", action="store_true", help="Subscribe to trades for every perp in Hyperliquid meta")
    p_aw.add_argument("--exclude-coins", default="", help="Comma-separated perps to skip with --all-perps")
    p_aw.add_argument("--network", choices=["mainnet", "testnet"], default=None, help="Defaults to HL_NETWORK or registry")
    p_aw.add_argument("--min-notional", type=float, default=25_000.0, help="Large-trade threshold in USD")
    p_aw.add_argument(
        "--track-wallet",
        action="append",
        default=[],
        help="Wallet to poll for positions/open orders; repeat for multiple wallets",
    )
    p_aw.add_argument(
        "--track-wallet-file",
        type=Path,
        default=None,
        help="File of wallet addresses to poll, one per line",
    )
    p_aw.add_argument("--wallet-poll-sec", type=float, default=5.0, help="REST polling cadence for tracked wallets")
    p_aw.add_argument("--max-wallets", type=int, default=100, help="Maximum wallets to auto-track from trade payloads")
    p_aw.add_argument("--no-auto-track", action="store_true", help="Only poll wallets passed with --track-wallet")
    p_aw.add_argument("--include-backfill", action="store_true", help="Record recent trades sent on websocket subscribe")
    p_aw.add_argument(
        "--initial-backfill-grace-sec",
        type=float,
        default=10.0,
        help="Live-only mode still accepts trades this many seconds before startup",
    )
    p_aw.add_argument("--output", default=None, help="Append NDJSON records to this path; stdout if omitted")
    p_aw.add_argument("--duration-sec", type=float, default=None, help="Optional finite run duration")
    p_aw.set_defaults(fn=_cmd_watch_actors)

    p_aa = sub.add_parser("analyze-actors", help="Summarize actor-watch NDJSON by wallet")
    p_aa.add_argument("ndjson", type=Path, help="Path to actor-watch NDJSON")
    p_aa.add_argument("--top", type=int, default=20, help="Number of wallets to show")
    p_aa.add_argument(
        "--sort-by",
        choices=["attention", "market-maker", "directional"],
        default="attention",
        help="Ranking score to use for text output",
    )
    p_aa.add_argument("--report", action="store_true", help="Emit compact trading-oriented sections")
    p_aa.add_argument("--discovery-report", action="store_true", help="Emit trade-only all-perps discovery report")
    p_aa.add_argument("--export-watchlist", type=Path, default=None, help="Write selected wallet addresses to this file")
    p_aa.add_argument(
        "--export-discovery-watchlist",
        type=Path,
        default=None,
        help="Write top trade-discovery wallets to this file",
    )
    p_aa.add_argument(
        "--watchlist-archetypes",
        default="market_maker,mixed,directional",
        help="Comma-separated archetypes for --export-watchlist",
    )
    p_aa.add_argument("--json", action="store_true", help="Emit JSON instead of text")
    p_aa.set_defaults(fn=_cmd_analyze_actors)

    p_sig = sub.add_parser("wallet-signals", help="Research directional wallet position signals")
    p_sig.add_argument("ndjson", type=Path, help="Path to targeted actor-watch NDJSON")
    p_sig.add_argument("--coins", default="LIT,HYPE,SOL", help="Comma-separated coins to score")
    p_sig.add_argument("--lookback-min", type=float, default=120.0, help="Recent window for signal aggregation")
    p_sig.add_argument("--min-delta-notional", type=float, default=1_000.0, help="Minimum position delta notional")
    p_sig.add_argument("--min-follow-notional", type=float, default=100_000.0, help="Minimum dominant follow notional")
    p_sig.add_argument("--min-follow-wallets", type=int, default=2, help="Minimum wallets on dominant side")
    p_sig.add_argument("--min-imbalance", type=float, default=0.75, help="Minimum follow-side imbalance")
    p_sig.add_argument("--max-opposite-ratio", type=float, default=0.35, help="Maximum opposite/follow ratio")
    p_sig.add_argument("--max-fade-ratio", type=float, default=0.50, help="Maximum adverse-fade/follow ratio")
    p_sig.add_argument("--top-events", type=int, default=8, help="Events to show per coin")
    p_sig.add_argument("--json", action="store_true", help="Emit JSON instead of text")
    p_sig.set_defaults(fn=_cmd_wallet_signals)

    p_live_sig = sub.add_parser("live-wallet-signals", help="Continuously score directional wallet signals")
    p_live_sig.add_argument("--coins", default="LIT,HYPE,SOL", help="Comma-separated coins to score")
    p_live_sig.add_argument("--track-wallet", action="append", default=[], help="Wallet to poll; repeatable")
    p_live_sig.add_argument("--track-wallet-file", type=Path, default=None, help="Wallet file, one address per line")
    p_live_sig.add_argument("--network", choices=["mainnet", "testnet"], default=None, help="Defaults to HL_NETWORK or registry")
    p_live_sig.add_argument("--poll-sec", type=float, default=120.0, help="Wallet polling interval")
    p_live_sig.add_argument("--lookback-min", type=float, default=120.0, help="Signal event window")
    p_live_sig.add_argument("--min-delta-notional", type=float, default=1_000.0, help="Minimum wallet position delta")
    p_live_sig.add_argument("--min-follow-notional", type=float, default=100_000.0, help="Minimum dominant follow notional")
    p_live_sig.add_argument("--min-follow-wallets", type=int, default=2, help="Minimum wallets on dominant side")
    p_live_sig.add_argument("--min-imbalance", type=float, default=0.75, help="Minimum follow-side imbalance")
    p_live_sig.add_argument("--max-opposite-ratio", type=float, default=0.35, help="Maximum opposite/follow ratio")
    p_live_sig.add_argument("--max-fade-ratio", type=float, default=0.50, help="Maximum adverse-fade/follow ratio")
    p_live_sig.add_argument("--output", default=None, help="Append live events/decisions to NDJSON")
    p_live_sig.add_argument("--duration-sec", type=float, default=None, help="Optional finite run duration")
    p_live_sig.set_defaults(fn=_cmd_live_wallet_signals)

    p_live_analysis = sub.add_parser("analyze-live-signals", help="Summarize live wallet signal NDJSON")
    p_live_analysis.add_argument("ndjson", type=Path, help="Path to live-wallet-signals NDJSON")
    p_live_analysis.add_argument("--coins", default="", help="Comma-separated coins to include; default all")
    p_live_analysis.add_argument("--top-wallets", type=int, default=15, help="Number of active wallets to show")
    p_live_analysis.add_argument("--top-entries", type=int, default=15, help="Number of trade entries to show")
    p_live_analysis.add_argument(
        "--horizons-min",
        default="5,15,30,60",
        help="Comma-separated forward-return horizons in minutes",
    )
    p_live_analysis.add_argument("--calibration-horizon-min", type=float, default=15.0, help="Wallet/pattern edge horizon")
    p_live_analysis.add_argument("--min-calibration-events", type=int, default=5, help="Minimum evaluated events per edge")
    p_live_analysis.add_argument("--json", action="store_true", help="Emit JSON instead of text")
    p_live_analysis.set_defaults(fn=_cmd_analyze_live_signals)

    p_holders = sub.add_parser("analyze-holders", help="Analyze current long/short holders from wallet snapshots")
    p_holders.add_argument("ndjson", type=Path, help="Path to actor-watch NDJSON with wallet snapshots")
    p_holders.add_argument("--coins", default="LIT,HYPE", help="Comma-separated coins to include")
    p_holders.add_argument("--top", type=int, default=25, help="Number of holders/movers to show")
    p_holders.add_argument(
        "--min-abs-notional",
        type=float,
        default=0.0,
        help="Minimum absolute position notional to include",
    )
    p_holders.add_argument("--json", action="store_true", help="Emit JSON instead of text")
    p_holders.set_defaults(fn=_cmd_analyze_holders)

    args = parser.parse_args()
    args.fn(args)


def _cmd_run(_args: argparse.Namespace) -> None:
    run_default_engine()


def _cmd_snapshot(_args: argparse.Namespace) -> None:
    logging.basicConfig(level=logging.INFO)
    settings = get_settings()
    info = create_info_rest_only(settings)
    view = fetch_portfolio_view(info, settings.account_address)
    if view.raw is None:
        print("{}")
        return
    json.dump(view.raw, sys.stdout, indent=2)
    sys.stdout.write("\n")


def _cmd_replay(args: argparse.Namespace) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = get_settings()
    replay_file(
        args.ndjson,
        settings,
        load_strategy(get_settings().strategy_entrypoint),
        max_events=args.max_events,
        sleep_s=args.sleep,
        log_every=args.log_every,
    )


def _cmd_reconcile(_args: argparse.Namespace) -> None:
    logging.basicConfig(level=logging.INFO)
    settings = get_settings()
    run_reconcile_once(settings)


def _cmd_pnl_rollup(args: argparse.Namespace) -> None:
    logging.basicConfig(level=logging.INFO)
    settings = get_settings()
    rollup_pnl_daily(settings, lookback_days=args.days)


def _cmd_watch_actors(args: argparse.Namespace) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    network = args.network or os.getenv("HL_NETWORK") or registry.DEFAULT_HL_NETWORK
    base_url = constants.MAINNET_API_URL if network == "mainnet" else constants.TESTNET_API_URL
    info = Info(base_url, skip_ws=False)
    if args.all_perps:
        coins = _fetch_all_perp_coins(info)
        exclude = {x.strip().upper() for x in args.exclude_coins.split(",") if x.strip()}
        if exclude:
            coins = [c for c in coins if c.upper() not in exclude]
    else:
        coins = [x.strip() for x in args.coins.split(",") if x.strip()] if args.coins else list(registry.WATCH_COINS)
    tracked_wallets = list(args.track_wallet)
    if args.track_wallet_file is not None:
        tracked_wallets.extend(_read_wallet_file(args.track_wallet_file))
    watcher = LargeTradeActorWatcher(
        info,
        coins=coins,
        min_notional_usd=args.min_notional,
        tracked_wallets=tracked_wallets,
        wallet_poll_interval_s=args.wallet_poll_sec,
        auto_track_trade_wallets=not args.no_auto_track,
        max_tracked_wallets=args.max_wallets,
        include_backfill=args.include_backfill,
        initial_backfill_grace_s=args.initial_backfill_grace_sec,
        output_path=args.output,
    )
    try:
        watcher.run(duration_s=args.duration_sec)
    finally:
        info.disconnect_websocket()


def _cmd_analyze_actors(args: argparse.Namespace) -> None:
    result = analyze_actor_ndjson(args.ndjson)
    if args.export_watchlist is not None:
        archetypes = tuple(x.strip() for x in args.watchlist_archetypes.split(",") if x.strip())
        wallets = select_watchlist_wallets(result, top_per_archetype=args.top, archetypes=archetypes)
        write_watchlist(args.export_watchlist, wallets)
        print(f"wrote {len(wallets)} wallet(s) to {args.export_watchlist}", file=sys.stderr)
    if args.export_discovery_watchlist is not None:
        wallets = select_discovery_watchlist_wallets(result, top_each=args.top)
        write_watchlist(args.export_discovery_watchlist, wallets)
        print(f"wrote {len(wallets)} wallet(s) to {args.export_discovery_watchlist}", file=sys.stderr)
    if args.json:
        json.dump(result.to_record(top=args.top), sys.stdout, indent=2)
        sys.stdout.write("\n")
        return
    if args.discovery_report:
        print(format_actor_discovery_report(result, top=args.top))
        return
    if args.report:
        print(format_actor_strategy_report(result, top=args.top))
        return
    print(format_actor_analysis(result, top=args.top, sort_by=args.sort_by))


def _cmd_wallet_signals(args: argparse.Namespace) -> None:
    coins = [x.strip() for x in args.coins.split(",") if x.strip()]
    report = build_wallet_signal_report(
        args.ndjson,
        target_coins=coins,
        lookback_minutes=args.lookback_min,
        min_delta_notional=args.min_delta_notional,
        min_follow_notional=args.min_follow_notional,
        min_follow_wallets=args.min_follow_wallets,
        min_imbalance=args.min_imbalance,
        max_opposite_ratio=args.max_opposite_ratio,
        max_adverse_fade_ratio=args.max_fade_ratio,
    )
    if args.json:
        json.dump(report.to_record(), sys.stdout, indent=2)
        sys.stdout.write("\n")
        return
    print(format_wallet_signal_report(report, top_events=args.top_events))


def _cmd_live_wallet_signals(args: argparse.Namespace) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    network = args.network or os.getenv("HL_NETWORK") or registry.DEFAULT_HL_NETWORK
    base_url = constants.MAINNET_API_URL if network == "mainnet" else constants.TESTNET_API_URL
    wallets = list(args.track_wallet)
    if args.track_wallet_file is not None:
        wallets.extend(_read_wallet_file(args.track_wallet_file))
    info = Info(base_url, skip_ws=False)
    daemon = LiveWalletSignalDaemon(
        info,
        coins=[x.strip() for x in args.coins.split(",") if x.strip()],
        wallets=wallets,
        poll_interval_s=args.poll_sec,
        lookback_minutes=args.lookback_min,
        min_delta_notional=args.min_delta_notional,
        min_follow_notional=args.min_follow_notional,
        min_follow_wallets=args.min_follow_wallets,
        min_imbalance=args.min_imbalance,
        max_opposite_ratio=args.max_opposite_ratio,
        max_adverse_fade_ratio=args.max_fade_ratio,
        output_path=args.output,
    )
    try:
        daemon.run(duration_s=args.duration_sec)
    finally:
        info.disconnect_websocket()


def _cmd_analyze_live_signals(args: argparse.Namespace) -> None:
    coins = tuple(x.strip().upper() for x in args.coins.split(",") if x.strip())
    horizons = tuple(float(x.strip()) for x in args.horizons_min.split(",") if x.strip())
    analysis = analyze_live_signal_ndjson(
        args.ndjson,
        target_coins=coins,
        horizons_minutes=horizons,
        calibration_horizon_minutes=args.calibration_horizon_min,
        min_calibration_events=args.min_calibration_events,
        top_wallets=args.top_wallets,
    )
    if args.json:
        json.dump(analysis.to_record(top_wallets=args.top_wallets, top_entries=args.top_entries), sys.stdout, indent=2)
        sys.stdout.write("\n")
        return
    print(format_live_signal_analysis(analysis, top_wallets=args.top_wallets, top_entries=args.top_entries))


def _cmd_analyze_holders(args: argparse.Namespace) -> None:
    coins = tuple(x.strip().upper() for x in args.coins.split(",") if x.strip())
    result = analyze_holder_ndjson(
        args.ndjson,
        target_coins=coins,
        min_abs_notional_usd=args.min_abs_notional,
    )
    if args.json:
        json.dump(result.to_record(top=args.top), sys.stdout, indent=2)
        sys.stdout.write("\n")
        return
    print(format_holder_analysis(result, top=args.top))


def _read_wallet_file(path: Path) -> list[str]:
    wallets: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        value = line.strip()
        if not value or value.startswith("#"):
            continue
        wallets.append(value)
    return wallets


def _fetch_all_perp_coins(info: Info) -> list[str]:
    meta = info.meta()
    universe = meta.get("universe") if isinstance(meta, dict) else None
    if not isinstance(universe, list):
        raise RuntimeError("Hyperliquid meta response did not contain universe")
    coins: list[str] = []
    for asset in universe:
        if not isinstance(asset, dict):
            continue
        name = asset.get("name")
        if not isinstance(name, str) or not name:
            continue
        if asset.get("isDelisted") is True:
            continue
        coins.append(name)
    if not coins:
        raise RuntimeError("Hyperliquid meta universe was empty")
    return coins


if __name__ == "__main__":
    main()
