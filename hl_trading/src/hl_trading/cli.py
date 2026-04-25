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
from hl_trading.services.actor_analysis import analyze_actor_ndjson, format_actor_analysis
from hl_trading.services.actor_watch import LargeTradeActorWatcher
from hl_trading.services.portfolio import fetch_portfolio_view
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
    p_aw.add_argument("--network", choices=["mainnet", "testnet"], default=None, help="Defaults to HL_NETWORK or registry")
    p_aw.add_argument("--min-notional", type=float, default=25_000.0, help="Large-trade threshold in USD")
    p_aw.add_argument(
        "--track-wallet",
        action="append",
        default=[],
        help="Wallet to poll for positions/open orders; repeat for multiple wallets",
    )
    p_aw.add_argument("--wallet-poll-sec", type=float, default=5.0, help="REST polling cadence for tracked wallets")
    p_aw.add_argument("--max-wallets", type=int, default=100, help="Maximum wallets to auto-track from trade payloads")
    p_aw.add_argument("--no-auto-track", action="store_true", help="Only poll wallets passed with --track-wallet")
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
    p_aa.add_argument("--json", action="store_true", help="Emit JSON instead of text")
    p_aa.set_defaults(fn=_cmd_analyze_actors)

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
    coins = [x.strip() for x in args.coins.split(",") if x.strip()] if args.coins else list(registry.WATCH_COINS)
    info = Info(base_url, skip_ws=False)
    watcher = LargeTradeActorWatcher(
        info,
        coins=coins,
        min_notional_usd=args.min_notional,
        tracked_wallets=args.track_wallet,
        wallet_poll_interval_s=args.wallet_poll_sec,
        auto_track_trade_wallets=not args.no_auto_track,
        max_tracked_wallets=args.max_wallets,
        output_path=args.output,
    )
    try:
        watcher.run(duration_s=args.duration_sec)
    finally:
        info.disconnect_websocket()


def _cmd_analyze_actors(args: argparse.Namespace) -> None:
    result = analyze_actor_ndjson(args.ndjson)
    if args.json:
        json.dump(result.to_record(top=args.top), sys.stdout, indent=2)
        sys.stdout.write("\n")
        return
    print(format_actor_analysis(result, top=args.top, sort_by=args.sort_by))


if __name__ == "__main__":
    main()
