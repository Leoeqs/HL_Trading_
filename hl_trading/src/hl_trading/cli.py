"""CLI — run, snapshot, replay, reconcile."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from hl_trading.adapters.hyperliquid_factory import create_info_rest_only
from hl_trading.config import get_settings
from hl_trading.reconcile.reconciler import run_reconcile_once
from hl_trading.replay.replay_runner import replay_file
from hl_trading.runtime.engine import run_default_engine
from hl_trading.services.portfolio import fetch_portfolio_view
from hl_trading.strategies.null_strategy import NullStrategy


def main() -> None:
    parser = argparse.ArgumentParser(prog="hl-trade")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="Live engine: L2 books + optional storage + NullStrategy")
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
        NullStrategy(),
        max_events=args.max_events,
        sleep_s=args.sleep,
        log_every=args.log_every,
    )


def _cmd_reconcile(_args: argparse.Namespace) -> None:
    logging.basicConfig(level=logging.INFO)
    settings = get_settings()
    run_reconcile_once(settings)


if __name__ == "__main__":
    main()
