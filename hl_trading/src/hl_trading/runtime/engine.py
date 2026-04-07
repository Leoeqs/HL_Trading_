"""Feeds → L2 books → strategy → execution; storage, fills, order updates, metrics."""

from __future__ import annotations

import logging
import signal
import threading
import time
from typing import Any

from hl_trading.adapters.hyperliquid_factory import create_exchange, create_info_for_feeds
from hl_trading.book.l2 import PerpL2Book
from hl_trading.config import Settings, get_settings
from hl_trading.domain import PortfolioView
from hl_trading.metrics import L2_APPLY_SECONDS, L2_UPDATES, ensure_metrics_server
from hl_trading.services.execution import ExecutionService
from hl_trading.services.market_data import MarketDataService
from hl_trading.services.portfolio import fetch_portfolio_view
from hl_trading.services.ws_user_parsers import (
    extract_fills_user_channel,
    extract_fills_user_fills,
    extract_order_updates,
)
from hl_trading.storage.hub import StorageHub
from hl_trading.strategies.base import Strategy

logger = logging.getLogger(__name__)


def _storage_hub_wanted(settings: Settings) -> bool:
    return bool(
        settings.postgres_dsn
        or settings.clickhouse_host
        or settings.l2_local_ndjson_path
        or (settings.redis_url and settings.redis_publish_l2)
    )


class TradingEngine:
    def __init__(self, settings: Settings, strategy: Strategy) -> None:
        self._settings = settings
        self._strategy = strategy
        self._info = create_info_for_feeds(settings)
        self._md = MarketDataService(self._info)
        self._exchange = create_exchange(settings)
        self._books: dict[str, PerpL2Book] = {}
        self._hub: StorageHub | None = None
        if _storage_hub_wanted(settings):
            self._hub = StorageHub(settings)
        self._exec = ExecutionService(
            self._exchange,
            settings,
            order_journal=self._hub.order_journal if self._hub else None,
        )
        self._portfolio: PortfolioView | None = None
        self._stop = threading.Event()

    def refresh_portfolio(self) -> PortfolioView:
        self._portfolio = fetch_portfolio_view(self._info, self._settings.account_address)
        return self._portfolio

    def _dispatch_intents(self, intents: list[Any]) -> None:
        if not self._portfolio:
            self.refresh_portfolio()
        assert self._portfolio is not None
        for intent in intents:
            try:
                self._exec.place_limit(self._portfolio, intent, mid_px=None)
            except Exception:
                logger.exception("execution failed for %s", intent)

    def _persist_fills(self, fills: list[dict[str, Any]]) -> None:
        store = self._hub.postgres_store if self._hub else None
        if not store or not fills:
            return
        addr = self._settings.account_address.lower()
        for f in fills:
            store.enqueue_fill(addr, f)

    def _on_user_fills(self, ws_msg: Any) -> None:
        fills = extract_fills_user_fills(
            ws_msg,
            skip_snapshot=not self._settings.ingest_fill_snapshots,
        )
        self._persist_fills(fills)

    def _on_order_updates(self, ws_msg: Any) -> None:
        store = self._hub.postgres_store if self._hub else None
        if not store:
            return
        addr = self._settings.account_address.lower()
        for oid, st in extract_order_updates(ws_msg):
            store.enqueue_order_status(addr, oid, st)

    def _on_l2(self, coin: str):
        def _cb(ws_msg: Any) -> None:
            if ws_msg.get("channel") != "l2Book":
                return
            t0 = time.perf_counter()
            book = self._books.setdefault(coin, PerpL2Book(coin))
            book.apply_ws_message(ws_msg)
            if L2_APPLY_SECONDS is not None:
                L2_APPLY_SECONDS.labels(coin=coin).observe(time.perf_counter() - t0)
            if L2_UPDATES is not None:
                L2_UPDATES.labels(coin=coin).inc()

            ingest_ns = time.time_ns()
            if self._hub:
                self._hub.on_l2_ws_message(ws_msg, ingest_ns)
                self._hub.publish_book(coin, book.to_snapshot_payload())

            if self._portfolio is None:
                self.refresh_portfolio()
            assert self._portfolio is not None
            intents = self._strategy.on_l2_book(coin, book, self._portfolio)
            self._dispatch_intents(intents)

        return _cb

    def _on_bbo(self, coin: str):
        def _cb(msg: Any) -> None:
            if self._portfolio is None:
                self.refresh_portfolio()
            assert self._portfolio is not None
            intents = self._strategy.on_bbo(coin, msg, self._portfolio)
            self._dispatch_intents(intents)

        return _cb

    def _on_user(self, msg: Any) -> None:
        if self._hub and self._hub.postgres_store and self._settings.ingest_fills_from_user_events:
            ch_fills = extract_fills_user_channel(msg)
            self._persist_fills(ch_fills)

        self.refresh_portfolio()
        assert self._portfolio is not None
        intents = self._strategy.on_user_event(msg, self._portfolio)
        self._dispatch_intents(intents)

    def run_forever(self) -> None:
        ensure_metrics_server(self._settings.metrics_port)
        if self._hub:
            self._hub.start()

        self.refresh_portfolio()
        for coin in self._settings.watch_coin_list():
            if self._settings.subscribe_l2:
                self._md.subscribe_l2_book(coin, self._on_l2(coin))
            if self._settings.subscribe_bbo:
                self._md.subscribe_bbo(coin, self._on_bbo(coin))
        self._md.subscribe_user_events(self._settings.account_address, self._on_user)

        if self._hub and self._hub.postgres_store and self._settings.postgres_dsn:
            if self._settings.ingest_fills_ws:
                self._md.subscribe_user_fills(self._settings.account_address, self._on_user_fills)
            if self._settings.track_order_updates:
                self._md.subscribe_order_updates(self._settings.account_address, self._on_order_updates)

        def _handle_sig(*_args: object) -> None:
            logger.warning("shutdown signal")
            self._stop.set()

        signal.signal(signal.SIGINT, _handle_sig)
        signal.signal(signal.SIGTERM, _handle_sig)

        logger.info(
            "engine network=%s dry_run=%s l2=%s bbo=%s coins=%s fills_ws=%s order_updates=%s",
            self._settings.hl_network,
            self._settings.dry_run,
            self._settings.subscribe_l2,
            self._settings.subscribe_bbo,
            self._settings.watch_coin_list(),
            self._settings.ingest_fills_ws,
            self._settings.track_order_updates,
        )
        while not self._stop.is_set():
            time.sleep(0.5)
        if self._hub:
            self._hub.shutdown()
        if self._info.ws_manager:
            self._info.disconnect_websocket()
        logger.info("engine stopped")


def run_default_engine() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    from hl_trading.strategies.null_strategy import NullStrategy

    settings = get_settings()
    TradingEngine(settings, NullStrategy()).run_forever()
