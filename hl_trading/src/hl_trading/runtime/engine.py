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
from hl_trading.strategies.loader import load_strategy

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
        self._portfolio_lock = threading.Lock()
        self._portfolio_refresh_inflight = False
        self._last_portfolio_refresh_monotonic = 0.0
        self._stop = threading.Event()
        self._l2_tick_count = 0
        self._last_l2_heartbeat_monotonic = 0.0
        self._last_mid_drift_cancel_at = 0.0

    def refresh_portfolio(self) -> PortfolioView:
        portfolio = fetch_portfolio_view(self._info, self._settings.account_address)
        with self._portfolio_lock:
            self._portfolio = portfolio
            self._last_portfolio_refresh_monotonic = time.monotonic()
        return portfolio

    def _current_portfolio(self) -> PortfolioView | None:
        with self._portfolio_lock:
            return self._portfolio

    def _refresh_portfolio_async(self) -> None:
        try:
            self.refresh_portfolio()
        except Exception:
            logger.exception("background portfolio refresh failed")
        finally:
            with self._portfolio_lock:
                self._portfolio_refresh_inflight = False

    def _schedule_portfolio_refresh(self, *, force: bool = False) -> None:
        now = time.monotonic()
        with self._portfolio_lock:
            if self._portfolio_refresh_inflight:
                return
            if not force and now - self._last_portfolio_refresh_monotonic < 0.25:
                return
            self._portfolio_refresh_inflight = True
        threading.Thread(
            target=self._refresh_portfolio_async,
            name="portfolio-refresh",
            daemon=True,
        ).start()

    def _dispatch_intents(self, intents: list[Any]) -> None:
        portfolio = self._current_portfolio()
        if portfolio is None:
            portfolio = self.refresh_portfolio()
        for intent in intents:
            try:
                self._exec.place_limit(portfolio, intent, mid_px=None)
            except Exception:
                logger.exception("execution failed for %s", intent)

    def _cancel_orders_past_mid_drift(self, coin: str, book: PerpL2Book) -> None:
        """Cancel resting limits when |mid - limitPx| exceeds configured USD drift (e.g. 0.05)."""
        drift = self._settings.cancel_on_mid_drift_usd
        if drift <= 0:
            return
        mid = book.mid()
        if mid is None:
            return
        portfolio = self._current_portfolio()
        if portfolio is None or not isinstance(portfolio.raw, dict):
            return
        stale: list[tuple[str, int]] = []
        seen: set[int] = set()
        for row in portfolio.raw.get("openOrders") or []:
            if not isinstance(row, dict):
                continue
            o = row.get("order") if isinstance(row.get("order"), dict) else row
            if not isinstance(o, dict):
                continue
            if str(o.get("coin", "")) != coin:
                continue
            try:
                oid = int(o.get("oid", 0))
                lp = float(o.get("limitPx", 0))
            except (TypeError, ValueError):
                continue
            if oid <= 0 or oid in seen:
                continue
            if abs(mid - lp) > drift:
                seen.add(oid)
                stale.append((coin, oid))
        if not stale:
            return
        now = time.monotonic()
        if now - self._last_mid_drift_cancel_at < 0.15:
            return
        self._last_mid_drift_cancel_at = now
        try:
            self._exec.bulk_cancel_by_oid(stale)
            logger.info("mid drift: canceled %d resting order(s)", len(stale))
        except Exception:
            logger.exception("bulk_cancel failed")
            return
        try:
            self.refresh_portfolio()
        except Exception:
            logger.exception("refresh_portfolio after mid-drift cancel failed")

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
        if fills:
            logger.info("userFills: received %d fill(s)", len(fills))
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

            self._cancel_orders_past_mid_drift(coin, book)

            portfolio = self._current_portfolio()
            if portfolio is None:
                portfolio = self.refresh_portfolio()
            intents = self._strategy.on_l2_book(coin, book, portfolio)
            self._l2_tick_count += 1
            now = time.monotonic()
            if now - self._last_l2_heartbeat_monotonic >= 15.0:
                self._last_l2_heartbeat_monotonic = now
                db, da = book.depth_levels()
                bb = book.best_bid()
                logger.debug(
                    "l2 heartbeat #%d coin=%s book_depth=%d/%d mid=%s best_bid=%s strategy_intents=%d",
                    self._l2_tick_count,
                    coin,
                    db,
                    da,
                    book.mid(),
                    (bb.px, bb.sz) if bb else None,
                    len(intents),
                )
            self._dispatch_intents(intents)

        return _cb

    def _on_bbo(self, coin: str):
        def _cb(msg: Any) -> None:
            portfolio = self._current_portfolio()
            if portfolio is None:
                portfolio = self.refresh_portfolio()
            intents = self._strategy.on_bbo(coin, msg, portfolio)
            self._dispatch_intents(intents)

        return _cb

    def _on_user(self, msg: Any) -> None:
        if self._hub and self._hub.postgres_store and self._settings.ingest_fills_from_user_events:
            ch_fills = extract_fills_user_channel(msg)
            self._persist_fills(ch_fills)

        portfolio = self._current_portfolio()
        if portfolio is None:
            portfolio = self.refresh_portfolio()
        else:
            self._schedule_portfolio_refresh(force=True)
        intents = self._strategy.on_user_event(msg, portfolio)
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

        lev = self._settings.initial_perp_leverage
        if lev is not None:
            for coin in self._settings.watch_coin_list():
                if self._settings.dry_run:
                    logger.info("[dry_run] would set %s cross leverage to %sx", coin, lev)
                else:
                    try:
                        self._exchange.update_leverage(lev, coin, True)
                        logger.info("set %s cross leverage to %sx", coin, lev)
                    except Exception:
                        logger.exception("update_leverage failed for %s", coin)

        logger.info(
            "engine network=%s dry_run=%s l2=%s bbo=%s coins=%s fills_ws=%s order_updates=%s portfolio_refresh_s=%s cancel_mid_drift_usd=%s",
            self._settings.hl_network,
            self._settings.dry_run,
            self._settings.subscribe_l2,
            self._settings.subscribe_bbo,
            self._settings.watch_coin_list(),
            self._settings.ingest_fills_ws,
            self._settings.track_order_updates,
            self._settings.portfolio_refresh_interval_sec,
            self._settings.cancel_on_mid_drift_usd,
        )
        last_periodic_pf = time.monotonic()
        interval = self._settings.portfolio_refresh_interval_sec
        while not self._stop.is_set():
            time.sleep(0.5)
            if interval > 0 and time.monotonic() - last_periodic_pf >= interval:
                last_periodic_pf = time.monotonic()
                try:
                    self.refresh_portfolio()
                except Exception:
                    logger.exception("periodic portfolio refresh failed")
        if self._hub:
            self._hub.shutdown()
        if self._info.ws_manager:
            self._info.disconnect_websocket()
        logger.info("engine stopped")


def run_default_engine() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = get_settings()
    TradingEngine(settings, load_strategy(settings.strategy_entrypoint)).run_forever()
