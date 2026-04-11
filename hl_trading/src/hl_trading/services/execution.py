"""Order placement — risk gate, optional dry-run, journal, metrics."""

from __future__ import annotations

import logging
from typing import Any

from hyperliquid.exchange import Exchange
from hyperliquid.utils.signing import OrderType
from hyperliquid.utils.types import Cloid

from hl_trading.adapters.hl_order_response import parse_order_placement_response
from hl_trading.config import Settings
from hl_trading.domain import LimitOrderIntent, PortfolioView
from hl_trading.metrics import ORDERS_SUBMITTED
from hl_trading.services.risk import NotionalLimitRisk, RiskGate
from hl_trading.storage.postgres_journal import OrderJournal

logger = logging.getLogger(__name__)


def _intent_to_order_type(intent: LimitOrderIntent) -> OrderType:
    return {"limit": {"tif": intent.tif}}


class ExecutionService:
    def __init__(
        self,
        exchange: Exchange,
        settings: Settings,
        risk: RiskGate | None = None,
        order_journal: OrderJournal | None = None,
    ) -> None:
        self._exchange = exchange
        self._settings = settings
        self._risk = risk or NotionalLimitRisk(settings)
        self._journal = order_journal

    def place_limit(
        self,
        portfolio: PortfolioView,
        intent: LimitOrderIntent,
        *,
        mid_px: float | None = None,
    ) -> dict[str, Any] | None:
        self._risk.check_new_order(portfolio, intent, mid_px)
        cloid: Cloid | None = None
        if intent.client_order_id_hex:
            cloid = Cloid.from_str(intent.client_order_id_hex)
        response: dict[str, Any] | None = None
        normalized_status: str | None = None
        exchange_oid: int | None = None
        error_message: str | None = None

        if self._settings.dry_run:
            logger.info(
                "[dry_run] would place %s %s %s @ %s ro=%s",
                intent.side,
                intent.size,
                intent.coin,
                intent.limit_px,
                intent.reduce_only,
            )
            normalized_status = "dry_run"
        else:
            is_buy = intent.side == "buy"
            try:
                response = self._exchange.order(
                    intent.coin,
                    is_buy,
                    intent.size,
                    intent.limit_px,
                    _intent_to_order_type(intent),
                    intent.reduce_only,
                    cloid,
                )
            except Exception:
                logger.exception(
                    "exchange.order raised for %s %s %s @ %s",
                    intent.side,
                    intent.size,
                    intent.coin,
                    intent.limit_px,
                )
                raise
            if not isinstance(response, dict):
                logger.error("exchange.order returned non-dict: %r", response)
            elif response.get("status") != "ok":
                logger.warning("exchange HTTP response status=%s: %s", response.get("status"), response)
            normalized_status, exchange_oid, error_message = parse_order_placement_response(response)
            logger.info(
                "order %s %s %s @ %s ro=%s -> %s oid=%s err=%s",
                intent.side,
                intent.size,
                intent.coin,
                intent.limit_px,
                intent.reduce_only,
                normalized_status,
                exchange_oid,
                error_message,
            )

        if self._journal:
            self._journal.enqueue_order_record(
                intent,
                portfolio,
                response,
                dry_run=self._settings.dry_run,
                normalized_status=normalized_status,
                exchange_oid=exchange_oid,
                error_message=error_message,
            )
        if ORDERS_SUBMITTED is not None:
            ORDERS_SUBMITTED.labels(dry_run=str(self._settings.dry_run).lower()).inc()
        if self._settings.dry_run:
            return None
        return response

    def bulk_cancel_by_oid(self, cancels: list[tuple[str, int]]) -> Any:
        """Cancel resting orders by (coin, exchange oid). No-op if empty."""
        if not cancels:
            return None
        if self._settings.dry_run:
            logger.info("[dry_run] would bulk_cancel %s", cancels)
            return None
        reqs: list[dict[str, Any]] = [{"coin": c, "oid": oid} for c, oid in cancels]
        return self._exchange.bulk_cancel(reqs)
