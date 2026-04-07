"""`OrderJournal` protocol — implemented by `PostgresStore`."""

from __future__ import annotations

from typing import Any, Protocol

from hl_trading.domain import LimitOrderIntent, PortfolioView


class OrderJournal(Protocol):
    def enqueue_order_record(
        self,
        intent: LimitOrderIntent,
        portfolio: PortfolioView,
        exchange_response: Any | None,
        *,
        dry_run: bool,
        normalized_status: str | None,
        exchange_oid: int | None,
        error_message: str | None,
    ) -> None: ...

    def close(self) -> None: ...
