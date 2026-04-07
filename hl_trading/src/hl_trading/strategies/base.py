"""Strategy interface — order intents from market / book / user events."""

from __future__ import annotations

from typing import Any, Protocol

from hl_trading.book.l2 import PerpL2Book
from hl_trading.domain import LimitOrderIntent, PortfolioView


class Strategy(Protocol):
    def on_bbo(self, coin: str, msg: Any, portfolio: PortfolioView) -> list[LimitOrderIntent]:
        """BBO websocket payload for `coin`."""
        ...

    def on_l2_book(self, coin: str, book: PerpL2Book, portfolio: PortfolioView) -> list[LimitOrderIntent]:
        """Local L2 book after applying latest `l2Book` snapshot."""
        ...

    def on_user_event(self, msg: Any, portfolio: PortfolioView) -> list[LimitOrderIntent]:
        """User websocket payload (fills, liquidations, …)."""
        ...
