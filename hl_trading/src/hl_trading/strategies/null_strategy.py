"""No-op strategy."""

from __future__ import annotations

from typing import Any

from hl_trading.book.l2 import PerpL2Book
from hl_trading.domain import LimitOrderIntent, PortfolioView


class NullStrategy:
    def on_bbo(self, coin: str, msg: Any, portfolio: PortfolioView) -> list[LimitOrderIntent]:
        return []

    def on_l2_book(self, coin: str, book: PerpL2Book, portfolio: PortfolioView) -> list[LimitOrderIntent]:
        return []

    def on_user_event(self, msg: Any, portfolio: PortfolioView) -> list[LimitOrderIntent]:
        return []
