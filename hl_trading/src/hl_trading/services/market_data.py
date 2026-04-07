"""Websocket subscriptions — thin wrapper over `Info.subscribe` with bookkeeping."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from hyperliquid.info import Info
from hyperliquid.utils.types import Subscription

logger = logging.getLogger(__name__)

WsCallback = Callable[[Any], None]


class MarketDataService:
    def __init__(self, info: Info) -> None:
        self._info = info
        self._sub_ids: list[int] = []

    def subscribe_bbo(self, coin: str, on_message: WsCallback) -> int:
        sub: Subscription = {"type": "bbo", "coin": coin}
        sid = self._info.subscribe(sub, on_message)
        self._sub_ids.append(sid)
        logger.info("subscribed bbo coin=%s id=%s", coin, sid)
        return sid

    def subscribe_l2_book(self, coin: str, on_message: WsCallback) -> int:
        sub: Subscription = {"type": "l2Book", "coin": coin}
        sid = self._info.subscribe(sub, on_message)
        self._sub_ids.append(sid)
        logger.info("subscribed l2Book coin=%s id=%s", coin, sid)
        return sid

    def subscribe_user_events(self, user: str, on_message: WsCallback) -> int:
        sub: Subscription = {"type": "userEvents", "user": user}
        sid = self._info.subscribe(sub, on_message)
        self._sub_ids.append(sid)
        logger.info("subscribed userEvents user=%s id=%s", user, sid)
        return sid

    def subscribe_order_updates(self, user: str, on_message: WsCallback) -> int:
        sub: Subscription = {"type": "orderUpdates", "user": user}
        sid = self._info.subscribe(sub, on_message)
        self._sub_ids.append(sid)
        logger.info("subscribed orderUpdates user=%s id=%s", user, sid)
        return sid
