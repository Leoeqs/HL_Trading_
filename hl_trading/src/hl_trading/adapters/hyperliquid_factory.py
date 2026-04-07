"""Construct Hyperliquid Info (market data + WS) and Exchange (signing) clients."""

from __future__ import annotations

import eth_account
from eth_account.signers.local import LocalAccount
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils import constants

from hl_trading.config import Settings


def _base_url(settings: Settings) -> str:
    if settings.hl_network == "mainnet":
        return constants.MAINNET_API_URL
    return constants.TESTNET_API_URL


def create_wallet(settings: Settings) -> LocalAccount:
    return eth_account.Account.from_key(settings.api_wallet_private_key.get_secret_value())


def create_info_for_feeds(settings: Settings) -> Info:
    """REST + websocket manager — use for `subscribe` and public REST."""
    return Info(_base_url(settings), skip_ws=False)


def create_info_rest_only(settings: Settings) -> Info:
    """REST only (no websocket thread) — CLI tools, reconciler, light polling."""
    return Info(_base_url(settings), skip_ws=True)


def create_exchange(settings: Settings) -> Exchange:
    """Signed trading client. Nested `exchange.info` uses `skip_ws=True` (REST-only)."""
    wallet = create_wallet(settings)
    return Exchange(
        wallet,
        base_url=_base_url(settings),
        account_address=settings.account_address,
    )
