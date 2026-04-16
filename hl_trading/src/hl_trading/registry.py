"""Non-secret trading defaults: watch list, strategy, leverage, network.

Secrets stay in `.env` (`ACCOUNT_ADDRESS`, `API_WALLET_PRIVATE_KEY`). Any value here can
still be overridden by environment variables (see ``Settings`` field aliases).

To run a different setup, edit this module or add a composite strategy and set ``STRATEGY_ENTRYPOINT``.
"""

from __future__ import annotations

from typing import Literal

# Perps to subscribe (L2 + startup leverage). Order does not matter.
WATCH_COINS: tuple[str, ...] = ("LIT", "HYPE")

# ``module.path:ClassName`` — no-arg constructor, same format as ``HL_STRATEGY``.
STRATEGY_ENTRYPOINT = "hl_trading.strategies.lit_hype_depth_strategy:LitHypeDepthStrategy"

# Passed to ``Settings.perp_leverage_map`` — engine calls ``update_leverage`` per coin at start.
PERP_LEVERAGE_MAP = "LIT=5,HYPE=10"

# Default chain when ``HL_NETWORK`` is unset (minimal .env).
DEFAULT_HL_NETWORK: Literal["mainnet", "testnet"] = "mainnet"


def watch_coins_csv() -> str:
    return ",".join(WATCH_COINS)
