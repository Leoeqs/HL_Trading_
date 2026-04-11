"""Load a ``Strategy`` from ``HL_STRATEGY=module.path:ClassName`` (class must be constructable with no args)."""

from __future__ import annotations

import importlib
import logging

from hl_trading.strategies.base import Strategy
from hl_trading.strategies.null_strategy import NullStrategy

logger = logging.getLogger(__name__)


def load_strategy(entrypoint: str | None) -> Strategy:
    if not entrypoint or not entrypoint.strip():
        return NullStrategy()
    ep = entrypoint.strip()
    if ":" not in ep:
        raise ValueError(
            f"Invalid strategy entrypoint {ep!r}; expected 'module.path:ClassName' (e.g. mypkg.strats:MyStrategy)"
        )
    mod_name, _, qual = ep.partition(":")
    mod_name, qual = mod_name.strip(), qual.strip()
    if not mod_name or not qual:
        raise ValueError(f"Invalid strategy entrypoint {ep!r}")
    mod = importlib.import_module(mod_name)
    cls = getattr(mod, qual)
    instance = cls()
    logger.info("strategy loaded %s", ep)
    return instance  # type: ignore[return-value]
