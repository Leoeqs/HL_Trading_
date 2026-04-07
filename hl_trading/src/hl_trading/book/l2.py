"""Perpetual L2 order book from Hyperliquid `l2Book` websocket snapshots.

`levels[0]` are bids (best bid first, descending price away from mid).
`levels[1]` are asks (best ask first, ascending price away from mid).
See: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterator


@dataclass(slots=True)
class L2LevelView:
    px: float
    sz: float
    n: int


@dataclass(slots=True)
class PerpL2Book:
    coin: str
    time_ms: int = 0
    _bids: dict[float, tuple[float, int]] = field(default_factory=dict)  # px -> (sz, n)
    _asks: dict[float, tuple[float, int]] = field(default_factory=dict)

    def apply_ws_message(self, ws_msg: dict[str, Any]) -> None:
        """Apply full snapshot from `{"channel":"l2Book","data":{...}}`."""
        if ws_msg.get("channel") != "l2Book":
            raise ValueError("expected l2Book channel")
        self.apply_l2_data(ws_msg["data"])

    def apply_l2_data(self, data: dict[str, Any]) -> None:
        """Replace book state from `data` (coin, time, levels)."""
        self.coin = str(data["coin"])
        self.time_ms = int(data["time"])
        levels = data["levels"]
        bids_raw, asks_raw = levels[0], levels[1]
        self._bids.clear()
        self._asks.clear()
        for lvl in bids_raw:
            self._bids[float(lvl["px"])] = (float(lvl["sz"]), int(lvl["n"]))
        for lvl in asks_raw:
            self._asks[float(lvl["px"])] = (float(lvl["sz"]), int(lvl["n"]))

    def best_bid(self) -> L2LevelView | None:
        if not self._bids:
            return None
        px = max(self._bids)
        sz, n = self._bids[px]
        return L2LevelView(px=px, sz=sz, n=n)

    def best_ask(self) -> L2LevelView | None:
        if not self._asks:
            return None
        px = min(self._asks)
        sz, n = self._asks[px]
        return L2LevelView(px=px, sz=sz, n=n)

    def mid(self) -> float | None:
        bb, ba = self.best_bid(), self.best_ask()
        if bb is None or ba is None:
            return None
        return (bb.px + ba.px) / 2.0

    def spread(self) -> float | None:
        bb, ba = self.best_bid(), self.best_ask()
        if bb is None or ba is None:
            return None
        return ba.px - bb.px

    def bids_desc(self, depth: int | None = None) -> Iterator[L2LevelView]:
        """Bids from best (highest px) downward."""
        for px in sorted(self._bids.keys(), reverse=True):
            if depth is not None and depth <= 0:
                break
            sz, n = self._bids[px]
            yield L2LevelView(px=px, sz=sz, n=n)
            if depth is not None:
                depth -= 1

    def asks_asc(self, depth: int | None = None) -> Iterator[L2LevelView]:
        """Asks from best (lowest px) upward."""
        for px in sorted(self._asks.keys()):
            if depth is not None and depth <= 0:
                break
            sz, n = self._asks[px]
            yield L2LevelView(px=px, sz=sz, n=n)
            if depth is not None:
                depth -= 1

    def depth_levels(self) -> tuple[int, int]:
        return len(self._bids), len(self._asks)

    def to_snapshot_payload(self) -> dict[str, Any]:
        """JSON-friendly structure (for Redis / debugging)."""
        return {
            "coin": self.coin,
            "time": self.time_ms,
            "bids": [{"px": px, "sz": self._bids[px][0], "n": self._bids[px][1]} for px in sorted(self._bids, reverse=True)],
            "asks": [{"px": px, "sz": self._asks[px][0], "n": self._asks[px][1]} for px in sorted(self._asks)],
        }
