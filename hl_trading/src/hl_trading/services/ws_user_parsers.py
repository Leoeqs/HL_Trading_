"""Extract fills / order status updates from Hyperliquid websocket messages."""

from __future__ import annotations

from typing import Any


def extract_fills_user_fills(ws_msg: dict[str, Any], *, skip_snapshot: bool) -> list[dict[str, Any]]:
    if ws_msg.get("channel") != "userFills":
        return []
    data = ws_msg.get("data") or {}
    if skip_snapshot and data.get("isSnapshot") is True:
        return []
    fills = data.get("fills") or []
    return [f for f in fills if isinstance(f, dict)]


def extract_fills_user_channel(ws_msg: dict[str, Any]) -> list[dict[str, Any]]:
    """`user` channel payloads may include a ``fills`` key (WsUserEvent)."""
    if ws_msg.get("channel") != "user":
        return []
    data = ws_msg.get("data") or {}
    fills = data.get("fills")
    if not fills:
        return []
    return [f for f in fills if isinstance(f, dict)]


def extract_order_updates(ws_msg: dict[str, Any]) -> list[tuple[int, str]]:
    """Return ``(oid, hl_status)`` from ``orderUpdates`` (`WsOrder[]` in ``data``)."""
    if ws_msg.get("channel") != "orderUpdates":
        return []
    data = ws_msg.get("data")
    rows: list[Any]
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict) and isinstance(data.get("orders"), list):
        rows = data["orders"]
    else:
        return []
    out: list[tuple[int, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        order = row.get("order")
        if not isinstance(order, dict):
            order = row
        oid = order.get("oid")
        status = row.get("status")
        if oid is not None and status:
            out.append((int(oid), str(status)))
    return out


def map_hl_order_status_to_row_status(hl_status: str) -> str:
    s = hl_status.lower()
    if s in ("open", "triggered", "pending"):
        return "open"
    if s in ("filled",):
        return "filled"
    if "cancel" in s or s in ("canceled", "cancelled"):
        return "canceled"
    if s in ("bad", "rejected", "margin", "perp margin"):
        return "rejected"
    return s
