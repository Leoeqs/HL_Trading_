"""Parse Hyperliquid `POST /exchange` order responses for OID + normalized status."""

from __future__ import annotations

from typing import Any


def parse_order_placement_response(resp: Any) -> tuple[str, int | None, str | None]:
    """
    Returns ``(normalized_status, exchange_oid, error_message)``.

    Normalized status: ``dry_run``, ``open``, ``filled``, ``rejected``, ``error``, ``unknown``.
    """
    if resp is None:
        return ("unknown", None, None)
    if not isinstance(resp, dict):
        return ("unknown", None, None)
    if resp.get("status") != "ok":
        err = resp.get("response")
        if isinstance(err, str):
            return ("error", None, err)
        return ("error", None, str(err))

    inner = resp.get("response") or {}
    if inner.get("type") != "order":
        return ("unknown", None, f"unexpected response type {inner.get('type')!r}")

    data = inner.get("data") or {}
    statuses = data.get("statuses") or []
    if not statuses:
        return ("unknown", None, "no statuses in response")

    s0 = statuses[0]
    if not isinstance(s0, dict):
        return ("unknown", None, str(s0))

    if "filled" in s0:
        body = s0["filled"]
        oid = body.get("oid") if isinstance(body, dict) else None
        return ("filled", int(oid) if oid is not None else None, None)

    if "resting" in s0:
        body = s0["resting"]
        oid = body.get("oid") if isinstance(body, dict) else None
        return ("open", int(oid) if oid is not None else None, None)

    if "error" in s0:
        err = s0["error"]
        if isinstance(err, str):
            return ("rejected", None, err)
        return ("rejected", None, str(err))

    return ("unknown", None, str(s0))
