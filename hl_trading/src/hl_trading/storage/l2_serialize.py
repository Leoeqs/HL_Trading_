"""Shared L2 record serialization for ClickHouse, local NDJSON, and replay."""

from __future__ import annotations

from typing import Any

import orjson


def l2_record_bytes(ws_msg: dict[str, Any], ingest_ns: int) -> bytes:
    data = ws_msg.get("data") or {}
    exchange_ts = int(data.get("time", 0))
    return orjson.dumps({"exchange_ts": exchange_ts, "ingest_ns": ingest_ns, "raw": ws_msg})
