"""Prometheus metrics — start HTTP server in a daemon thread when METRICS_PORT is set."""

from __future__ import annotations

import logging
import threading
logger = logging.getLogger(__name__)

_started = False
_lock = threading.Lock()

try:
    from prometheus_client import Counter, Histogram, start_http_server

    L2_UPDATES = Counter("hl_l2_book_updates_total", "L2 snapshots applied", ["coin"])
    L2_APPLY_SECONDS = Histogram(
        "hl_l2_book_apply_seconds",
        "Time to apply one L2 snapshot to local book",
        ["coin"],
        buckets=(1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 0.001, 0.005, 0.01),
    )
    ORDERS_SUBMITTED = Counter("hl_orders_submitted_total", "Orders sent or dry-run logged", ["dry_run"])
    RECONCILE_RUNS = Counter("hl_reconcile_runs_total", "Reconciliation runs", ["status"])
    FILLS_INSERTED = Counter("hl_fills_inserted_total", "Fill rows inserted (deduped by hash)")
except ImportError:  # pragma: no cover
    L2_UPDATES = None  # type: ignore[assignment]
    L2_APPLY_SECONDS = None  # type: ignore[assignment]
    ORDERS_SUBMITTED = None  # type: ignore[assignment]
    RECONCILE_RUNS = None  # type: ignore[assignment]
    FILLS_INSERTED = None  # type: ignore[assignment]
    start_http_server = None  # type: ignore[assignment]


def ensure_metrics_server(port: int | None) -> None:
    global _started
    if port is None or start_http_server is None:
        return
    with _lock:
        if _started:
            return
        start_http_server(port)
        _started = True
        logger.info("prometheus metrics listening on :%s", port)
