"""Lifecycle for optional storage backends (Postgres / ClickHouse / Redis / local file)."""

from __future__ import annotations

from typing import Any

from hl_trading.config import Settings
from hl_trading.storage.clickhouse_l2 import ClickHouseL2Writer
from hl_trading.storage.file_l2 import FileL2Writer
from hl_trading.storage.l2_serialize import l2_record_bytes
from hl_trading.storage.postgres_journal import PostgresOrderJournal
from hl_trading.storage.redis_books import RedisBookMirror


class StorageHub:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._pg: PostgresOrderJournal | None = None
        self._ch: ClickHouseL2Writer | None = None
        self._file: FileL2Writer | None = None
        self._redis: RedisBookMirror | None = None

    def start(self) -> None:
        if self._settings.postgres_dsn:
            self._pg = PostgresOrderJournal(self._settings.postgres_dsn)
            self._pg.start()
        if self._settings.clickhouse_host:
            self._ch = ClickHouseL2Writer(
                host=self._settings.clickhouse_host,
                port=self._settings.clickhouse_port,
                username=self._settings.clickhouse_user,
                password=self._settings.clickhouse_password.get_secret_value()
                if self._settings.clickhouse_password
                else "",
                database=self._settings.clickhouse_database,
            )
            self._ch.start()
        if self._settings.l2_local_ndjson_path:
            self._file = FileL2Writer(self._settings.l2_local_ndjson_path)
            self._file.start()
        if self._settings.redis_url:
            self._redis = RedisBookMirror(self._settings.redis_url)

    def shutdown(self) -> None:
        if self._ch:
            self._ch.close()
        if self._file:
            self._file.close()
        if self._pg:
            self._pg.close()

    @property
    def order_journal(self) -> PostgresOrderJournal | None:
        return self._pg

    def on_l2_ws_message(self, ws_msg: dict[str, Any], ingest_ns: int) -> None:
        if ws_msg.get("channel") != "l2Book":
            return
        payload = l2_record_bytes(ws_msg, ingest_ns)
        if self._ch:
            self._ch.enqueue_payload(payload)
        if self._file:
            self._file.enqueue_payload(payload)

    def publish_book(self, coin: str, payload: dict) -> None:
        if self._redis and self._settings.redis_publish_l2:
            self._redis.publish_book(coin, payload)
