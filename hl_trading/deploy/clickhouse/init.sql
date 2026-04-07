CREATE DATABASE IF NOT EXISTS hl;

CREATE TABLE IF NOT EXISTS hl.l2_book_raw
(
    exchange_ts UInt64,
    coin LowCardinality(String),
    ingest_ns UInt64,
    raw_json String CODEC(ZSTD(3))
)
ENGINE = MergeTree
ORDER BY (coin, exchange_ts, ingest_ns);
