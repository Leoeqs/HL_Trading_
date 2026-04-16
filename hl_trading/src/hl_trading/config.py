"""Application settings — environment variables (see `.env.example`)."""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import AliasChoices, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    hl_network: Literal["mainnet", "testnet"] = "testnet"
    account_address: str = Field(
        ...,
        description="Main wallet address (0x...) for user_state and WS user channels",
    )
    api_wallet_private_key: SecretStr = Field(
        ...,
        description="Private key for signing (API agent or main wallet)",
    )

    watch_coins: str = Field(
        default="BTC",
        description="Comma-separated perp symbols, e.g. BTC,ETH",
    )
    dry_run: bool = True

    subscribe_l2: bool = Field(default=True, description="Subscribe to l2Book and maintain local L2")
    subscribe_bbo: bool = Field(default=False, description="Also subscribe to bbo (extra bandwidth)")

    max_position_usd_per_coin: float | None = Field(default=None)
    max_order_notional_usd: float | None = Field(default=None)

    metrics_port: int | None = Field(default=None, description="If set, expose Prometheus metrics on this port")

    postgres_dsn: str | None = Field(default=None, description="postgresql://… for orders / reconcile")

    ingest_fills_ws: bool = Field(default=True, description="Subscribe to userFills and store rows when Postgres enabled")
    ingest_fill_snapshots: bool = Field(
        default=False,
        description="If false, ignore userFills messages with isSnapshot=true (avoid duplicate history on restart)",
    )
    ingest_fills_from_user_events: bool = Field(
        default=True,
        description="Also persist fills embedded in `user` channel (userEvents); fills dedupe by hash",
    )
    track_order_updates: bool = Field(
        default=True,
        description="Subscribe to orderUpdates and update orders.status / hl_order_status in Postgres",
    )

    clickhouse_host: str | None = Field(default=None)
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: SecretStr | None = None
    clickhouse_database: str = "hl"

    redis_url: str | None = Field(default=None)
    redis_publish_l2: bool = Field(default=False, description="Mirror L2 JSON snapshot to Redis keys hl:l2book:{COIN}")

    l2_local_ndjson_path: str | None = Field(
        default=None,
        description="If set, append one JSON line per l2 message for offline replay (hot path: buffered write)",
    )

    strategy_entrypoint: str | None = Field(
        default=None,
        validation_alias=AliasChoices("HL_STRATEGY", "strategy_entrypoint"),
        description="`module.path:ClassName` for Strategy (no-arg ctor); unset = NullStrategy",
    )

    initial_perp_leverage: int | None = Field(
        default=None,
        validation_alias=AliasChoices("HL_INITIAL_PERP_LEVERAGE", "initial_perp_leverage"),
        description="If set, Exchange.update_leverage(leverage, coin, cross=True) once per watch coin at engine start (skip when dry_run)",
    )

    perp_leverage_map: str | None = Field(
        default=None,
        validation_alias=AliasChoices("HL_PERP_LEVERAGE_MAP", "perp_leverage_map"),
        description=(
            "Optional per-coin leverage: comma-separated COIN=int, e.g. LIT=5,HYPE=10. "
            "Takes precedence over initial_perp_leverage for listed symbols; others still use initial_perp_leverage."
        ),
    )

    portfolio_refresh_interval_sec: float = Field(
        default=3.0,
        ge=0.0,
        validation_alias=AliasChoices("HL_PORTFOLIO_REFRESH_SEC", "portfolio_refresh_interval_sec"),
        description="Background REST refresh of user_state + open orders (0 = disable). Keeps risk/dedup aligned with exchange.",
    )

    cancel_on_mid_drift_usd: float = Field(
        default=0.0,
        ge=0.0,
        validation_alias=AliasChoices("HL_CANCEL_ON_MID_DRIFT_USD", "cancel_on_mid_drift_usd"),
        description=(
            "Cancel resting limits when |L2 mid - limitPx| > this (USD). Default 0 (off): wall quotes "
            "often sit >$0.05 from mid and would be canceled immediately. Set e.g. 0.15 if you want drift hygiene."
        ),
    )

    def watch_coin_list(self) -> list[str]:
        parts = [x.strip() for x in self.watch_coins.split(",") if x.strip()]
        return parts or ["BTC"]

    def leverage_for_coin(self, coin: str) -> int | None:
        """Resolve cross leverage for a perp symbol; None means do not call update_leverage."""
        c = str(coin).strip().upper()
        raw = self.perp_leverage_map
        if raw:
            for part in raw.split(","):
                chunk = part.strip()
                if not chunk or "=" not in chunk:
                    continue
                sym, lev_s = chunk.split("=", 1)
                if sym.strip().upper() != c:
                    continue
                try:
                    return int(float(lev_s.strip()))
                except ValueError:
                    return None
        return self.initial_perp_leverage


@lru_cache
def get_settings() -> Settings:
    return Settings()


def clear_settings_cache() -> None:
    get_settings.cache_clear()
