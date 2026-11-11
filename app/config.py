"""
All settings come from env vars.

Same code runs as api, worker and beat - only the env changes between them,
so nothing (like the redis url) should be hardcoded here.
"""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # infra
    # broker and results on separate redis dbs, so we can flush one
    # without wiping the other
    redis_broker_url: str = "redis://localhost:6379/0"
    redis_backend_url: str = "redis://localhost:6379/1"
    database_url: str = "postgresql+psycopg://payflow:payflow@localhost:5432/payflow"

    # reliability
    # visibility_timeout has to be more than the slowest task, otherwise redis
    # gives the same message to another worker while the first one is still busy
    visibility_timeout: int = 3600          # seconds
    max_retries: int = 5
    retry_backoff_max: int = 600            # max 10 min between retries

    # how long we trust someone else's in-progress claim before assuming the
    # worker died. keep it above the slowest task (see services/idempotency.py)
    idempotency_lease_seconds: int = 900

    # fake gateway - lets us actually see retries and the DLQ
    gateway_failure_rate: float = 0.25      # 0 = never fails, 1 = always
    gateway_latency_ms: int = 300
    gateway_seed: int | None = None         # set this for repeatable failures

    # task timing
    payout_soft_time_limit: int = 25        # raises SoftTimeLimitExceeded, we can clean up
    payout_hard_time_limit: int = 30        # worker kills it, no cleanup
    result_expires: int = 3600              # drop results after 1h

    # payout stuck in PROCESSING longer than this = worker probably died,
    # beat will pick it up again
    stuck_payout_seconds: int = 600

    app_name: str = "payflow-jobs"
    timezone: str = "Asia/Kolkata"


@lru_cache
def get_settings() -> Settings:
    # cached so env is read only once. tests can reset with get_settings.cache_clear()
    return Settings()


settings = get_settings()
