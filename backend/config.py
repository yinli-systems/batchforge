from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=None, extra="ignore", case_sensitive=False)
    database_url: str = "postgresql://batchforge:batchforge@localhost:5432/batchforge"
    batchforge_api_key: str = ""
    batchforge_worker_token: str = ""
    model_name: str = "Qwen/Qwen2.5-0.5B-Instruct"
    max_upload_bytes: int = Field(default=10 * 1024 * 1024, ge=1024, le=100 * 1024 * 1024)
    max_tasks_per_batch: int = Field(default=10000, ge=1, le=100000)
    max_request_bytes: int = Field(default=131072, ge=1024, le=1048576)
    max_output_tokens: int = Field(default=4096, ge=1, le=32768)
    max_attempts: int = Field(default=3, ge=1, le=20)
    lease_seconds: int = Field(default=90, ge=5, le=3600)
    retry_base_seconds: int = Field(default=2, ge=1, le=60)
    max_queued_tasks_per_tenant: int = Field(default=100000, ge=1)
    db_pool_min: int = Field(default=2, ge=1)
    db_pool_max: int = Field(default=20, ge=2)
    frontend_dir: str = "frontend"


@lru_cache
def get_settings() -> Settings:
    return Settings()
