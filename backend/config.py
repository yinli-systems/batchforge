from functools import lru_cache
import json

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=None, extra="ignore", case_sensitive=False)
    database_url: str = "postgresql://batchforge:batchforge@localhost:5432/batchforge"
    batchforge_api_key: str = ""
    batchforge_worker_token: str = ""
    # A bounded overlap enables rolling rotation without sharing tenant keys.
    # JSON is kept as a string to avoid BaseSettings' implicit complex decoding.
    batchforge_worker_tokens_json: str = ""
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
    db_pool_timeout_seconds: float = Field(default=3, ge=0.1, le=30)
    db_statement_timeout_ms: int = Field(default=10000, ge=100, le=120000)
    db_lock_timeout_ms: int = Field(default=2000, ge=50, le=30000)
    readiness_timeout_seconds: float = Field(default=2, ge=0.1, le=10)
    sql_completion_enabled: bool = False
    sql_claim_enabled: bool = False
    frontend_dir: str = "frontend"

    @model_validator(mode="after")
    def valid_pool(self):
        if self.db_pool_min > self.db_pool_max:
            raise ValueError("db_pool_min must not exceed db_pool_max")
        return self

    def worker_tokens(self) -> tuple[str, ...]:
        tokens = [self.batchforge_worker_token] if self.batchforge_worker_token else []
        if self.batchforge_worker_tokens_json:
            try:
                extra = json.loads(self.batchforge_worker_tokens_json)
            except (ValueError, TypeError):
                raise ValueError("BATCHFORGE_WORKER_TOKENS_JSON must be a JSON array of worker credentials") from None
            if not isinstance(extra, list):
                raise ValueError("BATCHFORGE_WORKER_TOKENS_JSON must be a JSON array of worker credentials")
            tokens.extend(extra)
        if not tokens or len(tokens) > 4 or any(not isinstance(token, str) or not 16 <= len(token) <= 512 for token in tokens):
            raise ValueError("Configure one to four worker credentials, each 16 to 512 characters")
        tokens = tuple(dict.fromkeys(tokens))
        if self.batchforge_api_key and self.batchforge_api_key in tokens:
            raise ValueError("Tenant API key and worker credentials must be different")
        return tokens


@lru_cache
def get_settings() -> Settings:
    return Settings()
