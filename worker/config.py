"""Validated worker configuration. No tenant-controlled inference destinations."""
from __future__ import annotations

import json
import math
import os
import socket
import uuid
from dataclasses import dataclass
from urllib.parse import urlsplit

from backend.scheduling import validate_profile
from worker.kv_admission import MODEL as ADAPTIVE_MODEL


def _int(name: str, default: int) -> int:
    value = int(os.environ.get(name, default))
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def _bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name, str(default)).lower()
    if value not in {'true', 'false', '1', '0'}:
        raise ValueError(f'{name} must be true or false')
    return value in {'true', '1'}


def _url(value: str, name: str) -> str:
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username:
        raise ValueError(f"{name} must be an http(s) URL without credentials")
    if parsed.query or parsed.fragment:
        raise ValueError(f"{name} cannot contain a query or fragment")
    return value.rstrip("/")


@dataclass(frozen=True)
class Settings:
    control_url: str
    worker_token: str
    inference_urls: tuple[str, ...]
    model: str
    worker_id: str
    name: str
    control_urls: tuple[str, ...] = ()
    max_inflight: int = 8
    token_budget: int = 16384
    max_model_len: int = 2048
    request_timeout: float = 180
    poll_seconds: float = 0.5
    shutdown_seconds: float = 240
    report_retry_seconds: float = 60
    metrics_host: str = "127.0.0.1"
    metrics_port: int = 9101
    inference_api_key: str = ""
    scheduling_profile: dict | None = None
    adaptive_kv_enabled: bool = False

    def __post_init__(self):
        if type(self.adaptive_kv_enabled) is not bool:
            raise ValueError('Adaptive KV admission requires a boolean operator flag')
        if self.adaptive_kv_enabled:
            if self.model != ADAPTIVE_MODEL or len(self.inference_urls) != 1:
                raise ValueError('Adaptive KV admission supports one exact Qwen235B engine only')
            if type(self.max_model_len) is not int or self.max_model_len < 1:
                raise ValueError('Adaptive context must be a positive integer')
            if not math.isfinite(self.request_timeout) or not 0 < self.request_timeout <= 3600:
                raise ValueError('Adaptive wait/inference timeout must fit 1 hour')
            minimum = ((self.max_model_len + 15) // 16) * 16
            if (type(self.token_budget) is not int or self.token_budget < minimum
                    or type(self.max_inflight) is not int or not 1 <= self.max_inflight <= 4096):
                raise ValueError('Adaptive budget must fit one block-rounded full context')

    @property
    def control_origins(self) -> tuple[str, ...]:
        return self.control_urls or (self.control_url,)

    @property
    def slots(self) -> int:
        if self.adaptive_kv_enabled:
            return self.max_inflight
        # Reserve an entire engine context per sequence: a safe bound independent
        # of tokenizer, language, or user-supplied token estimates. The engine's
        # --max-model-len MUST match this configuration.
        return min(self.max_inflight, self.token_budget // self.max_model_len)

    @classmethod
    def from_env(cls) -> "Settings":
        token = os.environ.get("BATCHFORGE_WORKER_TOKEN", "")
        if len(token) < 24:
            raise ValueError("BATCHFORGE_WORKER_TOKEN must contain at least 24 characters")
        urls = tuple(_url(v.strip(), "INFERENCE_BASE_URLS") for v in os.environ.get(
            "INFERENCE_BASE_URLS", "http://127.0.0.1:8001"
        ).split(",") if v.strip())
        if not urls:
            raise ValueError("INFERENCE_BASE_URLS cannot be empty")
        raw_control_urls = os.environ.get("CONTROL_PLANE_URLS", "").strip()
        if raw_control_urls:
            control_urls = tuple(dict.fromkeys(
                _url(value.strip(), "CONTROL_PLANE_URLS")
                for value in raw_control_urls.split(",") if value.strip()
            ))
            if not control_urls:
                raise ValueError("CONTROL_PLANE_URLS cannot be empty")
        else:
            control_urls = (_url(os.environ.get("CONTROL_PLANE_URL", "http://127.0.0.1:8000"), "CONTROL_PLANE_URL"),)
        settings = cls(
            control_url=control_urls[0],
            control_urls=control_urls,
            worker_token=token,
            inference_urls=urls,
            model=os.environ.get("MODEL_NAME", "Qwen/Qwen2.5-0.5B-Instruct"),
            worker_id=os.environ.get("WORKER_ID") or str(uuid.uuid4()),
            name=os.environ.get("WORKER_NAME", socket.gethostname()),
            max_inflight=_int("WORKER_MAX_INFLIGHT", 8),
            token_budget=_int("WORKER_TOKEN_BUDGET", 16384),
            max_model_len=_int("ENGINE_MAX_MODEL_LEN", 2048),
            request_timeout=float(os.environ.get("INFERENCE_TIMEOUT_SECONDS", "180")),
            poll_seconds=float(os.environ.get("WORKER_POLL_SECONDS", "0.5")),
            shutdown_seconds=float(os.environ.get("WORKER_SHUTDOWN_SECONDS", "240")),
            report_retry_seconds=float(os.environ.get("WORKER_REPORT_RETRY_SECONDS", "60")),
            metrics_host=os.environ.get("WORKER_METRICS_HOST", "127.0.0.1"),
            metrics_port=_int("WORKER_METRICS_PORT", 9101),
            inference_api_key=os.environ.get("INFERENCE_API_KEY", ""),
            scheduling_profile=(validate_profile(json.loads(os.environ["SCHEDULER_PROFILE_JSON"]))
                                if os.environ.get("SCHEDULER_PROFILE_JSON", "").strip() else None),
            adaptive_kv_enabled=_bool('WORKER_ADAPTIVE_KV_ENABLED'),
        )
        if settings.slots < 1:
            raise ValueError("WORKER_TOKEN_BUDGET must fit at least one ENGINE_MAX_MODEL_LEN")
        if settings.max_inflight > 4096:
            raise ValueError("WORKER_MAX_INFLIGHT cannot exceed the control plane limit of 4096")
        if min(settings.request_timeout, settings.poll_seconds, settings.shutdown_seconds,
               settings.report_retry_seconds) <= 0:
            raise ValueError("Worker timeouts and poll interval must be positive")
        if not settings.model or len(settings.model) > 256:
            raise ValueError("MODEL_NAME must contain 1-256 characters")
        return settings
