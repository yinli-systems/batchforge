from __future__ import annotations

import time

from fastapi import FastAPI, Response
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST


class Metrics:
    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self.active = Gauge("batchforge_worker_inflight", "Currently executing or reporting tasks", registry=self.registry)
        self.tokens = Gauge("batchforge_worker_reserved_context_tokens", "Conservative total context reservations", registry=self.registry)
        self.slots = Gauge("batchforge_worker_capacity", "Effective task capacity after token bound", registry=self.registry)
        self.engines = Gauge("batchforge_worker_ready_engines", "Configured engines advertising the registered model", registry=self.registry)
        self.results = Counter("batchforge_worker_tasks", "Observed task outcomes", ["outcome"], registry=self.registry)
        self.requests = Counter("batchforge_worker_control_requests", "Control plane requests", ["operation", "outcome"], registry=self.registry)
        self.control_failovers = Counter("batchforge_worker_control_failovers", "Control origin changes for subsequent requests", registry=self.registry)
        self.control_origin = Gauge("batchforge_worker_control_origin_index", "Index of the selected configured control origin", registry=self.registry)
        self.latency = Histogram("batchforge_worker_inference_seconds", "Complete model request duration", buckets=(.1,.25,.5,1,2,5,10,20,40,80,160,320), registry=self.registry)
        self.prompt = Counter("batchforge_worker_prompt_tokens", "Reported model prompt tokens", registry=self.registry)
        self.completion = Counter("batchforge_worker_completion_tokens", "Reported model completion tokens", registry=self.registry)
        self.invalid_outputs = Counter("batchforge_worker_invalid_outputs", "Observed engine responses rejected for invalid JSON-object output", registry=self.registry)
        self.rejected_prompt = Counter("batchforge_worker_rejected_prompt_tokens", "Observed engine prompt tokens for rejected outputs; excluded from accepted usage", registry=self.registry)
        self.rejected_generated = Counter("batchforge_worker_rejected_generated_tokens", "Observed engine completion tokens for rejected outputs; excluded from accepted usage", registry=self.registry)
        self.last_control_success = 0.0
        self.registered = False
        self.stopping = False
        self.engine_ready = False

    def app(self) -> FastAPI:
        app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

        @app.get("/healthz")
        def health() -> dict:
            return {"status": "draining" if self.stopping else "alive"}

        @app.get("/readyz")
        def ready() -> Response:
            ready = self.registered and self.engine_ready and not self.stopping and time.monotonic() - self.last_control_success < 60
            return Response('ready\n' if ready else 'unavailable\n', status_code=200 if ready else 503)

        @app.get("/metrics")
        def metrics() -> Response:
            return Response(generate_latest(self.registry), media_type=CONTENT_TYPE_LATEST)

        return app
