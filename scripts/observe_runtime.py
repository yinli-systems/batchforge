#!/usr/bin/env python3
"""Read-only local control-plane/PG/GPU sampling during real inference load.

Never records SQL query text, prompt data, DSNs, tokens, or process command lines.
HTTP histograms remain cumulative in raw samples; summaries use observed counter
deltas and report percentile bucket bounds, never invented exact percentiles.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import stat
import subprocess
import threading
import time
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, Request, build_opener

import psycopg
from psycopg.rows import dict_row
from prometheus_client.parser import text_string_to_metric_families


def timestamp():
    return datetime.now(timezone.utc).isoformat()


def error_code(exc):
    # Never stringify a driver exception: it can include connection diagnostics.
    return {"type": type(exc).__name__, "sqlstate": getattr(exc, "sqlstate", None)}


def scalar(value):
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def parse_metrics(content):
    operations, requests, gauges = {}, [], {}
    for family in text_string_to_metric_families(content):
        for sample in family.samples:
            if sample.name.startswith("batchforge_http_request_duration_seconds_"):
                operation = (sample.labels.get("method"), sample.labels.get("route"))
                entry = operations.setdefault(operation, {"method": operation[0], "route": operation[1], "buckets": {}, "count": 0, "sum": 0})
                if sample.name.endswith("_bucket"):
                    entry["buckets"][sample.labels["le"]] = sample.value
                elif sample.name.endswith("_count"):
                    entry["count"] = sample.value
                elif sample.name.endswith("_sum"):
                    entry["sum"] = sample.value
            elif sample.name == "batchforge_http_requests_total":
                requests.append({**sample.labels, "count": sample.value})
            elif sample.name in {"process_start_time_seconds", "batchforge_recovered_leases_total", "batchforge_recovery_errors_total"}:
                gauges[sample.name] = sample.value
    return {"http_duration_cumulative": sorted(operations.values(), key=lambda item: (item["method"] or "", item["route"] or "")),
            "http_requests_cumulative": requests, "process_and_recovery_cumulative": gauges}


def quantile_bucket(buckets, count, quantile):
    if count <= 0:
        return None
    previous = 0.0
    for boundary, cumulative in sorted(buckets.items(), key=lambda item: float(item[0])):
        bound = float(boundary)
        if cumulative >= count * quantile:
            return {"lower_exclusive_seconds": previous, "upper_inclusive_seconds": bound if math.isfinite(bound) else None,
                    "upper_unbounded": not math.isfinite(bound)}
        previous = bound
    return None


def summarize(samples):
    operations, previous_by_api, resets, scrape_failures = {}, {}, {}, {}
    for sample in samples:
        for api in sample.get("apis", []):
            name = api["name"]
            if "error" in api:
                scrape_failures[name] = scrape_failures.get(name, 0) + 1
                continue
            previous = previous_by_api.get(name)
            previous_by_api[name] = api
            if not previous:
                continue
            if api["process_and_recovery_cumulative"].get("process_start_time_seconds") != previous["process_and_recovery_cumulative"].get("process_start_time_seconds"):
                resets[name] = resets.get(name, 0) + 1
                continue
            older = {(item["method"], item["route"]): item for item in previous["http_duration_cumulative"]}
            for current in api["http_duration_cumulative"]:
                key = (current["method"], current["route"])
                # A series first appearing during observation had an observed
                # cumulative zero before it existed in this same process.
                old = older.get(key, {"count": 0, "sum": 0, "buckets": {}})
                count, duration = current["count"] - old["count"], current["sum"] - old["sum"]
                buckets = {bound: value - old["buckets"].get(bound, 0) for bound, value in current["buckets"].items()}
                if count < 0 or duration < 0 or any(value < 0 for value in buckets.values()):
                    resets[name] = resets.get(name, 0) + 1
                    continue
                aggregate = operations.setdefault(key, {"method": key[0], "route": key[1], "count": 0, "sum_seconds": 0, "bucket_deltas": {}, "apis_observed": set()})
                aggregate["count"] += count
                aggregate["sum_seconds"] += duration
                aggregate["apis_observed"].add(name)
                for bound, value in buckets.items():
                    aggregate["bucket_deltas"][bound] = aggregate["bucket_deltas"].get(bound, 0) + value
    rows = []
    for aggregate in operations.values():
        count = aggregate["count"]
        rows.append({**aggregate, "apis_observed": sorted(aggregate["apis_observed"]),
                     "mean_seconds": aggregate["sum_seconds"] / count if count else None,
                     "p50_bucket": quantile_bucket(aggregate["bucket_deltas"], count, .50),
                     "p95_bucket": quantile_bucket(aggregate["bucket_deltas"], count, .95)})
    queue = [sample["database"]["queue"] for sample in samples if "queue" in sample.get("database", {})]
    return {"sample_count": len(samples), "first_sample_at": samples[0]["started_at"] if samples else None,
            "last_sample_at": samples[-1]["finished_at"] if samples else None,
            "operations": sorted(rows, key=lambda item: (item["method"] or "", item["route"] or "")),
            "counter_resets_excluded_intervals": resets, "api_scrape_failures": scrape_failures,
            "first_durable_queue": queue[0] if queue else None, "last_durable_queue": queue[-1] if queue else None,
            "measurement_bounds": ["HTTP statistics are deltas between successful scrapes of each observed API process; activity before its first scrape is excluded",
                "Intervals spanning a process restart/counter reset are excluded; unobserved requests during such intervals are not reconstructed",
                "p50/p95 are containing Prometheus histogram bucket bounds, not exact quantiles or interpolated point estimates",
                "HTTP durations measure API request handling, not GPU inference task latency; all traffic on observed replicas is included",
                "Database counters aggregate the database; PostgreSQL wait samples are periodic observations, not wait-time durations",
                "Runtime-role visibility of other roles' activity may be redacted; WAL statistics can include other databases on the same cluster",
                "GPU utilization/memory include all workloads visible on each GPU, including unrelated tenants; no GPU process inventory is queried",
                "Database statistics, metrics and GPU probes finish at different times; each source includes its own elapsed time",
                "Polling, metrics aggregation and sampling themselves consume some control-plane/database capacity"]}


class Sampler:
    def __init__(self, database_url, token, api_urls):
        self.database_url, self.token, self.api_urls = database_url, token, api_urls
        self.database = None
        self.http = build_opener(ProxyHandler({}))

    def api(self, item):
        name, url = item
        start = time.monotonic()
        try:
            request = Request(url + "/metrics", headers={"Authorization": "Bearer " + self.token})
            with self.http.open(request, timeout=3) as response:
                body = response.read(2 * 1024 * 1024 + 1)
                if len(body) > 2 * 1024 * 1024:
                    raise ValueError("Metrics response exceeds bound")
            result = parse_metrics(body.decode())
        except Exception as exc:
            result = {"error": error_code(exc)}
        return {"name": name, "observed_at": timestamp(), "elapsed_seconds": time.monotonic() - start, **result}

    def postgres(self):
        start, output = time.monotonic(), {}
        try:
            if self.database is None or self.database.closed:
                self.database = psycopg.connect(self.database_url, row_factory=dict_row, autocommit=True, connect_timeout=3,
                    application_name="batchforge-observer", options="-c default_transaction_read_only=on -c client_encoding=UTF8 -c statement_timeout=1500 -c lock_timeout=500")
            statements = {
                "activity_counts": "SELECT COALESCE(state,'not_visible') AS state,COALESCE(wait_event_type,'not_waiting_or_not_visible') AS wait_event_type,COALESCE(wait_event,'none_or_not_visible') AS wait_event,count(*) AS connections FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() GROUP BY 1,2,3 ORDER BY 1,2,3",
                "queue": "SELECT COALESCE(sum(pending_count),0) AS pending,COALESCE(sum(running_count),0) AS running,COALESCE(sum(completed_count),0) AS completed,COALESCE(sum(failed_count),0) AS failed,COALESCE(sum(cancelled_count),0) AS cancelled,count(*) AS batches FROM batches",
                "database_stats": "SELECT xact_commit,xact_rollback,blks_read,blks_hit,temp_files,temp_bytes,deadlocks,stats_reset FROM pg_stat_database WHERE datname=current_database()",
                "database_size": "SELECT pg_database_size(current_database()) AS bytes",
                "relation_sizes": "SELECT c.relname AS relation,pg_relation_size(c.oid) AS heap_bytes,pg_total_relation_size(c.oid) AS total_bytes FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname IN ('tasks','attempts','batches','files','idempotency','tenant_queues') ORDER BY c.relname",
                "wal_stats": "SELECT wal_records,wal_fpi,wal_bytes,wal_buffers_full,wal_write,wal_sync,wal_write_time,wal_sync_time,stats_reset FROM pg_stat_wal",
            }
            for key, statement in statements.items():
                try:
                    rows = [{name: scalar(value) for name, value in row.items()} for row in self.database.execute(statement).fetchall()]
                    output[key] = rows[0] if key in {"queue", "database_stats", "database_size", "wal_stats"} and rows else rows
                except Exception as exc:
                    output[key] = {"error": error_code(exc)}
            activity = output.get("activity_counts")
            if isinstance(activity, list):
                output["connections_total_excluding_sampler"] = sum(row["connections"] for row in activity)
                output["connections_active_visible"] = sum(row["connections"] for row in activity if row["state"] == "active")
        except Exception as exc:
            output["error"] = error_code(exc)
            if self.database:
                self.database.close()
                self.database = None
        return {"observed_at": timestamp(), "elapsed_seconds": time.monotonic() - start, **output}

    @staticmethod
    def gpu():
        start = time.monotonic()
        try:
            result = subprocess.run(["nvidia-smi", "--query-gpu=index,name,utilization.gpu,memory.used,memory.total", "--format=csv,noheader,nounits"],
                                    stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=2, check=False)
            if result.returncode or len(result.stdout) > 65536:
                raise ValueError("GPU aggregate query unavailable")
            rows = []
            for line in result.stdout.decode().splitlines():
                fields = [field.strip() for field in line.split(",")]
                if len(fields) != 5:
                    raise ValueError("Invalid GPU aggregate output")
                def numeric(value):
                    try:
                        return float(value)
                    except ValueError:
                        return None
                rows.append({"index": fields[0], "name": fields[1], "utilization_percent": numeric(fields[2]), "memory_used_mib": numeric(fields[3]), "memory_total_mib": numeric(fields[4])})
            output = {"devices": rows}
        except Exception as exc:
            output = {"error": error_code(exc)}
        return {"observed_at": timestamp(), "elapsed_seconds": time.monotonic() - start, **output}


def atomic_json(path, value):
    temporary = path.with_name(path.name + f".{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "w") as handle:
            json.dump(value, handle, indent=2, allow_nan=False)
            handle.write("\n")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--api", action="append", help="name=http://127.0.0.1:PORT; repeat for replicas")
    parser.add_argument("--interval-seconds", type=float, default=2)
    parser.add_argument("--duration-seconds", type=float, default=1800)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-output-bytes", type=int, default=64 * 1024 * 1024)
    args = parser.parse_args()
    if not .2 <= args.interval_seconds <= 60 or not 1 <= args.duration_seconds <= 14400 or not 1024 <= args.max_output_bytes <= 256 * 1024 * 1024:
        parser.error("Invalid bounded sampling configuration")
    if args.env_file.stat().st_uid != os.geteuid() or stat.S_IMODE(args.env_file.stat().st_mode) & 0o077:
        parser.error("Use an owner-only configuration file")
    from dotenv import dotenv_values
    environment = dotenv_values(args.env_file, interpolate=False)
    database_url, token = environment.get("DATABASE_URL"), environment.get("BATCHFORGE_WORKER_TOKEN")
    if not database_url or not token:
        parser.error("Configuration must define database and worker credentials")
    apis = []
    for item in args.api or ["api-a=http://127.0.0.1:8082", "api-b=http://127.0.0.1:8083"]:
        name, separator, url = item.partition("=")
        parsed = urlsplit(url)
        if not separator or not name or parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost", "::1"} or parsed.username or parsed.password or parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
            parser.error("Metrics sources must be named local HTTP replica endpoints")
        apis.append((name, url.rstrip("/")))
    if len(apis) > 8 or len({name for name, _ in apis}) != len(apis):
        parser.error("Use one to eight uniquely named replicas")
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    stopped = threading.Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: stopped.set())
    sampler = Sampler(database_url, token, apis)
    samples, first, written = [], time.monotonic(), 0
    try:
        with os.fdopen(descriptor, "w") as output, ThreadPoolExecutor(max_workers=len(apis) + 2) as executor:
            while not stopped.is_set() and time.monotonic() - first <= args.duration_seconds:
                started = time.monotonic()
                observed_at = timestamp()
                api_futures = [executor.submit(sampler.api, item) for item in apis]
                database = executor.submit(sampler.postgres)
                gpu = executor.submit(sampler.gpu)
                sample = {"started_at": observed_at, "apis": [future.result() for future in api_futures], "database": database.result(), "gpu": gpu.result()}
                sample.update(finished_at=timestamp(), elapsed_seconds=time.monotonic() - started)
                rendered = json.dumps(sample, separators=(",", ":"), allow_nan=False) + "\n"
                if written + len(rendered.encode()) > args.max_output_bytes:
                    break
                output.write(rendered)
                output.flush()
                written += len(rendered.encode())
                samples.append(sample)
                summary = summarize(samples)
                summary.update(observer_pid=os.getpid(), sample_interval_target_seconds=args.interval_seconds, raw_series_bytes=written, sampler_read_only=True)
                atomic_json(args.output.with_suffix(".summary.json"), summary)
                stopped.wait(max(0, args.interval_seconds - (time.monotonic() - started)))
    finally:
        if sampler.database:
            sampler.database.close()
    print(json.dumps({"samples": len(samples), "raw_series_bytes": written, "sampler_read_only": True}))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(json.dumps({"observer_failed": error_code(exc)}), file=__import__("sys").stderr)
        raise SystemExit(1) from None
