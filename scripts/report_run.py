"""Read-only durable attribution and observation windows for a benchmark report."""
from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import stat

from dotenv import dotenv_values
import psycopg
from psycopg.rows import dict_row

from scripts.benchmark import distribution
from scripts.observe_runtime import summarize


def collect(dsn, benchmark):
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        conn.execute("SET TRANSACTION READ ONLY")
        batches = conn.execute("SELECT id,total_count,completed_count,failed_count,cancelled_count,prompt_tokens,completion_tokens FROM batches WHERE id=ANY(%s)", (benchmark["batch_ids"],)).fetchall()
        tasks = conn.execute("SELECT id,worker_id,status,attempts,prompt_tokens,completion_tokens,extract(epoch FROM finished_at-created_at)::float8 AS turnaround_seconds FROM tasks WHERE batch_id=ANY(%s)", (benchmark["batch_ids"],)).fetchall()
        attempts = conn.execute("SELECT task_id,status,extract(epoch FROM finished_at-started_at)::float8 AS duration_seconds FROM attempts WHERE batch_id=ANY(%s)", (benchmark["batch_ids"],)).fetchall()
    workers = {}
    for task in tasks:
        worker = workers.setdefault(task["worker_id"] or "unassigned", {"completed": 0, "other_statuses": {}, "prompt_tokens": 0, "completion_tokens": 0})
        if task["status"] == "completed":
            worker["completed"] += 1
            worker["prompt_tokens"] += task["prompt_tokens"]
            worker["completion_tokens"] += task["completion_tokens"]
        else:
            state = task["status"]
            worker["other_statuses"][state] = worker["other_statuses"].get(state, 0) + 1
    completed = [task for task in tasks if task["status"] == "completed"]
    finished_attempts = [a for a in attempts if a["status"] == "completed"]
    task_input = sum(t["prompt_tokens"] for t in tasks)
    task_output = sum(t["completion_tokens"] for t in tasks)
    consistent = (len(tasks) == len({t["id"] for t in tasks}) == sum(b["total_count"] for b in batches) == benchmark["requested_tasks"]
        and len(completed) == len(finished_attempts) == sum(b["completed_count"] for b in batches) == benchmark["completed"]
        and task_input == sum(b["prompt_tokens"] for b in batches) == benchmark["observed_prompt_tokens"]
        and task_output == sum(b["completion_tokens"] for b in batches) == benchmark["observed_completion_tokens"])
    return {"run_id": benchmark["run_id"], "durable_results_and_usage_match_downloaded_outputs": consistent,
        "durable_task_count": len(tasks), "durable_attempt_count": len(attempts),
        "tasks_with_multiple_attempts": sum(t["attempts"] > 1 for t in tasks),
        "accepted_completions_by_worker": dict(sorted(workers.items())),
        "task_creation_to_durable_finish_seconds": distribution([t["turnaround_seconds"] for t in completed]),
        "accepted_attempt_claim_to_durable_finish_seconds": distribution([a["duration_seconds"] for a in finished_attempts]),
        "limitations": ["Attempt duration includes worker scheduling, GPU execution, network and durable completion; not isolated GPU latency", "Task turnaround includes queue wait", "Only accepted result usage is counted; unsuccessful physical inference may consume additional compute"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--benchmark", type=Path, required=True)
    parser.add_argument("--samples", type=Path, action="append", default=[])
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if stat.S_IMODE(args.env_file.stat().st_mode) & 0o077:
        parser.error("Use an owner-only environment file")
    benchmark = json.loads(args.benchmark.read_text())
    result = collect(dotenv_values(args.env_file, interpolate=False)["DATABASE_URL"], benchmark)
    windows = {}
    start = datetime.fromisoformat(benchmark["measurement_started_at"])
    end = datetime.fromisoformat(benchmark["terminal_observation_ended_at"])
    for path in args.samples:
        # Keep only fully contained samples. Boundary requests and activity
        # before the first successful scrape are deliberately not reconstructed.
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        selected = [row for row in rows if start <= datetime.fromisoformat(row["started_at"]) and datetime.fromisoformat(row["finished_at"]) <= end]
        windows[path.name] = summarize(selected)
    result["observation_windows"] = windows
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({key: value for key, value in result.items() if key != "observation_windows"}, indent=2))
    if not result["durable_results_and_usage_match_downloaded_outputs"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
