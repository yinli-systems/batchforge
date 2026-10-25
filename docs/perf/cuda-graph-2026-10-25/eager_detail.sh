#!/bin/sh
set -e
E=/root/batchforge-runtime/app/.runtime/qwen235b-adaptive64-v1/engine.env
umask 077
cp /root/perf/engine.env.eager-backup $E
cd /root/batchforge-runtime/app
/root/batchforge-runtime/venv/bin/python ops/manage.py stop engine --name qwen235b --env-file $E >/dev/null 2>&1 || true
sleep 15
for i in $(seq 1 180); do curl -sf -m 3 127.0.0.1:8300/v1/models >/dev/null 2>&1 && break; sleep 5; done
PID=$(pgrep -f "vllm.entrypoints.openai.api_server" | tail -1)
ps -o args= -p $PID | grep -c enforce-eager > /root/perf/results/eager-detail.flag
for r in 1 2 3; do
/root/batchforge-runtime/engine-venv/bin/vllm bench serve --backend vllm --base-url http://127.0.0.1:8300 --endpoint /v1/completions --model Qwen/Qwen3-235B-A22B-Instruct-2507-FP8 --tokenizer /root/batchforge-runtime/models/qwen3-235b-fp8 --dataset-name random --random-input-len 4096 --random-output-len 512 --ignore-eos --num-prompts 256 --max-concurrency 64 --save-result --save-detailed --result-dir /root/perf/results --result-filename eager-detail-r$r.json
done
echo done-eager-detail
