# Batch text jobs (product layer)

A small user-facing app on top of the BatchForge public API. Users pick a task
(sentiment, summary, field extraction), paste one item per line, and get a
results table and CSV back.

The tenant API key stays on this server. The browser only talks to `/api/*`.

## Run

```bash
BATCHFORGE_API_KEY=<tenant key> \
BATCHFORGE_URL=http://127.0.0.1:8000 \
PRODUCT_MODEL=Qwen/Qwen3-235B-A22B-Instruct-2507-FP8 \
python -m product
```

Open http://127.0.0.1:8090.

## API

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/api/templates` | Available tasks |
| POST | `/api/jobs` | `{template, text, idempotency_key?}` uploads items and creates a batch |
| GET | `/api/jobs/{id}` | Progress; `rows` appear once the batch is terminal |
| GET | `/api/jobs/{id}/export.csv` | CSV of finished results (409 while running) |

Jobs hold no state on this server. The template id is stored in batch metadata,
and inputs are read back from the stored input file.

## Limits

- 500 items per job, 8,000 characters per item (`product/jobs.py`).
- Extraction requests use `response_format: json_object`. Outputs that do not
  parse are marked `invalid_json` rather than shown as results.

## Tests

```bash
python -m pytest product/tests
```
