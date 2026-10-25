# Evidence packs

An evidence pack is the minimum record needed to reproduce and check an
experiment or a hardware observation. Each pack contains:

- `collected_at` and `host` for every observation;
- the exact command or script that produced it (committed in this repository);
- the raw structured output, unmodified, as JSON;
- a short statement of what the observation does and does not show.

## Current packs

- `gpu-snapshot-2026-10-25.json`: a read-only `nvidia-smi` inventory of the
  four-H100 host, taken during the eager engine run. Produced by
  `ops/evidence/gpu_inventory.py`. Shows 4 H100 80GB devices, about 297 GB of
  GPU memory in use across the four cards, and four processes holding memory.
  It does **not** establish interconnect topology, NVLink state or P2P
  capability; the collector does not measure those.

## Reproducing the snapshot

```bash
python3 ops/evidence/gpu_inventory.py > snapshot.json
```

Requires `nvidia-smi` on the path. The script issues only read queries.
