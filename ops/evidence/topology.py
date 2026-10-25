"""Parse `nvidia-smi topo -m` into measured GPU-pair link classes.

Only the link class printed by the driver is recorded (for example NV18, PIX, SYS).
Anything missing or unparseable stays absent, so callers can treat it as unknown.
"""
from __future__ import annotations

import re

ANSI = re.compile(r"\x1b\[[0-9;]*m")
GPU_ROW = re.compile(r"^GPU(\d+)$")


def parse_topo_matrix(text: str) -> dict:
    """Return {"gpus": [0, 1, ...], "links": {"0-1": "NV18", ...}} from the GPU block of the matrix."""
    lines = [ANSI.sub("", line).rstrip() for line in text.splitlines()]
    header = next((line for line in lines if line.strip().startswith("GPU0") or "GPU0" in line.split("\t")), None)
    if header is None:
        raise ValueError("no GPU header in topology output")
    columns = [cell.strip() for cell in header.split("\t") if cell.strip().startswith("GPU")]
    gpu_ids = [int(GPU_ROW.match(name).group(1)) for name in columns if GPU_ROW.match(name)]
    links: dict[str, str] = {}
    for line in lines:
        cells = line.split("\t")
        label = cells[0].strip() if cells else ""
        match = GPU_ROW.match(label)
        if not match:
            continue
        row = int(match.group(1))
        values = [cell.strip() for cell in cells[1:]]
        for column, value in zip(gpu_ids, values):
            if column == row or value in {"", "X"}:
                continue
            key = "-".join(map(str, sorted((row, column))))
            if links.get(key, value) != value:
                raise ValueError(f"asymmetric topology for GPU pair {key}: {links[key]} vs {value}")
            links[key] = value
    return {"gpus": gpu_ids, "links": links}


def nvlink_pairs(topology: dict) -> set[str]:
    """Pairs whose measured class is an NVLink class (NV<n>)."""
    return {pair for pair, kind in topology["links"].items() if kind.startswith("NV")}
