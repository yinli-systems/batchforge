#!/usr/bin/env python3
"""Copy only allowlisted, credential-free model reports through the SSH gateway."""
from __future__ import annotations

import argparse
import datetime
import json
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
FILES = {
    "model": ("pod2-qwen7b-model.json", "pod2-7b-model-assets"),
    "gpu0": ("pod2-7b-gpu0-probe.json", "pod2-7b-gpu0-probe"),
    "all": ("pod2-7b-all-engines-probe.json", "pod2-7b-engine-load"),
    "hardware": ("pod2-7b-hardware.json", "pod2-7b-hardware"),
    "workers": ("pod2-7b-workers-ready.json", "pod2-7b-workers-ready"),
    "managed_api": ("pod2-managed-api-cutover.json", "pod2-managed-api-cutover"),
    "supervision": ("supervision-pod2-services.json", "pod2-service-supervision"),
    "visibility": ("pod2-device-visibility.json", "pod2-device-visibility"),
    "expansion": ("pod2-expansion-plan.json", "pod2-expansion-plan"),
    "allocation": ("pod2-extra-mig-allocation.json", "pod2-extra-mig-allocation"),
    "controlplane": ("observability/controlplane-secondary-20261024.summary.json", "pod2-controlplane-observation"),
    "api_phase1": ("pod2-api-phase1.json", "pod2-api-phase1"),
    "api_phase2": ("pod2-api-phase2.json", "pod2-api-phase2"),
    "baseline_observation": ("pod2-baseline-secondary-observation.json", "squad7b-analysis-2048-baseline-secondary-observation"),
    "sampler_continuity": ("pod2-sampler-continuity.json", "pod2-sampler-continuity"),
    "sql_before_workers": ("analysis-sql10-before-workers.json", "pod2-analysis-sql10-before-workers"),
    "sql_after_workers": ("analysis-sql10-after-workers.json", "pod2-analysis-sql10-after-workers"),
    "sql_observation": ("pod2-sql10-secondary-observation.json", "squad7b-analysis-2048-sql10-secondary-observation"),
    "model_expansion": ("pod2-7b-expansion.json", "pod2-7b-expansion"),
    "fleet_ready": ("pod2-16mig-ready.json", "pod2-16mig-ready"),
    "mig_plugin_probe": ("pod2-7b-plugin-gpu6-probe.json", "pod2-7b-mig-plugin-probe"),
    "controlplane_ab": ("observability/controlplane-secondary-ab-20261024.summary.json", "pod2-controlplane-ab-observation"),
    "sql20_before_workers": ("analysis-sql20-before-workers.json", "pod2-analysis-sql20-before-workers"),
    "sql20_after_workers": ("analysis-sql20-after-workers.json", "pod2-analysis-sql20-after-workers"),
    "sql20_observation": ("pod2-sql20-secondary-observation.json", "squad7b-analysis-2048-sql20v2-secondary-observation"),
    "claim_phase1": ("pod2-api-claim-phase1.json", "pod2-api-claim-phase1"),
    "claim_phase2": ("pod2-api-claim-phase2.json", "pod2-api-claim-phase2"),
    "claim20_before_workers": ("analysis-sqlclaim20-before-workers.json", "pod2-analysis-sqlclaim20-before-workers"),
    "claim20_after_workers": ("analysis-sqlclaim20-after-workers.json", "pod2-analysis-sqlclaim20-after-workers"),
    "claim20_observation": ("pod2-sqlclaim20-secondary-observation.json", "squad7b-analysis-2048-sql20v3-secondary-observation"),
    "seq16_tuning": ("pod2-16seq-tuning.json", "pod2-16seq-tuning"),
    "seq16_ready": ("pod2-16seq-ready.json", "pod2-16seq-ready"),
    "seq16_before_workers": ("analysis-seq16-before-workers.json", "pod2-analysis-seq16-before-workers"),
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="Client-local ISO date for report filenames")
    parser.add_argument("--include", nargs="+", choices=FILES, default=list(FILES))
    args = parser.parse_args()
    if datetime.date.fromisoformat(args.date).isoformat() != args.date:
        parser.error("date must use YYYY-MM-DD")
    names = {key: FILES[key][0] for key in args.include}
    script = ("set -eu\npython3 - <<'BF_REPORTS'\nimport json\nfrom pathlib import Path\n"
              f"names={names!r}\n"
              "bundle={key:json.loads((Path('.runtime')/name).read_text()) for key,name in names.items()}\n"
              "print('BATCHFORGE_REPORT_JSON '+json.dumps(bundle,separators=(',',':')))\nBF_REPORTS\n")
    with tempfile.NamedTemporaryFile(mode="w", prefix="batchforge-model-reports-", suffix=".sh", delete=False) as temporary:
        temporary.write(script)
        path = Path(temporary.name)
    try:
        completed = subprocess.run([sys.executable, str(ROOT / "ops/pod2/proxy_exec.py"), str(path), "--timeout", "120"],
                                   cwd=ROOT, capture_output=True, text=True, timeout=150)
        if completed.returncode:
            raise RuntimeError(f"Report transfer failed (exit {completed.returncode}); no credentials were requested")
        marker = "BATCHFORGE_REPORT_JSON "
        line = next(line for line in completed.stdout.splitlines() if line.startswith(marker))
        bundle = json.loads(line[len(marker):])
        for key in args.include:
            target = ROOT / "ops/reports" / f"{FILES[key][1]}-{args.date}.json"
            target.write_text(json.dumps(bundle[key], indent=2) + "\n")
            print(json.dumps({"event": "report_copied", "file": str(target), "source": FILES[key][0]}))
    finally:
        path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
