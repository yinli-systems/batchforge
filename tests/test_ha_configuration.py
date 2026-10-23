"""Safety properties of operator-rendered HA and rolling credentials."""
import importlib.util
import json
from pathlib import Path
import stat

import pytest

from backend.config import Settings


def test_rotation_configuration_rejects_tenant_credentials_and_malformed_sets():
    for value in ('"not-an-array"', '[1]', '["short"]', 'not json'):
        with pytest.raises(ValueError):
            Settings(batchforge_worker_tokens_json=value).worker_tokens()
    tenant = "tenant-secret-must-not-work-as-worker"
    with pytest.raises(ValueError, match="must be different"):
        Settings(batchforge_api_key=tenant, batchforge_worker_tokens_json=json.dumps([tenant])).worker_tokens()


def test_ha_render_requires_distinct_domains_and_preserves_existing_credentials(tmp_path):
    root = Path(__file__).resolve().parents[1]
    module_path = root / "ops/ha/render.py"
    spec = importlib.util.spec_from_file_location("ha_render", module_path)
    renderer = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(renderer)
    inventory = json.loads((root / "ops/ha/inventory.example.json").read_text())
    destination = tmp_path / "operator-private"
    renderer.render(inventory, destination)
    for node in inventory["nodes"]:
        path = destination / node["name"] / "patroni.json"
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        config = json.loads(path.read_text())
        assert config["watchdog"]["mode"] == "required"
        assert config["bootstrap"]["dcs"]["synchronous_mode_strict"] is True
        assert len(config["etcd3"]["hosts"].split(",")) == 3
        assert "https://" not in config["etcd3"]["hosts"]
    existing = (destination / inventory["nodes"][0]["name"] / "patroni.json").read_bytes()
    with pytest.raises(FileExistsError):
        renderer.render(inventory, destination)
    assert existing == (destination / inventory["nodes"][0]["name"] / "patroni.json").read_bytes()
    inventory["nodes"][2]["failure_domain"] = inventory["nodes"][0]["failure_domain"]
    with pytest.raises(ValueError, match="failure domains"):
        renderer.render(inventory, tmp_path / "unsafe-cluster")
