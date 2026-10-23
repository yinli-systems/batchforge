"""Render per-node Patroni and etcd configuration, without starting services.

Three independent failure domains are required. Resulting files contain generated
credentials and belong in an operator-only deployment directory, never Git. TLS
certificates and watchdog devices must be provisioned before starting services.
"""
from __future__ import annotations

import argparse
import ipaddress
import json
import os
from pathlib import Path
import re
import secrets


def render(inventory: dict, output: Path):
    nodes = inventory["nodes"]
    if len(nodes) != 3 or len({node["failure_domain"] for node in nodes}) != 3:
        raise ValueError("Exactly three distinct failure domains are required")
    network = ipaddress.ip_network(inventory["private_cidr"])
    if len({node["private_ip"] for node in nodes}) != 3 or len({node["name"] for node in nodes}) != 3:
        raise ValueError("Names and private IP addresses must be unique")
    for node in nodes:
        if not re.fullmatch(r"[a-z][a-z0-9-]{0,62}", node["name"]):
            raise ValueError("Node name must be a DNS-safe lowercase identifier")
        if ipaddress.ip_address(node["private_ip"]) not in network:
            raise ValueError("All node IPs must belong to the private CIDR")
    cluster = inventory["cluster_name"]
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,62}", cluster):
        raise ValueError("Cluster name must be a DNS-safe lowercase identifier")
    if output.exists():
        raise FileExistsError("Use a new operator-only output directory; existing cluster secrets must not be overwritten")
    output.mkdir(parents=True, mode=0o700)
    os.chmod(output, 0o700)
    passwords = {name: secrets.token_urlsafe(36) for name in ("superuser", "replication", "rewind", "restapi")}
    cluster_token = secrets.token_urlsafe(24)
    members = ",".join(f"{node['name']}=https://{node['private_ip']}:2380" for node in nodes)
    endpoints = ",".join(f"{node['private_ip']}:2379" for node in nodes)

    def write(path, value):
        with path.open("x") as handle:
            os.chmod(path, 0o600)
            handle.write(value)

    for node in nodes:
        directory = output / node["name"]
        directory.mkdir(mode=0o700)
        ip = node["private_ip"]
        tls = "/etc/batchforge-ha/tls"
        config = {
            "scope": cluster, "namespace": "/batchforge/", "name": node["name"],
            "restapi": {"listen": f"{ip}:8008", "connect_address": f"{ip}:8008", "certfile": f"{tls}/patroni.crt", "keyfile": f"{tls}/patroni.key", "cafile": f"{tls}/ca.crt", "authentication": {"username": "patroni", "password": passwords["restapi"]}},
            "ctl": {"cacert": f"{tls}/ca.crt", "authentication": {"username": "patroni", "password": passwords["restapi"]}},
            "etcd3": {"hosts": endpoints, "protocol": "https", "cacert": f"{tls}/etcd-ca.crt", "cert": f"{tls}/etcd-client.crt", "key": f"{tls}/etcd-client.key"},
            "bootstrap": {"dcs": {"ttl": 30, "loop_wait": 5, "retry_timeout": 5, "maximum_lag_on_failover": 0,
                "check_timeline": True, "synchronous_mode": True, "synchronous_mode_strict": True, "synchronous_node_count": 1,
                "postgresql": {"use_pg_rewind": True, "use_slots": True, "parameters": {"wal_level": "replica", "hot_standby": "on", "wal_log_hints": "on", "max_wal_senders": 10, "max_replication_slots": 10, "max_slot_wal_keep_size": "10GB", "synchronous_commit": "remote_apply", "password_encryption": "scram-sha-256", "ssl": "on", "ssl_cert_file": f"{tls}/postgres.crt", "ssl_key_file": f"{tls}/postgres.key", "ssl_ca_file": f"{tls}/ca.crt"}}},
                "initdb": [{"encoding": "UTF8"}, "data-checksums"],
                "pg_hba": ["local all all peer", f"hostssl replication bf_repl {network} scram-sha-256", f"hostssl all all {network} scram-sha-256"]},
            "postgresql": {"listen": f"{ip}:5432", "connect_address": f"{ip}:5432", "data_dir": "/var/lib/batchforge-ha/postgres", "bin_dir": inventory["postgres_bin_dir"],
                "authentication": {"superuser": {"username": "postgres", "password": passwords["superuser"]}, "replication": {"username": "bf_repl", "password": passwords["replication"], "sslmode": "verify-full", "sslrootcert": f"{tls}/ca.crt"}, "rewind": {"username": "bf_rewind", "password": passwords["rewind"], "sslmode": "verify-full", "sslrootcert": f"{tls}/ca.crt"}}},
            "watchdog": {"mode": "required", "device": "/dev/watchdog", "safety_margin": -1},
            "tags": {"nofailover": False, "noloadbalance": False, "nosync": False},
        }
        # JSON is a strict subset of YAML accepted by Patroni's configuration reader.
        write(directory / "patroni.json", json.dumps(config, indent=2) + "\n")
        values = {"ETCD_NAME": node["name"], "ETCD_DATA_DIR": "/var/lib/batchforge-ha/etcd", "ETCD_INITIAL_CLUSTER": members,
                  "ETCD_INITIAL_CLUSTER_TOKEN": cluster_token, "ETCD_INITIAL_CLUSTER_STATE": "new", "ETCD_LISTEN_PEER_URLS": f"https://{ip}:2380",
                  "ETCD_INITIAL_ADVERTISE_PEER_URLS": f"https://{ip}:2380", "ETCD_LISTEN_CLIENT_URLS": f"https://{ip}:2379", "ETCD_ADVERTISE_CLIENT_URLS": f"https://{ip}:2379",
                  "ETCD_CERT_FILE": f"{tls}/etcd-server.crt", "ETCD_KEY_FILE": f"{tls}/etcd-server.key", "ETCD_TRUSTED_CA_FILE": f"{tls}/etcd-ca.crt", "ETCD_CLIENT_CERT_AUTH": "true",
                  "ETCD_PEER_CERT_FILE": f"{tls}/etcd-peer.crt", "ETCD_PEER_KEY_FILE": f"{tls}/etcd-peer.key", "ETCD_PEER_TRUSTED_CA_FILE": f"{tls}/etcd-ca.crt", "ETCD_PEER_CLIENT_CERT_AUTH": "true"}
        write(directory / "etcd.env", "".join(f"{key}={value}\n" for key, value in values.items()))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inventory", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    render(json.loads(args.inventory.read_text()), args.output)
    print("Rendered three node configurations with owner-only permissions; no service was started")


if __name__ == "__main__":
    main()
