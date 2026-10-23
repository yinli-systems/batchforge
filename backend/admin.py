"""Local operator CLI. Run only on a trusted control-plane host.

Generated keys are printed once and only SHA-256 digests enter PostgreSQL.
This CLI deliberately uses DATABASE_URL; it is not a public administrative API.
"""
import argparse
import hashlib
import json
import secrets

import psycopg
from psycopg.rows import dict_row

from .config import get_settings
from .store import identifier


def main():
    parser = argparse.ArgumentParser(description="BatchForge tenant/key administration")
    commands = parser.add_subparsers(dest="command", required=True)
    tenant = commands.add_parser("create-tenant", help="Create a tenant and issue its first API key")
    tenant.add_argument("--name", required=True)
    key = commands.add_parser("add-key", help="Issue an additional tenant key for rotation")
    key.add_argument("--tenant-id", required=True)
    revoke = commands.add_parser("revoke-key", help="Revoke a key by its nonsecret SHA-256 digest")
    revoke.add_argument("--key-hash", required=True)
    args = parser.parse_args()
    settings = get_settings()
    with psycopg.connect(settings.database_url, row_factory=dict_row, options="-c client_encoding=UTF8") as conn:
        if args.command in {"create-tenant", "add-key"}:
            tenant_id = identifier("tenant") if args.command == "create-tenant" else args.tenant_id
            if args.command == "create-tenant":
                if not 1 <= len(args.name) <= 128:
                    parser.error("Tenant name must contain 1 to 128 characters")
                conn.execute("INSERT INTO tenants(id,name) VALUES(%s,%s)", (tenant_id, args.name))
            elif not conn.execute("SELECT id FROM tenants WHERE id=%s", (tenant_id,)).fetchone():
                parser.error("Tenant not found")
            api_key = "bf_" + secrets.token_urlsafe(32)
            digest = hashlib.sha256(api_key.encode()).hexdigest()
            conn.execute("INSERT INTO api_keys(key_hash,tenant_id) VALUES(%s,%s)", (digest, tenant_id))
            conn.commit()
            print(json.dumps({"tenant_id": tenant_id, "api_key": api_key, "key_hash": digest}))
        else:
            row = conn.execute("UPDATE api_keys SET revoked=true WHERE key_hash=%s RETURNING tenant_id", (args.key_hash,)).fetchone()
            if not row:
                parser.error("API key not found")
            conn.commit()
            print(json.dumps({"revoked": True, "tenant_id": row["tenant_id"], "key_hash": args.key_hash}))


if __name__ == "__main__":
    main()
