import os

import uvicorn

from .server import create_app


def main() -> None:
    if not os.environ.get("BATCHFORGE_API_KEY"):
        raise SystemExit("Set BATCHFORGE_API_KEY to a tenant API key; it stays on this server.")
    uvicorn.run(create_app(), host=os.environ.get("PRODUCT_HOST", "127.0.0.1"),
                port=int(os.environ.get("PRODUCT_PORT", "8090")), log_level="info")


if __name__ == "__main__":
    main()
