"""Live Odoo XML-RPC connection test.

Run manually with:
python -m scripts.test_odoo_connection
"""

from __future__ import annotations

import sys

from app.config import load_config
from app.odoo_client import OdooXmlRpcClient


TEST_PAYLOAD = {
    "MN": "A200-035C016P001 4208T01",
}


def main() -> int:
    config = load_config()
    print(f"Odoo URL : {config.odoo_url}")
    print(f"Database : {config.odoo_database}")
    print(f"Username : {config.odoo_username}")
    print(f"Model    : {config.odoo_model}")
    print(f"Method   : {config.odoo_submit_method}")

    client = OdooXmlRpcClient(
        url=config.odoo_url,
        database=config.odoo_database,
        username=config.odoo_username,
        password=config.odoo_password,
        model=config.odoo_model,
        submit_method=config.odoo_submit_method,
        timeout=config.odoo_timeout,
    )
    try:
        client.authenticate()
        response = client.submit_print_data(TEST_PAYLOAD)
        print(f"Response : {response!r}")
        return 0
    except Exception as exc:
        print(f"ERROR    : {exc}", file=sys.stderr)
        return 1
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
