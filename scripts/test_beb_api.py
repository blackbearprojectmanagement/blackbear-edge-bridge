"""Manual smoke test for the BEB Odoo-to-PLC command API."""

from __future__ import annotations

import os
import sys

import requests


def main() -> int:
    base_url = os.getenv("BEB_API_URL", "http://127.0.0.1:8000").rstrip("/")
    username = os.getenv("BEB_API_USERNAME", "odoo")
    password = os.getenv("BEB_API_PASSWORD", "")
    url = f"{base_url}/api/v1/plc/command"
    payload = {"messt01": "Z106-020C012P001"}

    print(f"POST {url}")
    print(f"Username: {username}")
    print("Password: [hidden]")

    try:
        response = requests.post(
            url,
            json=payload,
            headers={"Idempotency-Key": "BEB-TEST-T01-001"},
            auth=(username, password),
            timeout=int(os.getenv("BEB_API_REQUEST_TIMEOUT", "10")),
        )
    except requests.RequestException as exc:
        print(f"Request failed: {exc}")
        return 1

    print(f"Status code: {response.status_code}")
    print(f"Response body: {response.text}")
    return 0 if response.status_code == 200 else 1


if __name__ == "__main__":
    sys.exit(main())
