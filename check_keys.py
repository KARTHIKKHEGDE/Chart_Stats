#!/usr/bin/env python3
"""Check every configured Polygon key against the recent 1-minute bars API.

The script never prints key values. It sends at most one request per unique key,
in parallel, so checking a 50-key pool stays within the per-key request limit.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from zoneinfo import ZoneInfo

import requests


ROOT = Path(__file__).resolve().parent
MAX_WORKERS = 50


def load_env_file(path: Path) -> None:
    """Load simple KEY=value lines without requiring python-dotenv."""
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.lower().startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            continue
        name, value = line.split("=", 1)
        os.environ.setdefault(name.strip(), value.strip().strip("'\""))


def configured_keys() -> list[str]:
    raw = os.getenv("POLYGON_API_KEYS", "") or os.getenv("POLYGON_API_KEY", "")
    keys = [value.strip() for value in re.split(r"[,;\s]+", raw) if value.strip()]
    numbered = []
    for name, value in os.environ.items():
        match = re.fullmatch(r"POLYGON_API_KEYS?_(\d+)", name)
        if match and value.strip():
            numbered.append((int(match.group(1)), value.strip()))
    return keys + [value for _, value in sorted(numbered)]


def default_test_date() -> str:
    """Use yesterday in New York; empty holiday data still verifies API access."""
    return (dt.datetime.now(ZoneInfo("America/New_York")).date() - dt.timedelta(days=1)).isoformat()


def check_key(key: str, day: str) -> tuple[str, int | None, str, int | None]:
    """Return a non-secret status label, HTTP status and result count."""
    url = f"https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/minute/{day}/{day}"
    try:
        response = requests.get(
            url,
            params={"apiKey": key, "adjusted": "false", "sort": "asc", "limit": 1},
            timeout=(5, 15),
        )
    except requests.RequestException as exc:
        # Only the exception type is safe to print; request errors can contain URLs.
        return "NETWORK ERROR", None, type(exc).__name__, None

    if response.status_code == 200:
        try:
            payload = response.json()
        except ValueError:
            return "INVALID RESPONSE", 200, "response was not JSON", None
        if str(payload.get("status", "")).upper() == "ERROR":
            return "API ERROR", 200, "provider returned ERROR", None
        count = len(payload.get("results") or [])
        return "WORKING", 200, "minute-bars endpoint accepted the key", count
    if response.status_code == 401:
        return "INVALID KEY", 401, "unauthorized", None
    if response.status_code == 403:
        return "NO ACCESS", 403, "key or plan cannot access minute bars", None
    if response.status_code == 429:
        retry_after = response.headers.get("Retry-After", "unknown")
        return "RATE LIMITED", 429, f"retry-after={retry_after}", None
    return "HTTP ERROR", response.status_code, "provider request failed", None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", type=Path, default=ROOT / ".env", help="env file to read (default: project .env)")
    parser.add_argument("--date", default=None, help="test date YYYY-MM-DD (default: yesterday in New York)")
    args = parser.parse_args()

    if args.date:
        try:
            dt.date.fromisoformat(args.date)
        except ValueError:
            parser.error("--date must be YYYY-MM-DD")

    load_env_file(args.env)
    keys = configured_keys()
    if not keys:
        print(f"No Polygon keys found in {args.env} or environment variables.")
        print("Set POLYGON_API_KEYS or POLYGON_API_KEY, then run this script again.")
        return 2

    # Keep the original positions so duplicates are reported without sending
    # unnecessary extra requests using the same credential.
    first_position: dict[str, int] = {}
    unique: list[tuple[int, str]] = []
    duplicates: dict[int, int] = {}
    for position, key in enumerate(keys, 1):
        if key in first_position:
            duplicates[position] = first_position[key]
        else:
            first_position[key] = position
            unique.append((position, key))

    day = args.date or default_test_date()
    print(f"Checking {len(keys)} configured key(s) ({len(unique)} unique) against Polygon minute bars on {day}.")
    print("Keys are not displayed. Each unique key receives one request.")

    results: dict[int, tuple[str, int | None, str, int | None]] = {}
    workers = min(MAX_WORKERS, len(unique))
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="polygon-key-check") as pool:
        futures = {pool.submit(check_key, key, day): position for position, key in unique}
        for future in as_completed(futures):
            results[futures[future]] = future.result()

    counts = {"WORKING": 0, "RATE LIMITED": 0, "INVALID KEY": 0, "NO ACCESS": 0, "OTHER": 0}
    for position in range(1, len(keys) + 1):
        if position in duplicates:
            original = duplicates[position]
            print(f"Key {position:02d}: DUPLICATE of key {original:02d} (not requested again)")
            continue
        status, http_status, detail, result_count = results[position]
        if status in counts:
            counts[status] += 1
        else:
            counts["OTHER"] += 1
        suffix = f"HTTP {http_status}" if http_status is not None else detail
        if status == "RATE LIMITED":
            suffix = f"HTTP {http_status}, {detail}"
        elif status == "WORKING":
            suffix += f", minute rows returned: {result_count}"
        print(f"Key {position:02d}: {status} ({suffix})")

    print("\nSummary of unique keys:")
    print(f"  Working:       {counts['WORKING']}")
    print(f"  Rate limited:  {counts['RATE LIMITED']}")
    print(f"  Invalid:       {counts['INVALID KEY']}")
    print(f"  No access:     {counts['NO ACCESS']}")
    print(f"  Other errors:  {counts['OTHER']}")
    print("A working key may return zero rows on a market holiday or when data is unavailable for that date.")
    return 0 if counts["WORKING"] == len(unique) else 1


if __name__ == "__main__":
    raise SystemExit(main())
