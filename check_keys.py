#!/usr/bin/env python3
"""Quick parallel test of all Polygon API keys."""
import os, re, time, requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Load .env manually
env_file = Path(__file__).resolve().parent / ".env"
if env_file.exists():
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip("'\""))

raw = os.getenv("POLYGON_API_KEYS", "") or os.getenv("POLYGON_API_KEY", "")
keys = [k.strip() for k in re.split(r"[,;\s]+", raw) if k.strip()]
numbered = []
for name, value in os.environ.items():
    match = re.fullmatch(r"POLYGON_API_KEYS?_(\d+)", name)
    if match and value.strip():
        numbered.append((int(match.group(1)), value.strip()))
keys.extend(value for _, value in sorted(numbered))
seen, unique = set(), []
for k in keys:
    if k not in seen:
        seen.add(k)
        unique.append(k)

print(f"\n{'='*60}")
print(f"  Testing {len(unique)} Polygon API keys in parallel...")
print(f"{'='*60}\n")

TEST_URL = "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/minute/2026-11-04/2026-11-04"
results = {}

def test_key(key):
    start = time.time()
    try:
        r = requests.get(TEST_URL, params={"apiKey": key, "adjusted": "false", "limit": 1}, timeout=10)
        elapsed = time.time() - start
        if r.status_code == 200:
            count = len(r.json().get("results", []))
            return key, "[OK]", elapsed, count
        elif r.status_code == 403:
            return key, "[403 INVALID]", elapsed, 0
        elif r.status_code == 429:
            return key, "[429 RATE-LIMITED]", elapsed, 0
        else:
            return key, f"[{r.status_code}]", elapsed, 0
    except Exception as e:
        return key, f"[NETWORK ERROR: {type(e).__name__}]", time.time() - start, 0

start_all = time.time()
ok, bad, rate = 0, 0, 0

with ThreadPoolExecutor(max_workers=len(unique)) as pool:
    futs = {pool.submit(test_key, k): k for k in unique}
    for index, fut in enumerate(as_completed(futs), 1):
        key, status, elapsed, count = fut.result()
        print(f"  Key {index:02d}  {status:25s}  {elapsed:.2f}s")
        if "OK" in status:
            ok += 1
        elif "429" in status:
            rate += 1
        else:
            bad += 1

total = time.time() - start_all
print(f"\n{'='*60}")
print(f"  Working     : {ok}")
print(f"  Rate-ltd    : {rate}")
print(f"  Invalid/Err : {bad}")
print(f"  Total time  : {total:.2f}s  (all {len(unique)} tested in parallel)")
print(f"{'='*60}\n")
