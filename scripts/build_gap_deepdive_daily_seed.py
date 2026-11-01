"""Download two years of daily candles for every configured Gap Deep Dive ticker."""
from __future__ import annotations

import concurrent.futures
import datetime as dt
import json
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from chart_app.charts import GAP_DEEPDIVE_TICKERS  # noqa: E402
from chart_app.market_data import HISTORY_DAYS, load_daily  # noqa: E402


def fetch(ticker: str):
    rows = load_daily(ticker, force=True)
    if not rows:
        raise RuntimeError("Polygon returned no daily candles")
    if any(not isinstance(row, dict) or "vw" not in row for row in rows):
        raise RuntimeError("Polygon returned an incomplete daily candle")
    return ticker, rows


def main() -> int:
    tickers = list(dict.fromkeys(str(t).upper() for t in GAP_DEEPDIVE_TICKERS))
    workers = min(50, len(tickers))
    gathered: dict[str, list[dict]] = {}
    failures: dict[str, str] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers,
                                               thread_name_prefix="daily-seed") as pool:
        futures = {pool.submit(fetch, ticker): ticker for ticker in tickers}
        for index, future in enumerate(concurrent.futures.as_completed(futures), 1):
            ticker = futures[future]
            try:
                name, rows = future.result()
                gathered[name] = rows
                print(f"[{index}/{len(tickers)}] {ticker}: {len(rows)} candles", flush=True)
            except Exception as exc:
                failures[ticker] = f"{type(exc).__name__}: {exc}"
                print(f"[{index}/{len(tickers)}] {ticker}: FAILED ({type(exc).__name__})", flush=True)

    if failures:
        print(f"Incomplete download: {len(failures)} of {len(tickers)} tickers failed.", file=sys.stderr)
        print("Fix the failed ticker/API issue and rerun; the seed file was not replaced.", file=sys.stderr)
        for ticker, message in sorted(failures.items()):
            print(f"  {ticker}: {message}", file=sys.stderr)
        return 1

    all_dates = [row["date"] for rows in gathered.values() for row in rows]
    payload = {
        "version": 1,
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "range_days": HISTORY_DAYS,
        "date_from": min(all_dates),
        "date_through": max(all_dates),
        "ticker_count": len(gathered),
        "tickers": {ticker: gathered[ticker] for ticker in sorted(gathered)},
    }
    target = ROOT / "data" / "gap_deepdive_daily_2026.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=target.name, suffix=".tmp", dir=target.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as output:
            json.dump(payload, output, separators=(",", ":"), ensure_ascii=False)
        os.replace(temporary, target)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    print(f"Wrote {len(gathered)} tickers / {sum(map(len, gathered.values()))} candles to {target}")
    print(f"Date range: {payload['date_from']} through {payload['date_through']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
