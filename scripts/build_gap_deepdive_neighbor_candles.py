"""Bundle 1-minute candles for each Deep Dive gap day and two sessions either side.

The chart API aggregates these minute bars into 2-minute and 5-minute candles at
runtime. Keeping a single source interval avoids duplicated data and drift.
"""
from __future__ import annotations

import concurrent.futures
import datetime as dt
import gzip
import json
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Import charts first: it initializes the app's market-data configuration.
from chart_app.charts import GAP_DEEPDIVE_TICKERS  # noqa: E402
from chart_app.market_data import _DEEPDIVE_NEIGHBOR_CANDLE_DIR, minute_df  # noqa: E402


def read_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default


def atomic_gzip_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=path.name, suffix=".tmp", dir=path.parent)
    os.close(fd)
    try:
        with open(temporary, "wb") as raw:
            with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0, compresslevel=6) as zipped:
                zipped.write(json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def main() -> int:
    manifest = read_json(ROOT / "data" / "gap_deepdive_2026.json", {})
    daily = read_json(ROOT / "data" / "gap_deepdive_daily_2026.json", {})
    daily_by_ticker = daily.get("tickers", {})
    primary = read_json(ROOT / "data" / "gap_deepdive_candles_2026.json", {}).get("sessions", {})
    events = manifest.get("results", [])

    targets: set[tuple[str, str]] = set()
    for event in events:
        ticker = str(event.get("ticker", "")).upper()
        day = str(event.get("date", ""))
        sessions = daily_by_ticker.get(ticker, [])
        dates = [str(row.get("date", "")) for row in sessions]
        try:
            index = dates.index(day)
        except ValueError:
            print(f"Missing daily anchor {ticker} {day}; refusing incomplete bundle", file=sys.stderr)
            return 1
        for offset in range(max(0, index - 2), min(len(dates), index + 3)):
            targets.add((ticker, dates[offset]))

    missing = {(ticker, day) for ticker, day in targets if f"{ticker}|{day}" not in primary}
    existing: dict[str, dict[str, list[list]]] = {}
    for ticker in {ticker for ticker, _ in missing}:
        path = _DEEPDIVE_NEIGHBOR_CANDLE_DIR / f"{ticker}.json.gz"
        if path.exists():
            try:
                with gzip.open(path, "rt", encoding="utf-8") as source:
                    existing[ticker] = json.load(source).get("sessions", {})
            except Exception as exc:
                print(f"Cannot read existing {path.name}: {exc}", file=sys.stderr)
                return 1
        else:
            existing[ticker] = {}

    pending = sorted((ticker, day) for ticker, day in missing
                     if day not in existing.get(ticker, {}))
    print(f"Events: {len(events)} | unique ±2-session targets: {len(targets)} | "
          f"already bundled: {len(targets)-len(missing)} | to fetch: {len(pending)}", flush=True)

    downloaded: dict[str, dict[str, list[list]]] = {}
    failures: dict[tuple[str, str], str] = {}

    def fetch(item: tuple[str, str]):
        ticker, day = item
        frame = minute_df(ticker, day)
        if frame is None or frame.empty:
            raise RuntimeError("Polygon/cache returned no one-minute bars")
        rows = []
        for timestamp, row in frame.iterrows():
            ms = int(timestamp.tz_convert("UTC").timestamp() * 1000)
            rows.append([ms, float(row.o), float(row.h), float(row.l), float(row.c), int(row.v)])
        if not rows:
            raise RuntimeError("No valid one-minute bars after normalization")
        return ticker, day, rows

    workers = min(50, max(1, len(pending)))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix="deepdive-seed") as pool:
        futures = {pool.submit(fetch, item): item for item in pending}
        for index, future in enumerate(concurrent.futures.as_completed(futures), 1):
            ticker, day = futures[future]
            try:
                name, session, rows = future.result()
                downloaded.setdefault(name, {})[session] = rows
                print(f"[{index}/{len(pending)}] {ticker} {day}: {len(rows)} bars", flush=True)
            except Exception as exc:
                failures[(ticker, day)] = f"{type(exc).__name__}: {exc}"
                print(f"[{index}/{len(pending)}] {ticker} {day}: FAILED ({type(exc).__name__})", flush=True)

    if failures:
        print(f"Incomplete bundle: {len(failures)} session(s) failed; no bundle files were changed.", file=sys.stderr)
        for (ticker, day), error in sorted(failures.items()):
            print(f"  {ticker} {day}: {error}", file=sys.stderr)
        return 1

    all_tickers = sorted(set(existing) | set(downloaded))
    for ticker in all_tickers:
        sessions = dict(existing.get(ticker, {}))
        sessions.update(downloaded.get(ticker, {}))
        sessions = {day: sessions[day] for day in sorted(sessions)}
        payload = {"version": 1, "ticker": ticker, "interval": "1m", "sessions": sessions}
        atomic_gzip_json(_DEEPDIVE_NEIGHBOR_CANDLE_DIR / f"{ticker}.json.gz", payload)

    bar_count = sum(len(rows) for days in downloaded.values() for rows in days.values())
    print(f"Wrote {len(all_tickers)} ticker bundles, {len(downloaded)} new tickers, "
          f"{len(pending)} sessions / {bar_count} bars.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
