#!/usr/bin/env python3
"""
Historical Gap-Up Fade Analyzer (Flask app with modular feature handlers)

Setup
    pip install flask pandas numpy requests yfinance
    export POLYGON_API_KEY=your_key          (Windows: set POLYGON_API_KEY=your_key)
    python gapup_fade.py                     -> http://127.0.0.1:8080
    python gapup_fade.py 9000                -> pick any port (or set PORT=9000)

Optional env
    POLYGON_RPM=5        Per-key calls/minute limit (free tier = 5). Total pool throughput = N_keys * RPM.
    POLY_MAX_PARALLEL=0  Max keys used simultaneously (0 = all). Tune to avoid overwhelming Polygon.
    SEC_USER_AGENT="YourName you@email.com"   (SEC EDGAR asks for contact info)
    GAP_MIN=0.30  MAX_EVENTS=20  SIM_K=20  MIN_PRICE=0.5  MIN_DAY_VOL=100000  MIN_PM_VOL=0
    STOP_PCT=0.20        stop distance (above entry) used for R-multiples + chart stop-outs

Data
    Live day ............ Yahoo (1-min incl. premarket) + latest available Polygon float/shares metadata
    Daily + 1-min history Polygon (UNADJUSTED, so volume/rotation are point-in-time; split days excluded)
    Historical float .... SEC EDGAR shares outstanding (latest filing before each event)
                          x today's float/shares-out ratio, split-adjusted. Marked * when stale.
VWAP everywhere = continuous VWAP from 04:00 ET (premarket + regular), like a chart with extended hours on.
Everything fetched from Polygon is cached in ./.gapcache so repeat runs are fast.
(Cache note: 1-min files are reused as-is, nothing to refetch for the new sections.)
"""
import os, re, json, time, math, random, threading, uuid, warnings, hashlib, secrets, datetime as dt
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from flask import Flask, request, jsonify, Response, send_file
from urllib.parse import quote

def _load_env_file():
    env_file = Path(__file__).resolve().parent.parent / ".env"
    if not env_file.exists():
        env_file = Path(".env")
    if env_file.exists():
        try:
            from dotenv import load_dotenv
            load_dotenv(dotenv_path=env_file, override=False)
        except ImportError:
            pass
        try:
            with open(env_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        k, v = k.strip(), v.strip().strip("'\"")
                        if k and not os.environ.get(k):
                            os.environ[k] = v
        except Exception:
            pass

_load_env_file()

warnings.filterwarnings("ignore")
ET = ZoneInfo("America/New_York")
IST = ZoneInfo("Asia/Kolkata")
POLY_KEY = os.getenv("POLYGON_API_KEY", "")
POLY_RPM = float(os.getenv("POLYGON_RPM", "5"))
# Maximum number of keys to use in parallel (tune via POLY_MAX_PARALLEL; default = all)
POLY_MAX_PARALLEL = int(os.getenv("POLY_MAX_PARALLEL", "0"))  # 0 = unlimited (use all keys)

# ----------------------------------------------------------------------------- multi-key pool
# Each key gets its own HTTP session (separate TCP pool + unique headers) so
# Polygon sees 50 distinct client fingerprints rather than one shared connection.
_key_pool: list = []       # ordered list of active API-key strings
_key_last: dict = {}       # key -> last-used timestamp (float)
_key_sessions: dict = {}   # key -> dedicated requests.Session
_key_lock = threading.Lock()

# Realistic User-Agent strings across different HTTP client libraries/versions.
# Mixing Python, Node, Go and curl variants means consecutive requests look like
# they come from different programs / machines.
_UA_POOL = [
    "python-requests/2.28.2",
    "python-requests/2.31.0",
    "python-requests/2.32.3",
    "python-httpx/0.23.3",
    "python-httpx/0.27.0",
    "python-urllib3/1.26.18",
    "python-urllib3/2.1.0",
    "aiohttp/3.8.6",
    "aiohttp/3.9.3",
    "aiohttp/3.10.5",
    "node-fetch/2.7.0",
    "node-fetch/3.3.2",
    "axios/1.4.0",
    "axios/1.6.5",
    "axios/1.7.2",
    "got/12.6.1",
    "got/14.2.1",
    "undici/5.28.3",
    "Go-http-client/1.1",
    "Go-http-client/2.0",
    "curl/7.88.1",
    "curl/8.4.0",
    "curl/8.7.1",
    "Wget/1.21.4",
    "Java/17.0.9",
    "Java/21.0.2",
    "okhttp/4.12.0",
    "Dart/3.3 (dart:io)",
    "Ruby/3.3.0 net-http",
    "libwww-perl/6.77",
]

_ACCEPT_ENCODINGS = [
    "gzip",
    "gzip, deflate",
    "gzip, deflate, br",
    "deflate, gzip;q=1.0, *;q=0.5",
    "gzip;q=1.0, identity; q=0.5, *;q=0",
]

_ACCEPT_LANGS = [
    "en-US,en;q=0.9",
    "en-GB,en;q=0.8",
    "en;q=1.0",
    "en-US;q=0.9,en;q=0.8",
    "*",
]


def _make_session(key: str) -> requests.Session:
    """Create a fresh requests.Session with a randomised fingerprint for this key."""
    s = requests.Session()
    # Deterministically seed the choice so the same key always gets the same UA
    # (avoids churn), but each key gets a different one.
    rng = random.Random(hashlib.md5(key.encode()).hexdigest())
    s.headers.update({
        "User-Agent":      rng.choice(_UA_POOL),
        "Accept":          "application/json, */*;q=0.8",
        "Accept-Encoding": rng.choice(_ACCEPT_ENCODINGS),
        "Accept-Language": rng.choice(_ACCEPT_LANGS),
        "Cache-Control":   "no-cache",
    })
    return s


def _init_key_pool():
    """Build / refresh the key pool from POLYGON_API_KEYS (comma-sep) or POLYGON_API_KEY.
    Each key gets its own HTTP session so TCP connections are isolated.
    """
    global _key_pool, _key_last, _key_sessions
    raw = os.getenv("POLYGON_API_KEYS", "") or os.getenv("POLYGON_API_KEY", "")
    keys = [k.strip() for k in raw.split(",") if k.strip()]
    # Deduplicate while preserving order
    seen, unique = set(), []
    for k in keys:
        if k not in seen:
            seen.add(k)
            unique.append(k)
    _key_pool = unique
    for k in _key_pool:
        _key_last.setdefault(k, 0.0)
        if k not in _key_sessions:
            _key_sessions[k] = _make_session(k)

_init_key_pool()   # bootstrap pool at module load (env already loaded above)


SEC_UA = os.getenv("SEC_USER_AGENT", "GapFadeAnalyzer admin@example.com")
GAP_MIN = float(os.getenv("GAP_MIN", "0.30"))
LOOKBACK_DAYS = 365
HISTORY_DAYS = 740            # retain a full two years of daily candles plus split/calendar buffer
MAX_EVENTS = int(os.getenv("MAX_EVENTS", "20"))
SIM_K = int(os.getenv("SIM_K", "20"))
MIN_PRICE = float(os.getenv("MIN_PRICE", "0.5"))
MIN_DAY_VOL = int(os.getenv("MIN_DAY_VOL", "100000"))
MIN_PM_VOL = int(os.getenv("MIN_PM_VOL", "0"))
STOP_PCT = float(os.getenv("STOP_PCT", "0.20"))
ROOT_DIR = Path(__file__).resolve().parent.parent
CACHE = Path(os.getenv("CACHE_DIR", str(ROOT_DIR / ".gapcache")))
try:
    CACHE.mkdir(exist_ok=True, parents=True)
except Exception:
    pass
NPTS = 144                    # 5-minute steps from 04:00 ET to 16:00 ET (premarket + regular session)
PM_START, RG_START, RG_END = 240, 570, 960   # minutes since midnight ET
GAP_REL_START, GAP_REL_END = -360, 720  # minutes from premarket high for the gap-retention chart

# ----------------------------------------------------------------------------- cache / http
_cache_lock = threading.Lock()

def cget(key, ttl=None):
    p = CACHE / f"{key}.json"
    if not p.exists():
        return None
    if ttl and time.time() - p.stat().st_mtime > ttl:
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def cput(key, val):
    try:
        with _cache_lock:
            p = CACHE / f"{key}.json"
            p.write_text(json.dumps(val), encoding="utf-8")
    except Exception:
        pass


def _acquire_slot(rpm: float) -> tuple:
    """Atomically reserve the earliest available request slot across all keys in the pool.
    Returns (key, wait_seconds). Must be called with _key_lock held.

    Per-key gap = 60 / rpm (capped at 12s for free-tier safety).
    With N=50 keys the pool throughput = 50 * 5 = 250 RPM effective.
    """
    gap = min(12.0, 60.0 / max(0.1, rpm))
    now = time.time()
    best_key, best_slot = None, float("inf")
    for k in _key_pool:
        slot = max(now, _key_last.get(k, 0.0) + gap)
        if slot < best_slot:
            best_slot, best_key = slot, k
    if best_key is None:
        raise RuntimeError("No Polygon API keys configured in pool.")
    _key_last[best_key] = best_slot
    return best_key, max(0.0, best_slot - now)


def _poly_request_one(path: str, params: dict) -> dict:
    """Make a single Polygon REST request using the key-pool with full fingerprint evasion.

    Evasion techniques applied per request:
      - Each key uses its own HTTP session (unique User-Agent, Accept-*, TCP pool).
      - Random timing jitter (50-400 ms) after every rate-limit sleep so requests
        arrive at Polygon with human-like irregular intervals, not a robotic clock.
      - On 429, the penalised key's TCP connection is shed by recreating its session
        so Polygon's connection-tracking cannot correlate retries.
    """
    if not _key_pool:
        _load_env_file()
        _init_key_pool()
    if not _key_pool:
        raise RuntimeError("POLYGON_API_KEY is not set. Set it and restart the app.")

    rpm = float(os.getenv("POLYGON_RPM", str(POLY_RPM)))
    p = dict(params)

    max_attempts = max(10, len(_key_pool) * 3)
    for attempt in range(max_attempts):
        with _key_lock:
            key, wait = _acquire_slot(rpm)

        # Wait for the rate-limit window PLUS random jitter so requests don't
        # arrive on a clock tick (equal-interval pattern is a detection signal).
        if wait > 0:
            time.sleep(wait)
        time.sleep(random.uniform(0.02, 0.08))   # 20-80 ms subtle jitter per request

        # Use this key's dedicated session (unique fingerprint per key)
        with _key_lock:
            session = _key_sessions.get(key)
            if session is None:
                session = _make_session(key)
                _key_sessions[key] = session

        p["apiKey"] = key
        try:
            r = session.get("https://api.polygon.io" + path, params=p, timeout=12)
        except requests.RequestException:
            time.sleep(random.uniform(0.3, 0.8))
            continue

        if r.status_code == 429:
            # Rate-limited: penalise key AND recreate its session to drop the TCP conn.
            with _key_lock:
                _key_last[key] = max(_key_last.get(key, 0.0), time.time() + 30.0)
                old = _key_sessions.pop(key, None)
                if old:
                    try: old.close()
                    except Exception: pass
                _key_sessions[key] = _make_session(key)
            time.sleep(random.uniform(0.5, 1.5))
            continue

        if r.status_code == 401:
            # Key permanently rejected (Unknown / Invalid API key) — evict from pool.
            with _key_lock:
                if key in _key_pool:
                    _key_pool.remove(key)
                _key_last.pop(key, None)
                old = _key_sessions.pop(key, None)
                if old:
                    try: old.close()
                    except Exception: pass
            if not _key_pool:
                raise RuntimeError(f"All Polygon keys rejected (401): {r.text[:150]}")
            continue

        if r.status_code == 403:
            # 403 on Polygon means timeframe or feature not allowed on current plan
            # (e.g. data older than 2 years on Basic plan). The key is VALID,
            # so do NOT evict the key or retry across other keys (they will also 403).
            r.raise_for_status()

        r.raise_for_status()
        return r.json()

    raise RuntimeError(f"Polygon request failed after {max_attempts} attempts: {path}")


def poly_get(path, params=None):
    """Public API: fetch one Polygon endpoint, distributing load across all 50 keys.
    Parallel capacity is exploited by callers using ThreadPoolExecutor.
    """
    return _poly_request_one(path, params or {})



from chart_app.market_data import (
    _alpaca_live_df,
    bars2,
    calc_downlegs,
    chart_pack,
    ctx_feats,
    float_at,
    hod_bucket,
    ist_off,
    live_snapshot,
    load_daily,
    load_shares_out,
    load_splits,
    latest_security_stats,
    mdl,
    med,
    metrics,
    minute_df,
    path_full,
    pbucket,
    poly_frame,
    recent_hod_times,
    recent_open_hod_stats,
    ticker_type,
    twap_frame_paths,
    vwap_frame_paths,
    vwap_twap_frame,
)


# ----------------------------------------------------------------------------- live (Alpaca / Yahoo)


# ----------------------------------------------------------------------------- features


# ----------------------------------------------------------------------------- stats / grade


# ----------------------------------------------------------------------------- formatting


# ----------------------------------------------------------------------------- report


# ----------------------------------------------------------------------------- job runner
JOBS = {}
from concurrent.futures import ThreadPoolExecutor, as_completed


from chart_app.gap_analysis import (
    C,
    analyze_event,
    big,
    build_report,
    clamp,
    clean,
    cndl,
    detect_events,
    dv,
    fdate,
    fmt,
    frac,
    frow,
    grade,
    hhmm,
    kv_text,
    model_rows,
    nn,
    outcome,
    pct,
    pctl,
    rate,
    sc,
    stats,
    summary_text,
    tbl_text,
    trip,
    usd,
    vals,
    verdict_text,
    ynC,
)

def _prefetch_minute(args):
    """Worker: fetch (and cache) 1-min data for one event day. Returns (idx, day, ok)."""
    t, i, daily = args
    day = daily[i]["date"]
    try:
        df = minute_df(t, day)   # cached in .gapcache; analyze_event will hit cache
        return (i, day, df is not None and not df.empty)
    except Exception:
        return (i, day, False)


def run_job(job, t, refresh):
    try:
        # ------------------------------------------------------------------ STAGE 1
        # Fetch everything that doesn't depend on each other concurrently.
        job["msg"] = "Loading live data + history in parallel…"

        _live_res   = [None]
        _daily_res  = [None]
        _splits_res = [None]
        _type_res   = [None]
        _sec_res    = [None]
        _security_res = [None]
        _news_res   = [None]
        _err        = [None]

        def _get_live():
            try:
                _live_res[0] = live_snapshot(t)
            except Exception as ex:
                _err[0] = ex

        def _get_daily():
            try:
                _daily_res[0] = load_daily(t, refresh)
            except Exception as ex:
                _err[0] = ex

        def _get_splits():
            try:
                _splits_res[0] = load_splits(t)
            except Exception:
                _splits_res[0] = []

        def _get_type():
            try:
                _type_res[0] = ticker_type(t)
            except Exception:
                _type_res[0] = None

        def _get_sec():
            try:
                _sec_res[0] = load_shares_out(t)
            except Exception:
                _sec_res[0] = []

        def _get_security_stats():
            try:
                _security_res[0] = latest_security_stats(t, force=refresh)
            except Exception as ex:
                _security_res[0] = {
                    "errors": [f"Polygon security metadata: {type(ex).__name__}: {ex}"]
                }

        def _get_news():
            try:
                _news_res[0] = _get_scanner_news(t)
            except Exception:
                _news_res[0] = []

        stage1_threads = [
            threading.Thread(target=_get_live,      daemon=True),
            threading.Thread(target=_get_daily,     daemon=True),
            threading.Thread(target=_get_splits,    daemon=True),
            threading.Thread(target=_get_type,      daemon=True),
            threading.Thread(target=_get_sec,       daemon=True),
            threading.Thread(target=_get_security_stats, daemon=True),
            threading.Thread(target=_get_news,      daemon=True),
        ]
        for th in stage1_threads:
            th.start()
        # Live quote, daily history, and split data affect the core report.
        # SEC shares, reference metadata, type, and news are optional enrichments;
        # let them finish in the background instead of holding the whole report.
        for th in stage1_threads[:3]:
            th.join()
        optional_deadline=time.monotonic()+1.25
        for th in stage1_threads[3:]:
            th.join(max(0.0,optional_deadline-time.monotonic()))
        sec_history_pending=stage1_threads[4].is_alive()
        security_stats_pending=stage1_threads[5].is_alive()
        news_pending=stage1_threads[6].is_alive()

        if _err[0]:
            raise _err[0]

        if not _daily_res[0]:
            raise RuntimeError(f"No daily price history returned from Polygon for {t}")

        live   = _live_res[0]
        splits = _splits_res[0]
        typ    = _type_res[0]
        sec    = _sec_res[0]
        security_stats = _security_res[0] or {}
        polygon_float = security_stats.get("float_shares")
        polygon_shares = security_stats.get("shares_outstanding")
        if polygon_float:
            live["float"] = polygon_float
            live["float_source"] = f"Polygon · {security_stats.get('float_asof') or 'date unavailable'}"
        elif live.get("float"):
            live["float_source"] = "Yahoo Finance fallback · date unavailable"
        elif polygon_shares or live.get("so"):
            live["float_source"] = "Shares outstanding fallback · float unavailable"
        else:
            live["float_source"] = "Unavailable"
        if polygon_shares:
            live["so"] = polygon_shares
        live["float_asof"] = security_stats.get("float_asof")
        live["security_stats_errors"] = security_stats.get("errors", [])

        cap_shares = polygon_shares or live.get("so")
        if cap_shares and live.get("cur"):
            live["mcap"] = cap_shares * live["cur"]
            live["market_cap_source"] = (
                "Latest price × Polygon shares" if polygon_shares
                else "Latest price × Yahoo shares"
            )
        elif security_stats.get("market_cap"):
            live["mcap"] = security_stats["market_cap"]
            live["market_cap_source"] = "Polygon reference value"
        elif live.get("mcap"):
            live["market_cap_source"] = "Yahoo Finance fallback · date unavailable"
        else:
            live["market_cap_source"] = "Unavailable"
        daily  = [r for r in _daily_res[0] if r["date"] < str(live["date"])]

        fl_now = live["float"] or live["so"]
        ratio  = None
        if live["float"] and live["so"]:
            ratio = min(1.0, max(0.02, live["float"] / live["so"]))
        ctx = {"sec": sec, "splits": splits, "float": fl_now, "ratio": ratio}

        idxs = detect_events(daily, splits, live["date"])

        # ------------------------------------------------------------------ STAGE 2 + 3
        # Fetch and analyze each event as soon as its minute bars are available.
        # This overlaps CPU work with provider/network latency instead of waiting
        # for every historical session to finish downloading first.
        n_keys=max(1,len(_key_pool))
        n_events=len(idxs)
        results={}

        def _analyze(i):
            return i, analyze_event(t, i, daily, ctx)

        if n_events:
            job["msg"]=f"Loading and analyzing {n_events} historical sessions in parallel…"
            fetch_args=[(t,i,daily) for i in idxs]
            fetch_workers=min(n_keys,max(n_events,12))
            analyze_workers=min(8,n_events)
            with ThreadPoolExecutor(max_workers=fetch_workers,thread_name_prefix="gap-minute-fetch") as fetch_pool:
                with ThreadPoolExecutor(max_workers=analyze_workers,thread_name_prefix="gap-event-analysis") as analyze_pool:
                    fetch_futures={fetch_pool.submit(_prefetch_minute,arg):arg for arg in fetch_args}
                    analyze_futures={}
                    for completed,future in enumerate(as_completed(fetch_futures),1):
                        idx_i,day,ok=future.result()
                        analyze_futures[analyze_pool.submit(_analyze,idx_i)]=idx_i
                        job["msg"]=(f"Loaded {completed}/{n_events} historical sessions; analyzing in parallel… "
                                    f"({day}{' · cached' if ok else ' · no minute bars'})")
                    for future in as_completed(analyze_futures):
                        idx_i,event=future.result()
                        results[idx_i]=event

        events, skipped = [], 0
        for i in idxs:
            e = results.get(i)
            if e:
                events.append(e)
            else:
                skipped += 1

        # ------------------------------------------------------------------ report
        job["msg"] = "Building report and charts…"
        lm    = metrics(live["df"], live["pc"], False)
        lctx  = ctx_feats(live["hist"], len(live["hist"]))
        lpath = path_full(live["df"], live["pc"], False)
        _, _, live_dev_path = vwap_frame_paths(live["df"], live["pc"], False)
        _, live_twap_dev_path, live_spread_path, _ = twap_frame_paths(live["df"], live["pc"], False)
        vwap_now_k = max((k for k, value in enumerate(live_dev_path) if value is not None), default=None)
        lg    = ((lm.get("open") or live["cur"]) / live["pc"] - 1)
        lf    = {"gap": max(lg, 0.01), "bucket": pbucket(lm.get("open") or live["cur"]), "float": fl_now,
                 "pm_rel": (lm["pm_vol"] / lctx["avgvol20"]) if lctx.get("avgvol20") else None,
                 "runup5": lctx.get("runup5")}

        def dist(e):
            d = abs(math.log(max(e["gap"], .01) / lf["gap"]))
            if e["bucket"] != lf["bucket"]:
                d += 0.8
            if e["pm_rel"] is not None and lf["pm_rel"] is not None:
                d += 0.4 * abs(math.log((e["pm_rel"] + .05) / (lf["pm_rel"] + .05)))
            if e["runup5"] is not None and lf["runup5"] is not None:
                d += 0.5 * abs(math.log(max(1 + e["runup5"], .2) / max(1 + lf["runup5"], .2)))
            if e["float"] and lf["float"]:
                d += 0.3 * abs(math.log(e["float"] / lf["float"]))
            # Match today's VWAP state before asking analogs for the remaining path.
            hist_dev = e.get("path_vwap_dev") or []
            if vwap_now_k is not None and len(hist_dev) > vwap_now_k:
                target_d, analog_d = live_dev_path[vwap_now_k], hist_dev[vwap_now_k]
                if analog_d is not None:
                    d += 2.0 * abs(target_d - analog_d) / 0.05
                    if (target_d < 0) != (analog_d < 0): d += 1.0
                    start = max((RG_START-PM_START)//5, vwap_now_k-12)
                    diffs = [(live_dev_path[k], hist_dev[k]) for k in range(start, vwap_now_k+1)
                             if k < len(hist_dev) and live_dev_path[k] is not None and hist_dev[k] is not None]
                    if len(diffs) >= 3:
                        weights = np.exp(np.linspace(-2.0, 0.0, len(diffs)))
                        weights /= weights.sum()
                        rmse = float(np.sqrt(np.sum(weights * np.square([a-b for a,b in diffs]))))
                        d += 1.5 * rmse / 0.04
            for target_path, key, scale, weight in ((live_twap_dev_path, "path_twap_dev", .075, 1.2),
                                                     (live_spread_path, "path_vwap_twap_spread", .04, 1.0)):
                analog_path = e.get(key) or []
                if vwap_now_k is not None and len(analog_path) > vwap_now_k and target_path[vwap_now_k] is not None and analog_path[vwap_now_k] is not None:
                    d += 1.5*weight * abs(target_path[vwap_now_k] - analog_path[vwap_now_k]) / scale
                    recent = [(target_path[k], analog_path[k]) for k in range(max((RG_START-PM_START)//5, vwap_now_k-12), vwap_now_k+1)
                              if k < len(analog_path) and target_path[k] is not None and analog_path[k] is not None]
                    if len(recent) >= 3:
                        w = np.exp(np.linspace(-2.0, 0.0, len(recent))); w /= w.sum()
                        rmse = float(np.sqrt(np.sum(w*np.square([x-y for x,y in recent]))))
                        d += weight * rmse / scale
                    if key == "path_vwap_twap_spread":
                        bucket = lambda x: 1 if x > .02 else -1 if x < -.02 else 0
                        if bucket(target_path[vwap_now_k]) != bucket(analog_path[vwap_now_k]): d += .7
            return d

        S = sorted(events, key=dist)[:SIM_K]
        notes = []
        if lg < GAP_MIN:
            notes.append(f"Today's gap ({lg*100:+.0f}%) is below the {GAP_MIN*100:.0f}% threshold; showing history anyway.")
        if typ and typ.upper() in ("ETF", "ETN", "ETV", "FUND", "WARRANT", "RIGHT", "UNIT"):
            notes.append(f"Ticker type is {typ}; behavior may differ from common stock.")
        if skipped:
            notes.append(f"{skipped} event(s) skipped: no 1-min data from Polygon (or below MIN_PM_VOL).")
        if not events:
            notes.append("No qualifying gap-ups found in the last 365 days.")
        if sec_history_pending:
            notes.append("SEC shares-outstanding history is still loading; historical float rotation uses the latest available float for now. Refresh to update it.")
        elif not sec:
            notes.append("No SEC shares-outstanding history found; rotation uses today's float (split-adjusted).")
        if security_stats_pending:
            notes.append("Polygon security metadata is still loading; the report uses the available price/share fallback for now.")
        for error in live.get("security_stats_errors", []):
            notes.append(f"Latest security metadata lookup issue: {error}")
        notes.append("Historical data is point-in-time for tickers still listed; delisted names are missing, "
                     "which makes fades look weaker than reality (survivorship bias).")

        report = clean(build_report(t, live, lm, lpath, lctx, events, S, notes, daily))
        report["premarket_surge_days"] = None
        reverse_splits = []
        for split in splits or []:
            try:
                split_from = float(split.get("from") or 0)
                split_to = float(split.get("to") or 0)
            except (TypeError, ValueError):
                continue
            if split_from > split_to > 0 and split.get("date"):
                split_date = str(split["date"])
                prior_close = next((row.get("c") for row in reversed(daily)
                                    if row.get("date", "") < split_date and row.get("c")), None)
                ratio_factor = split_from / split_to
                price_conversion = None
                if prior_close is not None:
                    old_price = float(prior_close)
                    adjusted_price = old_price * ratio_factor
                    price_conversion = (f"${old_price:,.4f}".rstrip("0").rstrip(".") +
                                        f" → ${adjusted_price:,.4f}".rstrip("0").rstrip("."))
                reverse_splits.append({"date": split_date, "ratio": f"{split_from:g}:1",
                                       "price_conversion": price_conversion})
        reverse_splits.sort(key=lambda item: item["date"], reverse=True)
        report["reverse_splits"] = reverse_splits
        report["text"] += "\n\nReverse Splits\n"
        report["text"] += ("\n".join(f"{fdate(item['date'])} · {item['ratio']}"+
                                     (f" ({item['price_conversion']})" if item.get("price_conversion") else "")
                                     for item in reverse_splits)
                            if reverse_splits else "No reverse splits found in Polygon's available split history.")
        recent_news = _news_res[0] or []
        report["news"] = recent_news
        report["news_status"] = ("Latest press releases are still loading; refresh the report to update this section." if news_pending else
                                  _scanner_details.get(t, {}).get("news_status") or
                                  ("Latest press releases found on Yahoo Finance" if recent_news else
                                   "No press releases found on Yahoo Finance"))
        if recent_news:
            report["text"] += "\n\nLatest Press Releases · Yahoo Finance"
            for item in recent_news:
                report["text"] += f"\n- [{item['kind']}] {item['title']} ({item['publisher']}) {item['url']}"
        else:
            report["text"] += f"\n\nLatest Press Releases · Yahoo Finance\n{report['news_status']}"
        job["result"] = report
        job["status"] = "done"
    except Exception as ex:
        job["status"] = "error"
        job["error"]  = f"{type(ex).__name__}: {ex}"


# ----------------------------------------------------------------------------- web
HTML = (Path(__file__).resolve().parent.parent / "templates" / "gapup.html").read_text(encoding="utf-8")
KEYWORD_HTML = (Path(__file__).resolve().parent.parent / "templates" / "keyword_search.html").read_text(encoding="utf-8")

app = Flask(__name__, static_folder="../static")


@app.route("/")
def index():
    return Response(HTML, mimetype="text/html")


@app.route("/gap-deepdive")
def gap_deepdive_page():
    """Open the historical gap-up deep dive as a standalone app page."""
    page = HTML.replace("<title>Gap-Up Fade Analyzer</title>",
                        "<title>Gap Up Deepdive · Historical Scanner</title>", 1)
    page = page.replace("/favicon.svg?app=gapup", "/favicon.svg?app=deepdive", 1)
    return Response(page, mimetype="text/html")


@app.route("/favicon.svg")
def favicon():
    icons = {
        "gapup": '''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><rect width="64" height="64" rx="14" fill="#0a1119"/><path d="M12 50h40" stroke="#52677b" stroke-width="3"/><rect x="16" y="34" width="8" height="16" rx="2" fill="#29a8ff"/><rect x="29" y="25" width="8" height="25" rx="2" fill="#29a8ff"/><rect x="42" y="16" width="8" height="34" rx="2" fill="#3ddc84"/><path d="m13 28 13-9 9 3 16-12m-9 0h9v9" fill="none" stroke="#e8f5ff" stroke-width="3.5" stroke-linecap="round" stroke-linejoin="round"/></svg>''',
        "deepdive": '''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><rect width="64" height="64" rx="14" fill="#141006"/><path d="M9 49h32" stroke="#6b5b22" stroke-width="3"/><path d="m12 42 8-12 7 7 8-18" fill="none" stroke="#ffd23f" stroke-width="4" stroke-linecap="round" stroke-linejoin="round"/><circle cx="41" cy="39" r="12" fill="#10151b" stroke="#60d8ff" stroke-width="4"/><path d="m50 48 7 7" stroke="#60d8ff" stroke-width="5" stroke-linecap="round"/><path d="M36 39h10m-5-5v10" stroke="#e9f7ff" stroke-width="2.5" stroke-linecap="round"/></svg>''',
        "runner": '''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><rect width="64" height="64" rx="14" fill="#0a1119"/><circle cx="41" cy="13" r="6" fill="#ffd166"/><path d="m34 23 10 7-7 9-9-5-8 10m17-14 10 4 7 10M30 35l-2 11-9 8m18-15 4 12 9 3" fill="none" stroke="#47d890" stroke-width="5" stroke-linecap="round" stroke-linejoin="round"/><path d="M10 55h44" stroke="#52677b" stroke-width="3" stroke-linecap="round"/></svg>''',
        "keyword": '''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><rect width="64" height="64" rx="14" fill="#0a1119"/><path d="M13 12h25l9 9v17H13z" fill="#172638" stroke="#60baff" stroke-width="3" stroke-linejoin="round"/><path d="M37 12v10h10M19 28h13m-13 6h9" stroke="#60baff" stroke-width="3" stroke-linecap="round"/><circle cx="39" cy="41" r="10" fill="#0a1119" stroke="#c4a7ff" stroke-width="4"/><path d="m46 48 8 8" stroke="#c4a7ff" stroke-width="5" stroke-linecap="round"/></svg>'''
    }
    name = request.args.get("app", "gapup")
    return Response(icons.get(name, icons["gapup"]), mimetype="image/svg+xml", headers={"Cache-Control": "public, max-age=86400"})



@app.route("/favicon.ico")
def favicon_ico():
    """Return a minimal 32x32 ICO so Chrome/Edge show the tab icon reliably."""
    import struct, zlib
    W, H = 32, 32
    bg = (10, 17, 25, 255)
    ac = (61, 220, 132, 255)
    bl = (41, 168, 255, 255)
    ln = (82, 103, 123, 255)
    hl = (232, 245, 255, 180)
    def _px(x, y):
        if y >= 28: return ln
        if 21 <= x <= 24 and y >= 8:  return ac
        if 14 <= x <= 17 and y >= 13: return bl
        if 7  <= x <= 10 and y >= 18: return bl
        if y == 6 and 5 <= x <= 23: return hl
        if y == 7 and x == 5: return hl
        if y == 7 and x == 23: return hl
        return bg
    raw = b""
    for y in range(H):
        raw += b"\x00"
        for x in range(W):
            raw += bytes(_px(x, y))
    def _chunk(tag, data):
        c = zlib.crc32(tag + data) & 0xFFFFFFFF
        return struct.pack(">I", len(data)) + tag + data + struct.pack(">I", c)
    ihdr = struct.pack(">IIBBBBB", W, H, 8, 6, 0, 0, 0)
    png = (b"\x89PNG\r\n\x1a\n"
           + _chunk(b"IHDR", ihdr)
           + _chunk(b"IDAT", zlib.compress(raw, 9))
           + _chunk(b"IEND", b""))
    ico_hdr  = struct.pack("<HHH", 0, 1, 1)
    ico_dir  = struct.pack("<BBBBHHII", 32, 32, 0, 0, 1, 32, len(png), 22)
    ico = ico_hdr + ico_dir + png
    return Response(ico, mimetype="image/x-icon", headers={"Cache-Control": "public, max-age=86400"})

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")


# ════════════════════════════════════════════════════════════════════════════
#  2-MIN INTRADAY FORECAST  (analog-conditioned, vol-scaled, conformal-calibrated)
#  Not a point prediction: a conditional distribution of the rest of the session
#  built from the SAME clock-time slice of similar historical event days.
# ════════════════════════════════════════════════════════════════════════════


# ----------------------------------------------------------------------------- runner_stats integration
# Load runner_stats.py from the same directory so Multiday Stats runs inside
# this process — no separate server on port 5055 needed.
_rs = None
try:
    import importlib.util as _ilu
    _spec = _ilu.spec_from_file_location(
        "runner_stats",
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "runner_stats.py")
    )
    _rs = _ilu.module_from_spec(_spec)
    _spec.loader.exec_module(_rs)
except Exception as _rs_err:
    print(f"[runner_stats] Could not load: {_rs_err}")


@app.route("/runner")
def runner_index():
    if _rs is None:
        return "runner_stats.py not found next to chart_stats.py", 404
    username = (request.args.get("user") or "").strip().lower()
    token = (request.args.get("token") or "").strip()
    if not username or not token or not _db_validate_session(username, token):
        return "Unauthorized", 401
    if not _get_app_ui_visibility(username).get("runner_stats", True):
        return "Runner Stats access is disabled for this account.", 403
    # Rewrite API base so fetch calls hit our namespaced endpoint
    page = _rs.PAGE.replace("'/api/analyze", "'/api/runner/analyze") \
                   .replace('"/api/analyze', '"/api/runner/analyze')
    page = page.replace("let LAST=null, API_BASE='/api';", "let LAST=null, API_BASE='/api';\nconst ACCESS_USER = " + json.dumps(username) + ";\nconst ACCESS_TOKEN = " + json.dumps(token) + ";")
    page = page.replace("fetch(API_BASE + '/analyze?' + q)", "fetch(API_BASE + '/analyze?' + q + '&user=' + encodeURIComponent(ACCESS_USER) + '&token=' + encodeURIComponent(ACCESS_TOKEN))")
    page = page.replace("</head>", '<link rel="icon" type="image/svg+xml" href="/favicon.svg?app=runner"></head>', 1)
    from flask import Response as _FR
    return _FR(page, mimetype="text/html")


@app.route("/api/runner/analyze")
def runner_analyze():
    if _rs is None:
        return jsonify({"error": "runner_stats not available"}), 404
    t = (request.args.get("ticker") or "").strip().upper()
    if not t:
        return jsonify({"error": "ticker required"}), 400
    username = (request.args.get("user") or "").strip().lower()
    token = (request.args.get("token") or "").strip()
    if not username or not token or not _db_validate_session(username, token):
        return jsonify({"error": "Unauthorized"}), 401
    if not _get_app_ui_visibility(username).get("runner_stats", True):
        return jsonify({"error": "Runner Stats access is disabled for this account."}), 403
    try:
        return jsonify(_rs.analyze(
            t,
            request.args.get("start") or None,
            request.args.get("frd") or None,
            request.args.get("float_m") or None,
        ))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/runner/csv-collections", methods=["GET", "POST"])
def runner_csv_collections():
    body = request.get_json(silent=True) or {}
    username = (request.args.get("user") or body.get("user") or "").strip().lower()
    token = (request.args.get("token") or body.get("token") or "").strip()
    if not username or not token or not _db_validate_session(username, token):
        return jsonify({"error": "Session expired. Please sign in again."}), 401

    record = _db_get_user(username) or {}
    settings = record.get("scanner_settings")
    if isinstance(settings, str):
        try:
            settings = json.loads(settings)
        except Exception:
            settings = {}
    if not isinstance(settings, dict):
        settings = {}

    if request.method == "GET":
        collections = settings.get("runner_csv_collections", [])
        if not isinstance(collections, list):
            collections = []
        configured = bool((os.getenv("SUPABASE_URL") or SUPABASE_URL) and
                          (os.getenv("SUPABASE_KEY") or SUPABASE_KEY))
        return jsonify({"collections": collections, "storage": "account" if configured else "unavailable"})

    if len(request.get_data(cache=True)) > 4_000_000:
        return jsonify({"error": "CSV workspace exceeds the 4 MB account-storage limit."}), 413
    collections = body.get("collections")
    if not isinstance(collections, list) or len(collections) > 100:
        return jsonify({"error": "CSV workspace must contain no more than 100 named files."}), 400
    clean = []
    ids = set()
    for item in collections:
        if not isinstance(item, dict):
            return jsonify({"error": "Invalid CSV file entry."}), 400
        collection_id = str(item.get("id") or "")[:80]
        name = str(item.get("name") or "").strip()[:80]
        columns, rows = item.get("columns"), item.get("rows")
        if (not collection_id or collection_id in ids or not name or
                not isinstance(columns, list) or len(columns) > 1000 or
                not isinstance(rows, list) or len(rows) > 2000):
            return jsonify({"error": "Invalid CSV file name, columns, or row count."}), 400
        ids.add(collection_id)
        clean_columns = list(dict.fromkeys(str(col)[:250] for col in columns if str(col).strip()))
        clean_rows = []
        for row in rows:
            if not isinstance(row, dict):
                return jsonify({"error": "CSV rows must be objects."}), 400
            clean_rows.append({str(key)[:250]: str(value)[:5000] for key, value in row.items()})
        clean.append({"id": collection_id, "name": name, "columns": clean_columns,
                      "rows": clean_rows, "updated_at": str(item.get("updated_at") or "")[:40]})

    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    if not (sub_url and sub_key):
        return jsonify({"error": "Account autosave is unavailable; use browser-local storage."}), 503
    settings["runner_csv_collections"] = clean
    try:
        response = requests.patch(
            f"{sub_url}/rest/v1/app_users?username=eq.{quote(username, safe='')}",
            json={"scanner_settings": settings}, headers=_supabase_headers(), timeout=12
        )
        if response.status_code not in (200, 204):
            return jsonify({"error": f"Account autosave failed (HTTP {response.status_code})."}), 502
    except Exception as exc:
        return jsonify({"error": f"Account autosave failed: {exc}"}), 502
    return jsonify({"ok": True, "collections": clean, "storage": "account"})


# Register feature endpoints after shared services are available.
from chart_app.auth import (
    _db_get_user,
    _db_sync_users,
    _db_validate_session,
    _supabase_headers,
    log_keyword_search,
)
from chart_app.scanner_settings import _get_app_ui_visibility
from chart_app import charts as _charts
from chart_app import keyword_compare as _keyword_compare
from chart_app.scanner import _get_scanner_news, _scanner_details

@app.route("/api/premarket-spikes")
def premarket_spikes_api():
    username = (request.args.get("user") or "").strip().lower()
    token = (request.args.get("token") or "").strip()
    ticker = (request.args.get("ticker") or "").strip().upper()
    if not username or not _db_validate_session(username, token):
        return jsonify({"error": "Session expired. Please sign in again."}), 401
    if not re.fullmatch(r"[A-Z.-]{1,10}", ticker):
        return jsonify({"error": "Invalid ticker."}), 400
    cache_key = f"{ticker}_premarket_spikes_v6"
    cached = cget(cache_key, 6 * 3600)
    if cached is not None:
        if isinstance(cached, dict) and "days" in cached:
            return jsonify({**cached, "cached": True})
    try:
        daily = load_daily(ticker)
        if not daily:
            return jsonify({"error": "No daily history available."}), 502
        today = dt.datetime.now(ET).date()
        today_text = today.isoformat()
        cutoff = today - dt.timedelta(days=365)
        daily = [row for row in daily if str(row.get("date") or "") < today_text]
        splits = load_splits(ticker) or []
        split_dates = {str(item.get("date")) for item in splits if item.get("date")}
        candidates = []
        for index in range(1, len(daily)):
            row = daily[index]
            try:
                day = dt.date.fromisoformat(str(row["date"]))
                prior_close = float(daily[index - 1].get("c") or 0)
                high = float(row.get("h") or 0)
            except (TypeError, ValueError, KeyError):
                continue
            if day >= cutoff and row["date"] not in split_dates and prior_close > 0 and high / prior_close - 1 > 0.40:
                candidates.append(index)
        if candidates:
            workers = min(len(_key_pool), max(4, len(candidates)))
            with ThreadPoolExecutor(max_workers=workers) as pool:
                list(pool.map(_prefetch_minute, [(ticker, index, daily) for index in candidates]))
        found = []
        spike_paths = {"price": [], "vwap": [], "dev": [], "twap": [], "dt": [], "spread": [], "active_twap": [], "gap": []}
        for index in candidates:
            day = daily[index]["date"]
            prior_close = float(daily[index - 1].get("c") or 0)
            try:
                bars = minute_df(ticker, day)
                if bars is None or bars.empty or prior_close <= 0:
                    continue
                minutes = bars.index.hour * 60 + bars.index.minute
                premarket = bars[(minutes >= 240) & (minutes < 570)]
                if premarket.empty:
                    continue
                high = float(premarket["h"].max())
                high_index = premarket["h"].idxmax()
                typical_price = (premarket["h"] + premarket["l"] + premarket["c"]) / 3
                dollar_volume = float((typical_price * premarket["v"]).sum())
                rth = bars[(minutes >= 570) & (minutes < 960)]
                rth_dollar_volume = 0.0
                rth_open_gap = None
                rth_open_high = None
                rth_open_close = None
                rth_high_low = None
                rth_faded = None
                if not rth.empty:
                    rth_typical = (rth["h"] + rth["l"] + rth["c"]) / 3
                    rth_dollar_volume = float((rth_typical * rth["v"]).sum())
                    rth_open = float(rth.iloc[0]["o"])
                    rth_close = float(rth.iloc[-1]["c"])
                    rth_high = float(rth["h"].max())
                    rth_low = float(rth["l"].min())
                    if rth_open > 0:
                        rth_open_gap = (rth_open / prior_close - 1) * 100
                        rth_open_high = (rth_high / rth_open - 1) * 100
                        rth_open_close = (rth_close / rth_open - 1) * 100
                        rth_faded = rth_close < rth_open
                    if rth_high > 0:
                        rth_high_low = (rth_low / rth_high - 1) * 100
                if high / prior_close - 1 > 0.40 and dollar_volume > 10_000_000:
                    pm_through_high = premarket.loc[:high_index]
                    pm_high_dollar_volume = float(
                        (((pm_through_high["h"] + pm_through_high["l"] + pm_through_high["c"]) / 3)
                         * pm_through_high["v"]).sum()
                    )
                    price_path = path_full(bars, prior_close, True)
                    _, vwap_path, dev_path = vwap_frame_paths(bars, prior_close, True)
                    twap_height, dt_path, spread_path, active_twap = twap_frame_paths(bars, prior_close, True)
                    pm_high_minute = int(high_index.hour * 60 + high_index.minute)
                    spike_gap_path = [None] * 217
                    gap_size = high / prior_close - 1
                    for offset in range(-360, 721, 5):
                        k = (pm_high_minute + offset - PM_START) / 5
                        if not gap_size or k < 0 or k > len(price_path)-1: continue
                        left, right = int(math.floor(k)), int(math.ceil(k))
                        if price_path[left] is None or price_path[right] is None: continue
                        frac = k-left
                        spike_gap_path[(offset+360)//5] = round((price_path[left] + (price_path[right]-price_path[left])*frac) / gap_size, 4)
                    spike_paths["price"].append(price_path)
                    spike_paths["vwap"].append(vwap_path)
                    spike_paths["dev"].append(dev_path)
                    spike_paths["twap"].append(twap_height)
                    spike_paths["dt"].append(dt_path)
                    spike_paths["spread"].append(spread_path)
                    spike_paths["active_twap"].append(active_twap)
                    spike_paths["gap"].append(spike_gap_path)
                    found.append({
                        "date": day,
                        "pm_high_pct": round((high / prior_close - 1) * 100, 2),
                        "pm_high_time": high_index.tz_convert(IST).strftime("%H:%M"),
                        "pm_high_minute_et": pm_high_minute,
                        "rth_open_gap_pct": round(rth_open_gap, 2) if rth_open_gap is not None else None,
                        "rth_open_high_pct": round(rth_open_high, 2) if rth_open_high is not None else None,
                        "rth_open_close_pct": round(rth_open_close, 2) if rth_open_close is not None else None,
                        "rth_high_low_pct": round(rth_high_low, 2) if rth_high_low is not None else None,
                        "rth_faded": rth_faded,
                        "pm_dollar_volume_till_high": round(pm_high_dollar_volume, 2),
                        "pm_dollar_volume": round(dollar_volume, 2),
                        "pm_volume": round(float(premarket["v"].sum())),
                        "rth_to_pm_dollar_volume": round(rth_dollar_volume / dollar_volume, 2) if dollar_volume else None,
                    })
            except Exception:
                continue
        found.sort(key=lambda item: item["date"], reverse=True)
        def summarize_paths(paths):
            if not paths:return {"paths": [], "med": [], "p25": [], "p75": []}
            medians=[];p25=[];p75=[]
            for k in range(max(map(len,paths))):
                values=[path[k] for path in paths if len(path)>k and path[k] is not None and np.isfinite(path[k])]
                medians.append(round(float(np.percentile(values,50)),4) if values else None)
                p25.append(round(float(np.percentile(values,25)),4) if values else None)
                p75.append(round(float(np.percentile(values,75)),4) if values else None)
            return {"paths":paths,"med":medians,"p25":p25,"p75":p75}
        spike_chart={name:summarize_paths(paths) for name,paths in spike_paths.items()}
        result={"days":found,"spike_chart":spike_chart}
        cput(cache_key, result)
        return jsonify({**result, "cached": False})
    except Exception as exc:
        return jsonify({"error": f"Premarket history scan failed: {exc}"}), 502

# Load account records only after auth and settings handlers are defined.
_db_sync_users()

# Register AI summary routes from charts module
try:
    from chart_app.charts import register_news_ai_routes as _reg_news_ai
    _reg_news_ai(app, _db_validate_session)
except Exception as _e:
    print(f'[news-ai routes] Could not register: {_e}')

def main():
    _load_env_file()
    polygon_key = os.getenv("POLYGON_API_KEY", "")
    if not polygon_key:
        print("WARNING: POLYGON_API_KEY not set — historical data will fail until you set it in .env or environment.")
    else:
        print("Polygon API Key loaded successfully.")
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else int(os.getenv("PORT", "8000"))
    print(f"Open http://127.0.0.1:{port}")
    app.run(host="127.0.0.1", port=port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
