#!/usr/bin/env python3
"""
Historical Gap-Up Fade Analyzer (Flask app with modular feature handlers)

Setup
    pip install flask pandas numpy requests yfinance
    export POLYGON_API_KEY=your_key          (Windows: set POLYGON_API_KEY=your_key)
    python gapup_fade.py                     -> http://127.0.0.1:8080
    python gapup_fade.py 9000                -> pick any port (or set PORT=9000)

Optional env
    POLYGON_RPM=5        Polygon calls/minute (free tier = 5, paid = set high e.g. 600)
    SEC_USER_AGENT="YourName you@email.com"   (SEC EDGAR asks for contact info)
    GAP_MIN=0.30  MAX_EVENTS=20  SIM_K=20  MIN_PRICE=0.5  MIN_DAY_VOL=100000  MIN_PM_VOL=0
    STOP_PCT=0.20        stop distance (above entry) used for R-multiples + chart stop-outs

Data
    Live day ............ Yahoo (1-min incl. premarket) + Yahoo info for float
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

# ----------------------------------------------------------------------------- multi-key pool
# Loaded after env is ready (see _init_key_pool())
_key_pool: list = []          # list of API-key strings
_key_last: dict  = {}         # key -> last-call timestamp
_key_lock = threading.Lock()
_poly_session = requests.Session()          # persistent HTTP session: TCP reuse saves ~150ms/call
_poly_session.headers.update({"Accept-Encoding": "gzip"})


def _init_key_pool():
    """Build / refresh the key pool from POLYGON_API_KEYS (comma-sep) or POLYGON_API_KEY."""
    global _key_pool, _key_last
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

_init_key_pool()   # bootstrap pool at module load (env already loaded above)

SEC_UA = os.getenv("SEC_USER_AGENT", "GapFadeAnalyzer admin@example.com")
GAP_MIN = float(os.getenv("GAP_MIN", "0.30"))
LOOKBACK_DAYS = 365
HISTORY_DAYS = 725            # extra daily history so SMA50 / 52w-high exist for the oldest event
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
    """
    # Use 11s gap (5.45 effective RPM per key) — safe within Polygon's rolling 60s window
    gap = min(11.0, 60.0 / max(0.1, rpm))
    now = time.time()
    best_key = None
    best_slot = float("inf")
    for k in _key_pool:
        last = _key_last.get(k, 0.0)
        slot = max(now, last + gap)
        if slot < best_slot:
            best_slot = slot
            best_key = k
    if best_key is None:
        raise RuntimeError("No Polygon API keys configured in pool.")
    _key_last[best_key] = best_slot
    wait = max(0.0, best_slot - now)
    return best_key, wait


def poly_get(path, params=None):
    if not _key_pool:
        _load_env_file()
        _init_key_pool()
    if not _key_pool:
        raise RuntimeError("POLYGON_API_KEY is not set. Set it and restart the app.")

    rpm = float(os.getenv("POLYGON_RPM", str(POLY_RPM)))
    params = dict(params or {})

    max_attempts = max(10, len(_key_pool) * 3)
    for attempt in range(max_attempts):
        with _key_lock:
            key, wait = _acquire_slot(rpm)

        if wait > 0:
            time.sleep(wait)

        params["apiKey"] = key
        try:
            r = _poly_session.get("https://api.polygon.io" + path, params=params, timeout=12)
        except requests.RequestException:
            time.sleep(0.5)
            continue

        if r.status_code == 429:
            # Penalise this key: push its next slot 30s into the future (fast recovery)
            with _key_lock:
                _key_last[key] = max(_key_last.get(key, 0.0), time.time() + 30.0)
            continue

        if r.status_code in (401, 403):
            # If a key is rejected, remove from active pool so other keys can proceed
            with _key_lock:
                if key in _key_pool:
                    _key_pool.remove(key)
            if not _key_pool:
                raise RuntimeError(f"All Polygon keys rejected ({r.status_code}): {r.text[:150]}")
            continue

        r.raise_for_status()
        return r.json()

    raise RuntimeError("All Polygon keys exhausted / rate-limited. Try again in a moment.")


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

        stage1_threads = [
            threading.Thread(target=_get_live,      daemon=True),
            threading.Thread(target=_get_daily,     daemon=True),
            threading.Thread(target=_get_splits,    daemon=True),
            threading.Thread(target=_get_type,      daemon=True),
            threading.Thread(target=_get_sec,       daemon=True),
        ]
        for th in stage1_threads:
            th.start()
        for th in stage1_threads:
            th.join()

        if _err[0]:
            raise _err[0]

        if not _daily_res[0]:
            raise RuntimeError(f"No daily price history returned from Polygon for {t}")

        live   = _live_res[0]
        splits = _splits_res[0]
        typ    = _type_res[0]
        sec    = _sec_res[0]
        daily  = [r for r in _daily_res[0] if r["date"] < str(live["date"])]

        fl_now = live["float"] or live["so"]
        ratio  = None
        if live["float"] and live["so"]:
            ratio = min(1.0, max(0.02, live["float"] / live["so"]))
        ctx = {"sec": sec, "splits": splits, "float": fl_now, "ratio": ratio}

        idxs = detect_events(daily, splits, live["date"])

        # ------------------------------------------------------------------ STAGE 2
        # Pre-fetch all 1-min files in parallel.
        # With N keys the pool fires up to N concurrent requests; each key self-throttles
        # to its own 12-second window via _acquire_slot, so rate limits are strictly respected.
        n_keys   = max(1, len(_key_pool))
        n_events = len(idxs)

        if n_events:
            job["msg"] = (f"Fetching 1-min data for {n_events} event days "
                          f"in parallel ({n_keys} keys active)…")

            fetch_args = [(t, i, daily) for i in idxs]
            workers = min(20, max(n_keys * 2, 12))
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futs = {pool.submit(_prefetch_minute, a): a for a in fetch_args}
                done = 0
                for fut in as_completed(futs):
                    done += 1
                    _, day, ok = fut.result()
                    job["msg"] = (f"Cached {done}/{n_events} minute files… "
                                  f"({day}{'  ✓' if ok else '  ✗ skipped'})")

        # ------------------------------------------------------------------ STAGE 3
        # analyze_event is now fully CPU-bound (all 1-min data is in cache).
        # Run all events in parallel.
        job["msg"] = "Analyzing events (parallel)…"

        def _analyze(i):
            return i, analyze_event(t, i, daily, ctx)

        results = {}
        with ThreadPoolExecutor(max_workers=min(12, max(1, n_events))) as pool:
            futs = {pool.submit(_analyze, i): i for i in idxs}
            for fut in as_completed(futs):
                idx_i, e = fut.result()
                results[idx_i] = e

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
        if not sec:
            notes.append("No SEC shares-outstanding history found; rotation uses today's float (split-adjusted).")
        notes.append("Historical data is point-in-time for tickers still listed; delisted names are missing, "
                     "which makes fades look weaker than reality (survivorship bias).")

        report = clean(build_report(t, live, lm, lpath, lctx, events, S, notes, daily))
        recent_news = _get_scanner_news(t)
        report["news"] = recent_news
        report["news_status"] = (_scanner_details.get(t, {}).get("news_status") or
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

app = Flask(__name__)


@app.route("/")
def index():
    return Response(HTML, mimetype="text/html")


@app.route("/favicon.svg")
def favicon():
    icons = {
        "gapup": '''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><rect width="64" height="64" rx="14" fill="#0a1119"/><path d="M12 50h40" stroke="#52677b" stroke-width="3"/><rect x="16" y="34" width="8" height="16" rx="2" fill="#29a8ff"/><rect x="29" y="25" width="8" height="25" rx="2" fill="#29a8ff"/><rect x="42" y="16" width="8" height="34" rx="2" fill="#3ddc84"/><path d="m13 28 13-9 9 3 16-12m-9 0h9v9" fill="none" stroke="#e8f5ff" stroke-width="3.5" stroke-linecap="round" stroke-linejoin="round"/></svg>''',
        "runner": '''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><rect width="64" height="64" rx="14" fill="#0a1119"/><circle cx="41" cy="13" r="6" fill="#ffd166"/><path d="m34 23 10 7-7 9-9-5-8 10m17-14 10 4 7 10M30 35l-2 11-9 8m18-15 4 12 9 3" fill="none" stroke="#47d890" stroke-width="5" stroke-linecap="round" stroke-linejoin="round"/><path d="M10 55h44" stroke="#52677b" stroke-width="3" stroke-linecap="round"/></svg>''',
        "keyword": '''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><rect width="64" height="64" rx="14" fill="#0a1119"/><path d="M13 12h25l9 9v17H13z" fill="#172638" stroke="#60baff" stroke-width="3" stroke-linejoin="round"/><path d="M37 12v10h10M19 28h13m-13 6h9" stroke="#60baff" stroke-width="3" stroke-linecap="round"/><circle cx="39" cy="41" r="10" fill="#0a1119" stroke="#c4a7ff" stroke-width="4"/><path d="m46 48 8 8" stroke="#c4a7ff" stroke-width="5" stroke-linecap="round"/></svg>'''
    }
    name = request.args.get("app", "gapup")
    return Response(icons.get(name, icons["gapup"]), mimetype="image/svg+xml", headers={"Cache-Control": "public, max-age=86400"})


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

# Load account records only after auth and settings handlers are defined.
_db_sync_users()

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
