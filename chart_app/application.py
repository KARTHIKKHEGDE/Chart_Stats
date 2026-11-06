#!/usr/bin/env python3
"""
Historical Gap-Up Fade Analyzer (Flask app with modular feature handlers)

Setup
    pip install flask pandas numpy requests yfinance
    export POLYGON_API_KEY=your_key          (Windows: set POLYGON_API_KEY=your_key)
    python gapup_fade.py                     -> http://127.0.0.1:8080
    python gapup_fade.py 9000                -> pick any port (or set PORT=9000)

Optional env
    POLYGON_RPM=5        Per-key request-rate ceiling (50 keys = 250 requests/minute).
    SEC_USER_AGENT="Your Name your@email.com"   (real SEC contact identity; required for SEC data)
    GAP_MIN=0.30  MAX_EVENTS=12  SIM_K=20  MIN_PRICE=0.5  MIN_DAY_VOL=100000  MIN_PM_VOL=0
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
import os, re, json, time, math, random, threading, uuid, warnings, hashlib, secrets, datetime as dt, logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import contextlib
from email.utils import parsedate_to_datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
import yfinance as yf
try:
    import orjson as _orjson
except ImportError:
    _orjson = None
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
POLY_RPM = min(5.0, max(0.1, float(os.getenv("POLYGON_RPM", "5"))))
POLYGON_KEY_POOL_LIMIT = 50
LOCAL_MAX = os.getenv("LOCAL_MAX", "1").strip() == "1"
CPU_COUNT = os.cpu_count() or 4
FETCH_WORKERS_CAP = int(os.getenv("FETCH_WORKERS_CAP", "100" if LOCAL_MAX else "50"))
ANALYZE_WORKERS = int(os.getenv("ANALYZE_WORKERS", str(max(4, CPU_COUNT * 2) if LOCAL_MAX else 8)))
STAGE1_WAIT = float(os.getenv("OPTIONAL_WAIT", "1.25"))
REPORT_WORKERS = int(os.getenv("REPORT_WORKERS", str(max(4, CPU_COUNT // 2) if LOCAL_MAX else 2)))
SPIKE_WORKERS = int(os.getenv("SPIKE_WORKERS", "50"))
WARMUP_WORKERS = int(os.getenv("WARMUP_WORKERS", str(max(2, CPU_COUNT // 2) if LOCAL_MAX else 1)))
SERVER_THREADS = int(os.getenv("SERVER_THREADS", "50"))

# ----------------------------------------------------------------------------- Polygon client
_key_pool: list = []
_key_cooldown: dict = {}  # expiry time after an actual HTTP 429
_key_next_request: dict = {}  # per-key pacing for the configured Polygon requests/minute
_key_lock = threading.Lock()
_key_cursor = 0
_POLY_SESSION = requests.Session()
_POLY_SESSION.mount("https://", requests.adapters.HTTPAdapter(pool_connections=50, pool_maxsize=50))


def _init_key_pool():
    """Load configured Polygon credentials and preserve their round-robin order."""
    global _key_pool, _key_cooldown
    raw = os.getenv("POLYGON_API_KEYS", "") or os.getenv("POLYGON_API_KEY", "")
    keys = [key.strip() for key in re.split(r"[,;\s]+", raw) if key.strip()]
    # Also accept environments configured as POLYGON_API_KEY_1 … _50.
    numbered = []
    for name, value in os.environ.items():
        match = re.fullmatch(r"POLYGON_API_KEYS?_(\d+)", name)
        if match and value.strip():
            numbered.append((int(match.group(1)), value.strip()))
    keys.extend(value for _, value in sorted(numbered))
    unique_keys = list(dict.fromkeys(keys))
    _key_pool = unique_keys[:POLYGON_KEY_POOL_LIMIT]
    _key_cooldown = {key: expiry for key, expiry in _key_cooldown.items() if key in _key_pool}
    if _key_pool:
        aggregate_rpm = len(_key_pool) * POLY_RPM
        truncated = f", capped from {len(unique_keys)}" if len(unique_keys) > len(_key_pool) else ""
        print(f"[polygon] configured key pool: {len(_key_pool)} unique key(s){truncated}, "
              f"{POLY_RPM:g} RPM/key, max {aggregate_rpm:g} requests/minute", flush=True)
    else:
        print("[polygon] no API keys configured", flush=True)

_init_key_pool()   # bootstrap pool at module load (env already loaded above)
print(f"[startup] LOCAL_MAX={int(LOCAL_MAX)} CPU={CPU_COUNT} report_workers={REPORT_WORKERS} "
      f"fetch_cap={FETCH_WORKERS_CAP} spike_workers={SPIKE_WORKERS} "
      f"analyze_workers={ANALYZE_WORKERS} server_threads={SERVER_THREADS} "
      f"stage1_wait={STAGE1_WAIT:g}s",
      flush=True)


SEC_UA = os.getenv("SEC_USER_AGENT", "").strip()
GAP_MIN = float(os.getenv("GAP_MIN", "0.30"))
LOOKBACK_DAYS = 365
HISTORY_DAYS = 740            # retain a full two years of daily candles plus split/calendar buffer
MAX_EVENTS = int(os.getenv("MAX_EVENTS", "50" if LOCAL_MAX else "12"))
print(f"[startup] warmup_workers={WARMUP_WORKERS} max_events={MAX_EVENTS}", flush=True)
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

# ----------------------------------------------------------------------------- cache / http
def cget(key, ttl=None):
    p = CACHE / f"{key}.json"
    if not p.exists():
        return None
    if ttl and time.time() - p.stat().st_mtime > ttl:
        return None
    try:
        payload = p.read_bytes()
        return _orjson.loads(payload) if _orjson else json.loads(payload)
    except Exception:
        return None


def cput(key, val):
    try:
        p = CACHE / f"{key}.json"
        tmp = p.with_name(f"{p.name}.{uuid.uuid4().hex}.tmp")
        payload = _orjson.dumps(val) if _orjson else json.dumps(val).encode("utf-8")
        tmp.write_bytes(payload)
        os.replace(tmp, p)
    except Exception:
        pass


@contextlib.contextmanager
def _timed_stage(job, label):
    started = time.perf_counter()
    try:
        yield
    finally:
        elapsed = round(time.perf_counter() - started, 3)
        timings = job.setdefault("timings", {})
        timings[label] = round(timings.get(label, 0.0) + elapsed, 3)
        app.logger.info("[timing] %s %.3fs", label, elapsed)


def _timed_call(job, label, function):
    started = time.perf_counter()
    app.logger.info("[timing] %s started", label)
    try:
        return function()
    finally:
        elapsed = round(time.perf_counter() - started, 3)
        job.setdefault("timings", {})[label] = elapsed
        app.logger.info("[timing] %s %.3fs", label, elapsed)


def _event_fingerprint(daily, index, ctx):
    """Fingerprint only the daily context used by one event, not today's full history."""
    event_day = daily[index]["date"]
    rows = daily[max(0, index - 252):min(len(daily), index + 32)]
    sec = [(row.get("filed"), row.get("val")) for row in (ctx.get("sec") or [])
           if row.get("filed") and row["filed"] <= event_day]
    payload = {
        "rows": [(r.get("date"), r.get("o"), r.get("h"), r.get("l"), r.get("c"), r.get("v")) for r in rows],
        "splits": [row for row in (ctx.get("splits") or []) if row.get("date") and row["date"] <= event_day],
        "sec": sec,
        "ratio": round(float(ctx["ratio"]), 3) if ctx.get("ratio") else None,
    }
    return hashlib.sha1(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
                        .encode("utf-8")).hexdigest()[:16]


def _next_polygon_key():
    """Choose a key that is both out of cooldown and within its per-key RPM."""
    global _key_cursor
    # At the default 5 RPM, 50 credentials have a 250 requests/minute ceiling.
    min_interval = 60.0 / max(0.1, POLY_RPM)
    while True:
        with _key_lock:
            if not _key_pool:
                raise RuntimeError("POLYGON_API_KEY is not set. Set it and restart the app.")
            now = time.time()
            count = len(_key_pool)
            for offset in range(count):
                index = (_key_cursor + offset) % count
                key = _key_pool[index]
                ready_at = max(_key_cooldown.get(key, 0), _key_next_request.get(key, 0))
                if ready_at <= now:
                    _key_cursor = (index + 1) % count
                    _key_next_request[key] = now + min_interval
                    return key
            wait = max(0.05, min(max(_key_cooldown.get(key, 0), _key_next_request.get(key, 0)) for key in _key_pool) - now)
        time.sleep(wait)


def _poly_request_one(path: str, params: dict) -> dict:
    """Call Polygon through one pooled session, rotating configured keys on 429."""
    global _key_cursor
    if not _key_pool:
        _load_env_file()
        _init_key_pool()
    if not _key_pool:
        raise RuntimeError("POLYGON_API_KEY is not set. Set it and restart the app.")

    payload = dict(params)
    # Try configured credentials in round-robin order. Each concurrent fetch
    # worker takes a different key; network failures stop quickly, while 401s
    # and 429s can move on to the rest of the pool.
    max_attempts = len(_key_pool)
    network_failures = 0
    for attempt in range(max_attempts):
        key = _next_polygon_key()
        payload["apiKey"] = key
        try:
            response = _POLY_SESSION.get("https://api.polygon.io" + path, params=payload, timeout=(3, 15))
        except requests.RequestException:
            network_failures += 1
            if network_failures >= 3:
                break
            time.sleep(0.1 * network_failures)
            continue
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After", "30")
            try:
                cooldown = max(1.0, float(retry_after))
            except (TypeError, ValueError):
                try:
                    cooldown = max(1.0, parsedate_to_datetime(retry_after).timestamp() - time.time())
                except Exception:
                    cooldown = 30.0
            with _key_lock:
                _key_cooldown[key] = time.time() + cooldown
            app.logger.warning("[polygon] one configured key was rate-limited; cooling it down for %.0fs (%d key(s) remain configured)", cooldown, len(_key_pool))
            continue
        if response.status_code == 401:
            with _key_lock:
                if key in _key_pool:
                    _key_pool.remove(key)
                    _key_cursor = _key_cursor % max(1, len(_key_pool))
                _key_cooldown.pop(key, None)
                remaining_keys = len(_key_pool)
            app.logger.error("[polygon] one configured key was rejected (401); %d key(s) remain in the active pool", remaining_keys)
            if not _key_pool:
                raise RuntimeError(f"All Polygon keys rejected (401): {response.text[:150]}")
            continue
        response.raise_for_status()
        return response.json()
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
    pbucket,
    poly_frame,
    recent_hod_times,
    recent_open_hod_stats,
    ticker_type,
    vwap_twap_frame,
)


# ----------------------------------------------------------------------------- live (Alpaca / Yahoo)


# ----------------------------------------------------------------------------- features


# ----------------------------------------------------------------------------- stats / grade


# ----------------------------------------------------------------------------- formatting


# ----------------------------------------------------------------------------- report


# ----------------------------------------------------------------------------- job runner
JOBS = {}
JOB_LOCK = threading.RLock()
ACTIVE_TICKER_JOBS = {}
REPORT_JOB_SLOTS = threading.BoundedSemaphore(REPORT_WORKERS)
REPORT_JOB_POOL = ThreadPoolExecutor(max_workers=REPORT_WORKERS, thread_name_prefix="ticker-report")
_ACTIVE_REPORT_LOCK = threading.Lock()
_ACTIVE_REPORTS = 0
_WARMUP_PAUSED = threading.Event()


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
    was_cached = False
    try:
        was_cached = cget(f"{t}_{day}_1m_polygon_v1") is not None
        df = minute_df(t, day)   # cached in .gapcache; analyze_event will hit cache
        return (i, day, df is not None and not df.empty, was_cached)
    except Exception:
        return (i, day, False, was_cached)


def run_job(job, t, refresh):
    try:
        # ------------------------------------------------------------------ STAGE 1
        # Fetch everything that doesn't depend on each other concurrently.
        job.update(stage="core", progress_pct=4,
                   msg="Connecting to live quote, daily history, and reference data in parallel…")

        _live_res   = [None]
        _daily_res  = [None]
        _splits_res = [None]
        _sec_res    = [None]
        _security_res = [None]
        _news_res   = [None]
        _err        = [None]
        hod_times_res=[None]
        hod_start_lock=threading.Lock()
        hod_times_thread=[None]
        def _start_hod_times(hod_daily):
            if not hod_daily:
                return
            with hod_start_lock:
                if hod_times_thread[0] is not None:
                    return
                def _get_recent_hod_times():
                    try:
                        hod_times_res[0]=_timed_call(job,"hod_times",lambda:recent_hod_times(t,hod_daily))
                    except Exception:
                        hod_times_res[0]={"date_from":None,"date_to":None,"rows":[]}
                hod_times_thread[0]=threading.Thread(target=_get_recent_hod_times,daemon=True,
                                                     name=f"hod-times-{t}")
                hod_times_thread[0].start()

        # When a previous daily snapshot is available, use it immediately for
        # a speculative candle fan-out while the fresh daily request is in flight.
        stale_daily=cget(f"{t}_daily_polygon_v1")
        speculative_thread=None
        if LOCAL_MAX and isinstance(stale_daily,list) and stale_daily:
            stale_splits=cget(f"{t}_splits") or []
            if not refresh:
                _start_hod_times(stale_daily)
            def _speculative_prefetch():
                try:
                    provisional=detect_events(stale_daily,stale_splits,dt.datetime.now(ET).date())
                    provisional=provisional[-MAX_EVENTS:]
                    if provisional:
                        workers=min(max(1,len(_key_pool)),len(provisional),FETCH_WORKERS_CAP)
                        app.logger.info("[parallel] speculative candles ticker=%s sessions=%d workers=%d keys=%d rpm_per_key=%g",
                                        t,len(provisional),workers,len(_key_pool),POLY_RPM)
                        with ThreadPoolExecutor(max_workers=workers,thread_name_prefix="spec-minute-fetch") as pool:
                            list(pool.map(_prefetch_minute,[(t,index,stale_daily) for index in provisional]))
                except Exception:
                    app.logger.exception("Speculative historical prefetch failed for %s",t)
            speculative_thread=threading.Thread(target=_speculative_prefetch,daemon=True,
                                                name=f"spec-prefetch-{t}")
            speculative_thread.start()
        core_lock=threading.Lock();core_done=set()
        def core_finished(label):
            with core_lock:
                core_done.add(label);finished=len(core_done)
                job.update(stage='core',progress_pct=4+finished*2,
                           msg=f'Live data loading · {finished}/3 core sources ready ({label})…')

        def _get_live():
            try:
                _live_res[0] = _timed_call(job,"live",lambda:live_snapshot(t,fast=True,include_profile=False))
            except Exception as ex:
                _err[0] = ex
            finally:core_finished('Yahoo live candles')

        def _get_daily():
            try:
                _daily_res[0] = _timed_call(job,"daily",lambda:load_daily(t, refresh))
                _start_hod_times(_daily_res[0])
            except Exception as ex:
                _err[0] = ex
            finally:core_finished('daily price history')

        def _get_splits():
            try:
                _splits_res[0] = _timed_call(job,"splits",lambda:load_splits(t))
            except Exception:
                _splits_res[0] = []
            finally:core_finished('split history')

        def _get_sec():
            try:
                _sec_res[0] = _timed_call(job,"sec_shares",lambda:load_shares_out(t))
            except Exception:
                _sec_res[0] = []

        def _get_security_stats():
            try:
                _security_res[0] = _timed_call(job,"security_stats",lambda:latest_security_stats(t, force=refresh))
            except Exception as ex:
                _security_res[0] = {
                    "errors": [f"Polygon security metadata: {type(ex).__name__}: {ex}"]
                }

        def _get_news():
            try:
                _news_res[0] = _timed_call(job,"news",lambda:_get_scanner_news(t))
            except Exception:
                _news_res[0] = []

        stage1_threads = [
            threading.Thread(target=_get_live,      daemon=True),
            threading.Thread(target=_get_daily,     daemon=True),
            threading.Thread(target=_get_splits,    daemon=True),
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
        optional_deadline=time.monotonic()+STAGE1_WAIT
        for th in stage1_threads[3:]:
            th.join(max(0.0,optional_deadline-time.monotonic()))
        sec_history_pending=stage1_threads[3].is_alive()
        security_stats_pending=stage1_threads[4].is_alive()
        news_pending=stage1_threads[5].is_alive()

        if _err[0]:
            raise _err[0]

        if not _daily_res[0]:
            raise RuntimeError(f"No daily price history returned from Polygon for {t}")

        live   = _live_res[0]
        splits = _splits_res[0]
        sec    = _sec_res[0] or []
        security_stats = _security_res[0] or {}
        typ    = security_stats.get("type") or (None if security_stats_pending else ticker_type(t))
        polygon_float = security_stats.get("float_shares")
        polygon_shares = security_stats.get("shares_outstanding")
        if polygon_float:
            live["float"] = polygon_float
            live["float_source"] = (f"{security_stats.get('float_source')} · {security_stats.get('float_asof') or 'date unavailable'}"
                                    if security_stats.get("float_source") else
                                    f"Polygon · {security_stats.get('float_asof') or 'date unavailable'}")
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
        if security_stats.get("market_cap") is not None:
            live["mcap"] = security_stats["market_cap"]
            live["market_cap_source"] = security_stats.get("market_cap_source") or "ShareStructure.io"
        elif security_stats.get("provider") == "ShareStructure.io":
            # ShareStructure's documented partner response does not include a
            # market-cap field. Do not silently substitute another vendor.
            live["mcap"] = None
            live["market_cap_source"] = "Not supplied by ShareStructure.io"
        elif cap_shares and live.get("cur"):
            live["mcap"] = cap_shares * live["cur"]
            live["market_cap_source"] = (
                "Latest price × Polygon shares" if polygon_shares
                else "Latest price × Yahoo shares"
            )
        elif live.get("mcap"):
            live["market_cap_source"] = "Yahoo Finance fallback · date unavailable"
        else:
            live["market_cap_source"] = "Unavailable"
        daily  = [r for r in _daily_res[0] if r["date"] < str(live["date"])]
        if daily:
            # Reuse the report's Polygon daily response instead of making a
            # second annual Yahoo daily-history request on the critical path.
            live['hist']=daily
            live['pc']=float(daily[-1].get('c') or live.get('pc') or 0)
            live['avg20']=float(np.mean([row.get('v',0) for row in daily[-20:]]))

        # This report section needs one extra 15-minute request. Start it while
        # the historical event candles are loading so it doesn't add a serial
        # network wait at the end of the report.
        _start_hod_times(daily)

        fl_now = live["float"] or live["so"]
        ratio  = None
        if live["float"] and live["so"]:
            ratio = min(1.0, max(0.02, live["float"] / live["so"]))
        ctx = {"sec": sec, "splits": splits, "float": fl_now, "ratio": ratio}

        job.update(stage="finding_sessions", progress_pct=12,
                   msg="Core market data is in. Finding historical gap-up and premarket-spike sessions…")
        idxs = detect_events(daily, splits, live["date"])

        # Persist computed event summaries separately from raw candles. A repeat
        # analysis can reuse the full metrics/chart payload without refetching or
        # recomputing each historical session. Changes in prices, SEC shares,
        # splits, or current float context invalidate the cache automatically.
        results={}
        pending_idxs=[]
        for i in idxs:
            event_key=f"{t}_{daily[i]['date']}_gap_event_v2"
            event_fingerprint=_event_fingerprint(daily, i, ctx)
            cached_event=cget(event_key)
            if isinstance(cached_event,dict) and cached_event.get("fingerprint")==event_fingerprint and "event" in cached_event:
                cached_result=cached_event["event"]
                if isinstance(cached_result,dict):
                    changed=False
                    for path_key in ("path","path_vwap","path_vwap_dev","path_twap","path_twap_dev",
                                     "path_vwap_twap_spread","path_active_twap"):
                        if path_key in cached_result:
                            cached_result.pop(path_key)
                            changed=True
                    if changed:
                        cput(event_key,{"fingerprint":event_fingerprint,"event":cached_result})
                    results[i]=cached_result
                elif cached_result is None:
                    results[i]=None
                else:
                    pending_idxs.append(i)
            else:
                pending_idxs.append(i)
        event_cache_hits=len(results)

        # ------------------------------------------------------------------ STAGE 2 + 3
        # Fetch and analyze each event as soon as its minute bars are available.
        # This overlaps CPU work with provider/network latency instead of waiting
        # for every historical session to finish downloading first.
        n_keys=max(1,len(_key_pool))
        n_events=len(idxs)

        def _analyze(i):
            event_fingerprint=_event_fingerprint(daily, i, ctx)
            try:
                event=_timed_call(job,f"analyze:{daily[i]['date']}",lambda:clean(analyze_event(t,i,daily,ctx)))
            except Exception:
                app.logger.exception("Historical analysis failed for %s %s",t,daily[i]["date"])
                return i,None
            cput(f"{t}_{daily[i]['date']}_gap_event_v2",
                 {"fingerprint":event_fingerprint,"event":event})
            return i,event

        if pending_idxs:
            fetch_args=[(t,i,daily) for i in pending_idxs]
            # Keep provider concurrency bounded: spawning one worker per
            # event/key can saturate connection pools and trigger rate limits.
            # A single report benefits from more parallel historical fetches;
            # concurrency remains bounded by available Polygon keys and events.
            fetch_workers=min(n_keys,len(pending_idxs),FETCH_WORKERS_CAP)
            analyze_workers=min(ANALYZE_WORKERS,len(pending_idxs))
            app.logger.info("[parallel] historical candles ticker=%s tasks=%d workers=%d keys=%d rpm_per_key=%g",
                            t,len(pending_idxs),fetch_workers,len(_key_pool),POLY_RPM)
            cached_sessions=sum(cget(f"{t}_{daily[i]['date']}_1m_polygon_v1") is not None for i in pending_idxs)
            uncached_sessions=max(0,len(pending_idxs)-cached_sessions)
            effective_rpm=max(0.1,float(POLY_RPM))*max(1,fetch_workers)
            initial_eta=int(uncached_sessions*60/effective_rpm + max(5,n_events*.2))
            job.update(stage="loading_history",progress_pct=16+int(56*event_cache_hits/max(1,n_events)),progress_done=event_cache_hits,progress_total=n_events,
                       cached_done=event_cache_hits,uncached_total=uncached_sessions,uncached_done=0,
                       fetch_workers=fetch_workers,estimate_seconds=initial_eta,eta_seconds=initial_eta,
                       msg=f"Found {n_events} historical sessions · reusing {event_cache_hits} complete analyses · fetching with {fetch_workers} worker(s) across {n_keys} configured Polygon key(s)…")
            with ThreadPoolExecutor(max_workers=fetch_workers,thread_name_prefix="gap-minute-fetch") as fetch_pool:
                with ThreadPoolExecutor(max_workers=analyze_workers,thread_name_prefix="gap-event-analysis") as analyze_pool:
                    fetch_futures={fetch_pool.submit(_timed_call,job,f"minute_fetch:{daily[arg[1]]['date']}",lambda arg=arg:_prefetch_minute(arg)):arg for arg in fetch_args}
                    analyze_futures={}
                    completed=event_cache_hits;cached_done=event_cache_hits
                    for future in as_completed(fetch_futures):
                        idx_i,day,ok,was_cached=future.result();completed+=1;cached_done+=int(was_cached)
                        analyze_futures[analyze_pool.submit(_analyze,idx_i)]=idx_i
                        uncached_done=max(0,completed-cached_done)
                        eta=int(max(0,uncached_sessions-uncached_done)*60/effective_rpm + max(3,(n_events-completed)*.2))
                        job.update(progress_done=completed,cached_done=cached_done,uncached_done=uncached_done,
                                   progress_pct=16+int(56*completed/max(1,n_events)),eta_seconds=eta,
                                   msg=f"Historical minute candles · {completed}/{n_events} sessions fetched · {cached_done} from cache · {fetch_workers} workers / {n_keys} Polygon keys ({day}{' · cached' if was_cached else ' · no minute bars' if not ok else ''})")
                    # Analyze as soon as each session's bars are fetched. The
                    # old code waited for every analysis at the end, hiding
                    # progress and delaying report readiness behind stragglers.
                    analyzed=event_cache_hits
                    analyze_uncached_done=0
                    analyze_durations=[]
                    job.update(stage="analyzing",progress_pct=74,
                               msg=f"Candles loaded · calculating patterns for {n_events-event_cache_hits} uncached sessions…")
                    for future in as_completed(analyze_futures):
                        idx_i,event=future.result()
                        results[idx_i]=event
                        analyzed+=1
                        analyze_uncached_done+=1
                        analyze_durations.append(float(job.get("timings",{}).get(f"analyze:{daily[idx_i]['date']}",0.1)))
                        analyze_remaining=max(0,n_events-event_cache_hits-analyze_uncached_done)
                        analyze_eta=int(analyze_remaining*sum(analyze_durations)/max(1,len(analyze_durations)))
                        job.update(analysis_done=analyzed,analysis_total=n_events,
                                   progress_pct=74+int(22*analyzed/max(1,n_events)),
                                   eta_seconds=analyze_eta,
                                   msg=f"Analyzing historical patterns · {analyzed}/{n_events} sessions complete…")
        else:
            job.update(stage="building_report",progress_pct=92 if event_cache_hits else 78,
                       progress_done=event_cache_hits,progress_total=n_events,
                       eta_seconds=2,msg=(f"Reused all {event_cache_hits} cached historical analyses; assembling the live report…"
                                          if event_cache_hits else "No matching historical spike sessions; assembling the live report…"))

        events, skipped = [], 0
        for i in idxs:
            e = results.get(i)
            if e:
                events.append(e)
            else:
                skipped += 1

        # ------------------------------------------------------------------ report
        job.update(stage="building_report",progress_pct=97,eta_seconds=3,
                   msg="Historical analysis is complete · building the report and chart data…")
        lm    = _timed_call(job,"live_metrics",lambda:metrics(live["df"], live["pc"], False))
        lctx  = ctx_feats(live["hist"], len(live["hist"]))
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
            return d

        # Premarket-only spike sessions inform the dedicated PM fade grade, but
        # should not contaminate the regular-session gap-up analog set.
        S = sorted((e for e in events if e.get("gap", 0) >= GAP_MIN), key=dist)[:SIM_K]
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

        if hod_times_thread[0]:
            hod_times_thread[0].join(5)
        hod_times=hod_times_res[0] or {"date_from":None,"date_to":None,"rows":[]}
        report = _timed_call(job,"build_report",lambda:clean(build_report(t, live, lm, events, S, notes, daily, hod_times)))
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
        report["news_status"] = ("Latest news is still loading; refresh the report to update this section." if news_pending else
                                  _scanner_details.get(t, {}).get("news_status") or
                                  ("Latest news found on Yahoo Finance" if recent_news else
                                   "Yahoo Finance returned no recent news"))
        if recent_news:
            report["text"] += "\n\nLatest News · Yahoo Finance"
            for item in recent_news:
                report["text"] += f"\n- [{item['kind']}] {item['title']} ({item['publisher']}) {item['url']}"
        else:
            report["text"] += f"\n\nLatest News · Yahoo Finance\n{report['news_status']}"
        job["result"] = report
        job["status"] = "done"
        job.update(stage="complete",progress_pct=100,eta_seconds=0,msg="Report ready.",finished_at=time.time())
    except Exception as ex:
        app.logger.exception("Ticker analysis failed for %s", t)
        job["status"] = "error"
        job["stage"] = "error"
        job["error"]  = f"{type(ex).__name__}: {ex}"
        job["finished_at"] = time.time()


def _run_report_job_limited(job, ticker, refresh, user=None):
    global _ACTIVE_REPORTS
    with _ACTIVE_REPORT_LOCK:
        _ACTIVE_REPORTS += 1
        _WARMUP_PAUSED.set()
    try:
        run_job(job, ticker, refresh)
    finally:
        REPORT_JOB_SLOTS.release()
        with JOB_LOCK:
            active_key=(str(user or "").lower(),ticker,bool(refresh))
            if ACTIVE_TICKER_JOBS.get(active_key)==job.get("job_id"):
                ACTIVE_TICKER_JOBS.pop(active_key,None)
        with _ACTIVE_REPORT_LOCK:
            _ACTIVE_REPORTS=max(0,_ACTIVE_REPORTS-1)
            warmup_allowed=_ACTIVE_REPORTS==0
            if warmup_allowed:
                _WARMUP_PAUSED.clear()
        if warmup_allowed and LOCAL_MAX:
            _schedule_local_warmup()


# ----------------------------------------------------------------------------- web
HTML = (Path(__file__).resolve().parent.parent / "templates" / "gapup.html").read_text(encoding="utf-8")
KEYWORD_HTML = (Path(__file__).resolve().parent.parent / "templates" / "keyword_search.html").read_text(encoding="utf-8")

app = Flask(__name__, static_folder="../static")
app.logger.setLevel(logging.INFO)


@app.after_request
def _inject_local_reload_script(response):
    if os.getenv("LOCAL_AUTO_RELOAD") == "1" and response.mimetype == "text/html":
        html = response.get_data(as_text=True)
        if "</body>" in html and "/static/local_reload.js" not in html:
            response.set_data(html.replace("</body>", '<script src="/static/local_reload.js"></script></body>'))
    return response


@app.route("/api/local-reload-version")
def local_reload_version():
    if os.getenv("LOCAL_AUTO_RELOAD") != "1":
        return jsonify({"error": "Not found"}), 404
    marker = Path(__file__).resolve().parent.parent / ".local_reload_version"
    try:
        version = marker.read_text(encoding="utf-8").strip()
    except OSError:
        version = "0"
    return jsonify({"version": version}), 200, {"Cache-Control": "no-store"}


if not SEC_UA:
    app.logger.warning("SEC_USER_AGENT is unset; SEC-based dilution and filing data will be unavailable until a contact identity is configured.")


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


@app.route("/anychart")
def anychart_page():
    """Open the general historical chart workspace as its own app page."""
    page = HTML.replace("<title>Gap-Up Fade Analyzer</title>",
                        "<title>Anychart · Historical Charts</title>", 1)
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
        from chart_app.auth import record_activity_async
        record_activity_async(username, "ticker_search", "/runner", "Get stats",
                              {"ticker": t})
    except Exception:
        app.logger.debug("Could not queue runner activity event", exc_info=True)
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

_premarket_spike_pool = ThreadPoolExecutor(max_workers=SPIKE_WORKERS, thread_name_prefix="premarket-spikes")
_PREMARKET_SPIKE_SLOTS = threading.BoundedSemaphore(SPIKE_WORKERS if LOCAL_MAX else 2)


def _run_local_watchlist_warmup():
    if not LOCAL_MAX:
        return
    try:
        from chart_app.scanner import _scanner_cache, _scanner_tracked_symbols
        configured=[item.strip().upper() for item in re.split(r"[,;\s]+",os.getenv("LOCAL_WARMUP_TICKERS", "")) if item.strip()]
        scanned=[str(row.get("ticker") or "").upper() for row in (_scanner_cache.get("rows") or [])[:8]]
        tracked=sorted(_scanner_tracked_symbols)[:8]
        tickers=list(dict.fromkeys(configured+scanned+tracked))[:12]
        for ticker in tickers:
            if _WARMUP_PAUSED.is_set():
                return
            daily=load_daily(ticker)
            if not daily:
                continue
            splits=load_splits(ticker)
            indices=detect_events(daily,splits,dt.datetime.now(ET).date())[-5:]
            if not indices:
                continue
            workers=min(max(1,len(_key_pool)),len(indices),WARMUP_WORKERS)
            with ThreadPoolExecutor(max_workers=workers,thread_name_prefix="local-cache-warm") as pool:
                futures=[pool.submit(_prefetch_minute,(ticker,index,daily)) for index in indices]
                for future in as_completed(futures):
                    future.result()
                    if _WARMUP_PAUSED.is_set():
                        return
    except Exception:
        app.logger.exception("Local watchlist warm-up failed")


_LOCAL_WARMUP_POOL=ThreadPoolExecutor(max_workers=1,thread_name_prefix="local-watchlist-warm")


def _schedule_local_warmup():
    if LOCAL_MAX and not _WARMUP_PAUSED.is_set():
        try:
            _LOCAL_WARMUP_POOL.submit(_run_local_watchlist_warmup)
        except RuntimeError:
            pass


def _local_startup_warmup():
    if not LOCAL_MAX:
        return
    if _key_pool:
        def warm_connection(_):
            try:
                response=_POLY_SESSION.get("https://api.polygon.io/v1/marketstatus/now",timeout=(2,4))
                response.content
                response.close()
            except Exception:
                pass
        with ThreadPoolExecutor(max_workers=min(50,len(_key_pool)),thread_name_prefix="polygon-tls-warm") as pool:
            list(pool.map(warm_connection,range(min(50,len(_key_pool)))))
    if SEC_UA:
        try:
            from chart_app.market_data import _sec_ticker_map
            _sec_ticker_map()
        except Exception:
            app.logger.info("SEC ticker map startup warm-up unavailable",exc_info=True)
    _schedule_local_warmup()


def _compute_premarket_spikes(ticker):
    cache_key = f"{ticker}_premarket_spikes_v7"
    daily = load_daily(ticker)
    if not daily:
        raise RuntimeError("No daily history available from Polygon.")
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
        workers = min(SPIKE_WORKERS, max(1,len(_key_pool)), max(1,len(candidates)))
        app.logger.info("[parallel] premarket spike candles ticker=%s tasks=%d workers=%d keys=%d rpm_per_key=%g",
                        ticker,len(candidates),workers,len(_key_pool),POLY_RPM)
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(_prefetch_minute, [(ticker, index, daily) for index in candidates]))
    found = []
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
            pm_high_minute = int(high_index.hour * 60 + high_index.minute)
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

    result={"days":found}
    cput(cache_key, result)
    return result


def _premarket_spike_job_key(ticker):
    return f"{ticker}_premarket_spikes_job_v1"


def _premarket_spike_lock_path(ticker):
    return CACHE / f"{ticker}_premarket_spikes_job_v1.lock"


def _run_premarket_spike_job(ticker):
    try:
        _compute_premarket_spikes(ticker)
        cput(_premarket_spike_job_key(ticker), {"status": "complete", "finished_at": time.time()})
    except Exception as exc:
        app.logger.exception("Premarket spike history failed for %s", ticker)
        cput(_premarket_spike_job_key(ticker), {"status": "error",
                                                "error": f"Premarket history scan failed: {exc}",
                                                "finished_at": time.time()})
    finally:
        try:
            _premarket_spike_lock_path(ticker).unlink(missing_ok=True)
        except Exception:
            pass
        _PREMARKET_SPIKE_SLOTS.release()


@app.route("/api/premarket-spikes")
def premarket_spikes_api():
    username = (request.args.get("user") or "").strip().lower()
    token = (request.args.get("token") or "").strip()
    ticker = (request.args.get("ticker") or "").strip().upper()
    if not username or not _db_validate_session(username, token):
        return jsonify({"error": "Session expired. Please sign in again."}), 401
    if not re.fullmatch(r"[A-Z0-9.-]{1,12}", ticker):
        return jsonify({"error": "Invalid ticker."}), 400
    cache_key = f"{ticker}_premarket_spikes_v7"
    job_key = _premarket_spike_job_key(ticker)
    job_state_path = CACHE / f"{job_key}.json"
    retry = request.args.get("retry") == "1"
    if retry:
        try:
            job_state = cget(job_key, 1800)
            if isinstance(job_state, dict) and job_state.get("status") == "error":
                job_state_path.unlink(missing_ok=True)
        except Exception:
            pass
    cached = cget(cache_key, 6 * 3600)
    if isinstance(cached, dict) and "days" in cached:
        return jsonify({**cached, "cached": True})

    job_state = cget(job_key, 1800)
    if isinstance(job_state, dict) and job_state.get("status") == "error":
        return jsonify({"error": job_state.get("error") or "Premarket history scan failed."}), 502
    if isinstance(job_state, dict) and job_state.get("status") == "complete":
        # The result cache may have been evicted independently; queue a rebuild.
        try:
            job_state_path.unlink(missing_ok=True)
        except Exception:
            pass
    lock_path = _premarket_spike_lock_path(ticker)
    try:
        lock_fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        try:
            if time.time() - lock_path.stat().st_mtime > 1800:
                lock_path.unlink(missing_ok=True)
                lock_fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            else:
                return jsonify({"status": "loading", "message": "Loading premarket history from Polygon."}), 202
        except (FileExistsError, FileNotFoundError):
            return jsonify({"status": "loading", "message": "Loading premarket history from Polygon."}), 202
        except OSError as exc:
            return jsonify({"error": f"Could not coordinate premarket history loading: {exc}"}), 503
    except OSError as exc:
        return jsonify({"error": f"Could not start premarket history loading: {exc}"}), 503
    if not _PREMARKET_SPIKE_SLOTS.acquire(blocking=False):
        try:lock_path.unlink(missing_ok=True)
        except Exception:pass
        return jsonify({"error":"Premarket history workers are busy. Retry shortly."}),503
    try:
        with os.fdopen(lock_fd, "w", encoding="utf-8") as lock_file:
            lock_file.write(str(time.time()))
        cput(job_key, {"status": "running", "started_at": time.time()})
        _premarket_spike_pool.submit(_run_premarket_spike_job, ticker)
    except Exception as exc:
        _PREMARKET_SPIKE_SLOTS.release()
        try:
            lock_path.unlink(missing_ok=True)
        except Exception:
            pass
        return jsonify({"error": f"Could not queue premarket history scan: {exc}"}), 503
    return jsonify({"status": "loading", "message": "Loading premarket history from Polygon."}), 202

# Load account records only after auth and settings handlers are defined.
_db_sync_users()

# Register AI summary routes from charts module
try:
    from chart_app.charts import register_news_ai_routes as _reg_news_ai
    _reg_news_ai(app, _db_validate_session)
except Exception as _e:
    print(f'[news-ai routes] Could not register: {_e}')

if LOCAL_MAX:
    threading.Thread(target=_local_startup_warmup,daemon=True,name="local-startup-warmup").start()

def main():
    _load_env_file()
    if not _key_pool:
        print("WARNING: POLYGON_API_KEY not set — historical data will fail until you set it in .env or environment.")
    else:
        print(f"Polygon key pool loaded: {len(_key_pool)} key(s).")
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else int(os.getenv("PORT", "8000"))
    url=f"http://127.0.0.1:{port}"
    print(f"Open {url}")
    if LOCAL_MAX:
        if os.getenv("LOCAL_NO_BROWSER") != "1":
            import webbrowser
            threading.Timer(1.2,lambda:webbrowser.open(url)).start()
        from waitress import serve
        serve(app,host="127.0.0.1",port=port,threads=SERVER_THREADS)
    else:
        app.run(host="127.0.0.1", port=port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
