#!/usr/bin/env python3
"""
Historical Gap-Up Fade Analyzer  (single file, Flask)

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
    env_file = Path(__file__).resolve().parent / ".env"
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
ROOT_DIR = Path(__file__).resolve().parent
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


def load_daily(t, force=False):
    key = f"{t}_daily2"
    d = None if force else cget(key, 6 * 3600)
    if d is not None and any("vw" not in row for row in d):
        d = None
    if d is None:
        end = dt.date.today() + dt.timedelta(days=1)
        start = end - dt.timedelta(days=HISTORY_DAYS)
        j = poly_get(f"/v2/aggs/ticker/{t}/range/1/day/{start}/{end}",
                     {"adjusted": "false", "sort": "asc", "limit": 50000})
        d = [{"date": str(pd.to_datetime(r["t"], unit="ms", utc=True).tz_convert(ET).date()),
              "o": r["o"], "h": r["h"], "l": r["l"], "c": r["c"], "v": round(float(r["v"])), "vw": r.get("vw")}
             for r in j.get("results", [])]
        cput(key, d)
    return d


def recent_open_hod_stats(daily):
    """Summarize daily open-to-high moves for the last 30/90 sessions."""
    rows = daily[-252:]
    if not rows:
        return {"rows": [], "volume_multiple": 2.0}
    moves = []
    for i, row in enumerate(rows):
        prev_close = rows[i - 1]["c"] if i else None
        o, h = row.get("o"), row.get("h")
        moves.append({**row, "move": (h / o - 1) if o and h and o > 0 else None,
                      "gap": (o / prev_close - 1) if prev_close and o else None,
                      "avg_vol20": (float(np.mean([x["v"] for x in rows[max(0, i-20):i]]))
                                    if i and rows[max(0, i-20):i] else None)})
    def summarize(group):
        vals_ = [x["move"] for x in group if x["move"] is not None]
        return {"n": len(vals_), "median": med(vals_), "avg": float(np.mean(vals_)) if vals_ else None}
    rows30 = moves[-30:]
    volume_multiple = 10.0
    last30 = summarize([x for x in rows30 if x["move"] is not None])
    last90 = summarize([x for x in moves if x["move"] is not None])
    gapups = summarize([x for x in moves if x["gap"] is not None and x["gap"] >= .05 and x["move"] is not None])
    highvol = summarize([x for x in moves if x["move"] is not None and x["avg_vol20"] and x["v"] >= volume_multiple*x["avg_vol20"]])
    return {"date_from": rows30[0]["date"], "date_to": rows30[-1]["date"],
            "last30": last30, "last90": last90, "gapup5": gapups,
            "highvol": highvol, "volume_multiple": volume_multiple}


def recent_hod_times(t, daily, sessions=7):
    """Find approximate HOD time from 15-minute regular-session bars."""
    days = [r["date"] for r in daily[-sessions:]]
    if not days:
        return {"date_from": None, "date_to": None, "rows": []}
    start = (dt.date.fromisoformat(days[0]) - dt.timedelta(days=1)).isoformat()
    end = days[-1]
    key = f"{t}_{start}_{end}_15m_hod"
    data = cget(key, 6 * 3600)
    if data is None:
        try:
            j = poly_get(f"/v2/aggs/ticker/{t}/range/15/minute/{start}/{end}",
                         {"adjusted": "false", "sort": "asc", "limit": 50000})
            data = j.get("results") or []
            cput(key, data)
        except Exception:
            data = []
    best = {}
    for bar in data:
        stamp = dt.datetime.fromtimestamp(bar["t"] / 1000, dt.timezone.utc).astimezone(ET)
        if stamp.date().isoformat() not in days or not (dt.time(9, 30) <= stamp.time() < dt.time(16, 0)):
            continue
        day = stamp.date().isoformat()
        if day not in best or bar["h"] > best[day]["high"]:
            best[day] = {"date": day, "high": bar["h"], "time": stamp.astimezone(IST).strftime("%H:%M")}
    rows = [best[d] for d in days if d in best]
    return {"date_from": days[0], "date_to": days[-1], "rows": rows,
            "median_time": (sorted(x["time"] for x in rows)[len(rows)//2] if rows else None)}


def load_splits(t):
    d = cget(f"{t}_splits", 7 * 86400)
    if d is None:
        try:
            j = poly_get("/v3/reference/splits", {"ticker": t, "limit": 1000})
            d = [{"date": r["execution_date"], "from": r["split_from"], "to": r["split_to"]}
                 for r in j.get("results", [])]
            cput(f"{t}_splits", d)
        except Exception:
            d = []
    return d


def ticker_type(t):
    d = cget(f"{t}_type", 30 * 86400)
    if d is None:
        try:
            j = poly_get(f"/v3/reference/tickers/{t}")
            d = {"type": (j.get("results") or {}).get("type")}
        except Exception:
            d = {"type": None}
        cput(f"{t}_type", d)
    return d["type"]


def load_shares_out(t):
    """SEC EDGAR cover-page shares outstanding with filing dates (point-in-time)."""
    d = cget(f"{t}_sec", 3 * 86400)
    if d is not None:
        return d
    out = []
    try:
        h = {"User-Agent": SEC_UA}
        m = cget("sec_tickers", 7 * 86400)
        if m is None:
            raw = requests.get("https://www.sec.gov/files/company_tickers.json", headers=h, timeout=25).json()
            m = {v["ticker"].upper(): v["cik_str"] for v in raw.values()}
            cput("sec_tickers", m)
        cik = m.get(t.upper())
        if cik:
            j = requests.get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{int(cik):010d}.json",
                             headers=h, timeout=40).json()
            rows = j["facts"]["dei"]["EntityCommonStockSharesOutstanding"]["units"]["shares"]
            out = sorted([{"filed": r["filed"], "end": r["end"], "val": r["val"]}
                          for r in rows if r.get("val", 0) > 0], key=lambda r: r["filed"])
    except Exception:
        out = []
    cput(f"{t}_sec", out)
    return out


def poly_frame(rows):
    df = pd.DataFrame(rows)
    df.index = pd.DatetimeIndex(pd.to_datetime(df["t"], unit="ms", utc=True)).tz_convert(ET)
    df = df[["o", "h", "l", "c", "v"]].astype(float)
    df["v"] = df["v"].round()
    return df


def minute_df(t, day, force=False):
    key = f"{t}_{day}_1m"
    rows = None if force else cget(key)
    if rows is None:
        j = poly_get(f"/v2/aggs/ticker/{t}/range/1/minute/{day}/{day}",
                     {"adjusted": "false", "sort": "asc", "limit": 50000})
        rows = j.get("results") or []
        cput(key, rows)
    return poly_frame(rows) if rows else None


def _alpaca_live_df(t):
    key = os.getenv("ALPACA_API_KEY")
    sec = os.getenv("ALPACA_API_SECRET")
    if not (key and sec):
        return None
    try:
        now_et = dt.datetime.now(ET)
        today_start_et = dt.datetime(now_et.year, now_et.month, now_et.day, 4, 0, tzinfo=ET)
        start_utc = today_start_et.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        feed = os.getenv("ALPACA_FEED", "sip")
        url = f"https://data.alpaca.markets/v2/stocks/{t.upper()}/bars?timeframe=1Min&start={start_utc}&limit=10000&feed={feed}"
        headers = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": sec}
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            j = r.json()
            bars = j.get("bars") or []
            if bars:
                rows = []
                for b in bars:
                    dt_et = dt.datetime.fromisoformat(b["t"].replace("Z", "+00:00")).astimezone(ET)
                    rows.append({"Datetime": dt_et, "o": float(b["o"]), "h": float(b["h"]), "l": float(b["l"]), "c": float(b["c"]), "v": float(b["v"])})
                df = pd.DataFrame(rows).set_index("Datetime")
                if not df.empty and df["v"].sum() > 0:
                    # Use the SIP aggregate bars' full-session volume as-is.
                    # Scaling by an incomplete sample of up to 20,000 trades
                    # biased the day total downward (often by roughly half).
                    return df
    except Exception:
        pass
    return None


# ----------------------------------------------------------------------------- live (Alpaca / Yahoo)
def live_snapshot(t):
    tk = yf.Ticker(t.replace(".", "-"))
    df = tk.history(period="5d", interval="1m", prepost=True, auto_adjust=False)
    if df is None or df.empty:
        raise RuntimeError(f"No Yahoo intraday data for {t}")
    df.index = df.index.tz_convert(ET)
    df = df.rename(columns={"Open": "o", "High": "h", "Low": "l", "Close": "c", "Volume": "v"})
    df = df[["o", "h", "l", "c", "v"]].dropna().astype(float)
    day = df.index[-1].date()
    d = df[df.index.date == day]
    dly = tk.history(period="1y", interval="1d", auto_adjust=False).dropna()
    prev = dly[dly.index.date < day]
    if prev.empty:
        raise RuntimeError("Could not determine previous close from Yahoo")
    hist = [{"date": str(ix.date()), "o": float(r.Open), "h": float(r.High), "l": float(r.Low),
             "c": float(r.Close), "v": float(r.Volume)} for ix, r in prev.iterrows()]
    try:
        info = tk.info or {}
    except Exception:
        info = {}

    # Prefer 100% exact real-time tick volume from Alpaca SIP feed if configured in .env
    alp_df = _alpaca_live_df(t)
    if alp_df is not None and not alp_df.empty:
        d = alp_df
        session_vol = float(d["v"].sum())
    else:
        # Fallback to Yahoo Finance total volume
        session_vol = None
        fast = getattr(tk, "fast_info", None)
        for v_cand in [
            info.get("volume"),
            info.get("regularMarketVolume"),
            getattr(fast, "last_volume", None) if fast else None,
            getattr(fast, "day_volume", None) if fast else None,
            info.get("dayVolume")
        ]:
            if v_cand is not None:
                try:
                    fv = float(v_cand)
                    if fv > 0:
                        session_vol = fv
                        break
                except Exception:
                    pass

        if not session_vol:
            try:
                today_dly = dly[dly.index.date == day]
                if not today_dly.empty and float(today_dly["Volume"].iloc[-1]) > 0:
                    session_vol = float(today_dly["Volume"].iloc[-1])
            except Exception:
                pass

        # If intraday 1-minute volume is empty/0 (common in Yahoo Finance premarket feed for microcaps),
        # but Yahoo reports the actual session volume, distribute the volume across the session candles
        # based on price volatility (h - l) so VWAP, candle volume, pm_vol, and pm_dv are populated!
        df_vol_sum = float(d["v"].sum()) if "v" in d.columns else 0.0
        if (df_vol_sum == 0.0 or (session_vol and df_vol_sum < 0.2 * session_vol)) and session_vol and session_vol > 0:
            n_bars = len(d)
            if n_bars > 0:
                ranges = (d["h"] - d["l"]).clip(lower=0.0001)
                rng_sum = ranges.sum()
                weights = (ranges / rng_sum) if rng_sum > 0 else (1.0 / n_bars)
                d = d.copy()
                d["v"] = (weights * session_vol).round()

    return {"date": day, "df": d, "pc": hist[-1]["c"], "hist": hist,
            "avg20": float(np.mean([r["v"] for r in hist[-20:]])), "cur": float(d["c"].iloc[-1]),
            "float": info.get("floatShares"), "so": info.get("sharesOutstanding"),
            "session_vol": session_vol,
            "sector": info.get("industry") or info.get("sector") or "—", "mcap": info.get("marketCap"),
            "latest_time_ist": df.index[-1].astimezone(IST).strftime("%H:%M IST")}


# ----------------------------------------------------------------------------- features
def pbucket(p):
    return "<$2" if p < 2 else "$2-5" if p < 5 else "$5+"


def hod_bucket(m):
    return "First 5m" if m < 575 else "<10:00" if m < 600 else "Midday" if m < 840 else "Late"


def ist_off(day):
    """Minutes to add to an ET clock time to get IST on that date (DST aware)."""
    if isinstance(day, str):
        day = dt.date.fromisoformat(day)
    x = dt.datetime(day.year, day.month, day.day, 12, 0, tzinfo=ET)
    return int((x.astimezone(IST).utcoffset() - x.utcoffset()).total_seconds() // 60)


def ctx_feats(daily, i):
    """Chart structure before the gap. Uses daily[:i] only (daily[i-1] = previous session)."""
    f = {k: None for k in ("runup5", "runup20", "sma20d", "sma50d", "dist52h", "pdv_rvol",
                           "pc_loc", "avgvol20", "day2plus")}
    if i < 3:
        return f
    pc = daily[i - 1]["c"]

    def cl(k):
        return daily[i - 1 - k]["c"] if i - 1 - k >= 0 else None
    if cl(5):
        f["runup5"] = pc / cl(5) - 1
    if cl(20):
        f["runup20"] = pc / cl(20) - 1
    p20 = [r["c"] for r in daily[max(0, i - 20):i]]
    if len(p20) >= 15:
        f["sma20d"] = pc / float(np.mean(p20)) - 1
    p50 = [r["c"] for r in daily[max(0, i - 50):i]]
    if len(p50) >= 40:
        f["sma50d"] = pc / float(np.mean(p50)) - 1
    hs = [r["h"] for r in daily[max(0, i - 252):i]]
    if len(hs) >= 60:
        f["dist52h"] = pc / max(hs) - 1
    v30 = [r["v"] for r in daily[max(0, i - 31):i - 1]]
    if len(v30) >= 10 and np.mean(v30) > 0:
        f["pdv_rvol"] = daily[i - 1]["v"] / float(np.mean(v30))
    v20 = [r["v"] for r in daily[max(0, i - 20):i]]
    if v20:
        f["avgvol20"] = float(np.mean(v20))
    p, pp = daily[i - 1], daily[i - 2]
    if p["h"] > p["l"]:
        f["pc_loc"] = (p["c"] - p["l"]) / (p["h"] - p["l"])
    if pp["c"] > 0:
        f["day2plus"] = bool(p["o"] / pp["c"] - 1 >= GAP_MIN or p["c"] / pp["c"] - 1 >= GAP_MIN)
    return f


def path_full(df, pc, full):
    """Price path as % from PREVIOUS CLOSE at each 5-min step, 04:00 ET (k=0) -> 16:00 ET (k=144)."""
    mm_all = (df.index.hour * 60 + df.index.minute).to_numpy()
    keep = (mm_all >= PM_START) & (mm_all < RG_END)
    mm, c = mm_all[keep], df.c.to_numpy()[keep]
    if not len(mm):
        return [None] * (NPTS + 1)
    out = [0.0]
    for k in range(1, NPTS + 1):
        end = PM_START + 5 * k
        if not full and mm[-1] < end - 5:
            out.append(None)
            continue
        idx = int(np.searchsorted(mm, end, side="left")) - 1
        out.append(0.0 if idx < 0 else round(float(c[idx] / pc - 1), 4))
    return out


def bars2(df):
    """1-minute OHLCV + continuous VWAP, 04:00 -> 16:00 ET, for interactive chart timeframes."""
    mm = df.index.hour * 60 + df.index.minute
    s = df[(mm >= PM_START) & (mm < RG_END)]
    if s.empty:
        return None
    tp = (s.h + s.l + s.c) / 3
    vw = ((tp * s.v).cumsum() / s.v.cumsum().replace(0, np.nan)).ffill()
    b = s
    vb = vw.reindex(b.index)
    t = (b.index.hour * 60 + b.index.minute).tolist()
    return {"t": [int(x) for x in t], "o": b.o.round(4).tolist(), "h": b.h.round(4).tolist(),
            "l": b.l.round(4).tolist(), "c": b.c.round(4).tolist(),
            "v": [int(x) for x in b.v.round(0).tolist()], "vw": vb.round(4).tolist()}


def mdl(rg, start_ts, entry, entry_min, close):
    """Short entry at `entry`, exit at close or at the STOP_PCT stop, whichever comes first."""
    seg = rg.loc[start_ts:]
    hi = float(seg.h.max())
    hit = seg[seg.h >= entry * (1 + STOP_PCT)]
    stop_m = int(hit.index[0].hour * 60 + hit.index[0].minute) if len(hit) else None
    return {"pnl": (entry - close) / entry, "mae": max(hi / entry - 1, 0.0),
            "m": int(entry_min), "p": float(entry), "stop_m": stop_m}


def calc_downlegs(rg, hod_ts, th=0.12, min_drop=0.08):
    """Detects major downward legs from HOD to close if the stock fades out."""
    if rg is None or rg.empty or hod_ts not in rg.index:
        return []
    after = rg.loc[hod_ts:]
    if len(after) < 5:
        return []
    hod = float(after.h.max())
    close = float(after.c.iloc[-1])
    if close >= hod * 0.90:  # did not fade at least 10% from HOD
        return []

    mm = after.index.hour * 60 + after.index.minute
    grp = (mm - 570) // 5
    g5 = after.groupby(grp).agg(h=("h", "max"), l=("l", "min"), c=("c", "last"))
    if len(g5) < 2:
        drop = (close - hod) / hod
        return [drop] if drop <= -min_drop else []

    swings = [("H", hod)]
    mode = "DOWN"
    curr_ext = float(g5.l.iloc[0])

    for _, row in g5.iloc[1:].iterrows():
        h, l = float(row.h), float(row.l)
        if mode == "DOWN":
            if l < curr_ext:
                curr_ext = l
            if curr_ext > 0 and (h - curr_ext) / curr_ext >= th:
                swings.append(("L", curr_ext))
                mode = "UP"
                curr_ext = h
        else:
            if h > curr_ext:
                curr_ext = h
            if curr_ext > 0 and (curr_ext - l) / curr_ext >= th:
                swings.append(("H", curr_ext))
                mode = "DOWN"
                curr_ext = l

    if mode == "DOWN":
        swings.append(("L", min(curr_ext, close)))
    else:
        if curr_ext > 0 and (curr_ext - close) / curr_ext >= min_drop:
            swings.append(("H", curr_ext))
            swings.append(("L", close))

    legs = []
    for k in range(len(swings) - 1):
        if swings[k][0] == "H" and swings[k + 1][0] == "L":
            p_h, p_l = swings[k][1], swings[k + 1][1]
            if p_h > 0:
                d = (p_l - p_h) / p_h
                if d <= -min_drop:
                    legs.append(d)

    if not legs and (close - hod) / hod <= -min_drop:
        legs.append((close - hod) / hod)

    return legs


def metrics(df, pc, full=True):
    mma = df.index.hour * 60 + df.index.minute
    ses = df[(mma >= PM_START) & (mma < RG_END)]
    r = {"pm_high": None, "pm_low": None, "pm_vol": 0.0, "pm_dv": 0.0, "pm_range": None, "pm_spikes": None,
         "pm_hi_min": None, "pm_cum_hi": None, "pm_dv_hi": None, "pm_vb": None, "pm_vb_price": None,
         "pm_vr": None, "pm_vr_all": None, "rg": False}
    if ses.empty:
        return r
    mms = ses.index.hour * 60 + ses.index.minute
    pm, rg = ses[mms < RG_START], ses[mms >= RG_START]
    tps = (ses.h + ses.l + ses.c) / 3
    vws = ((tps * ses.v).cumsum() / ses.v.cumsum().replace(0, np.nan)).ffill()   # continuous pre+reg VWAP

    if len(pm):
        prev_c = pm.c.shift(1)
        spk = (((pm.o / prev_c - 1).abs() >= 0.05) | ((pm.h / pm.l - 1) >= 0.08)).sum()
        r.update(pm_high=float(pm.h.max()), pm_low=float(pm.l.min()), pm_vol=float(pm.v.sum()),
                 pm_dv=float((tps.loc[pm.index] * pm.v).sum()),
                 pm_range=float(pm.h.max() / pm.l.min() - 1), pm_spikes=int(spk))
        hi_ts = pm.h.idxmax()
        pmv = pm.v.loc[:hi_ts]
        r.update(pm_hi_min=int(hi_ts.hour * 60 + hi_ts.minute), pm_cum_hi=float(pmv.sum()),
                 pm_dv_hi=float((tps.loc[pmv.index] * pmv).sum()))
        aft = pm.loc[hi_ts:]
        bk = aft[aft.c < vws.loc[aft.index]]
        r["pm_vb"] = bool(len(bk))
        if len(bk):
            bts = bk.index[0]
            r["pm_vb_price"] = float(bk.c.iloc[0])
            rp = pm.loc[bts:].iloc[1:]
            later = ses.loc[bts:].iloc[1:]
            r["pm_vr"] = bool((rp.c > vws.loc[rp.index]).any())
            r["pm_vr_all"] = bool((later.c > vws.loc[later.index]).any())
    if rg.empty:
        return r

    mm = (rg.index.hour * 60 + rg.index.minute).to_numpy()
    o = float(rg.o.iloc[0])
    hod = float(rg.h.max())
    lod = float(rg.l.min())
    hod_ts = rg.h.idxmax()
    close = float(rg.c.iloc[-1])
    tp = tps.loc[rg.index]
    vw = vws.loc[rg.index]
    vwap_close = float(vw.iloc[-1]) if pd.notna(vw.iloc[-1]) else None
    pm_high = r["pm_high"]

    # post-HOD VWAP loss (used by the entry model + rejection stats)
    after = rg.loc[hod_ts:]
    below = after[after.c < vw.loc[after.index]]
    lost_ts = below.index[0] if len(below) else None
    lost_min, never = None, False
    if lost_ts is not None:
        lost_min = lost_ts.hour * 60 + lost_ts.minute
        aft2 = rg.loc[lost_ts:]
        never = bool((aft2.c < vw.loc[aft2.index]).all())

    # first VWAP break of the regular session (close < VWAP)
    vbk = rg[rg.c < vw]
    vb_min = vb_to_close = vb_to_high = None
    if len(vbk):
        vts, vp = vbk.index[0], float(vbk.c.iloc[0])
        vb_min = int(vts.hour * 60 + vts.minute)
        vb_to_close = close / vp - 1
        vb_to_high = float(rg.h.loc[vts:].max()) / vp - 1

    # opening candles
    first2 = rg[mm < 572]
    first2 = first2 if len(first2) else rg.iloc[:1]
    first5 = rg[mm < 575]
    first5 = first5 if len(first5) else rg.iloc[:1]
    n5 = len(first5)
    f2_ret, f5_ret = float(first2.c.iloc[-1] / o - 1), float(first5.c.iloc[-1] / o - 1)
    f2_l2c, f5_l2c = close / float(first2.l.min()) - 1, close / float(first5.l.min()) - 1
    or_low = float(first5.l.min())
    orl_broke = bool(rg.iloc[n5:].l.min() < or_low) if len(rg) > n5 else None
    failed_bo = None
    if pm_high:
        failed_bo = bool(hod >= pm_high * 0.99 and close < pm_high)
    consol_bd = False
    if len(after) > 55:
        base = after.iloc[10:40]
        if (base.h.max() - base.l.min()) / base.h.max() < 0.20:
            consol_bd = bool(after.iloc[40:].l.min() < base.l.min() * 0.99)

    # entry models (short, exit at close or at the stop) -- each carries entry time/price for the chart
    models = {"Short at open": mdl(rg, rg.index[0], o, mm[0], close)}
    if orl_broke:
        brk = rg.iloc[n5:]
        brk = brk[brk.l < or_low]
        bt = brk.index[0]
        models["5m low break"] = mdl(rg, bt, or_low, bt.hour * 60 + bt.minute, close)
    if lost_ts is not None:
        models["VWAP loss (post-HOD)"] = mdl(rg, lost_ts, float(rg.c.loc[lost_ts]), lost_min, close)
    if pm_high and o < pm_high:
        x = rg[rg.h >= pm_high]
        if len(x):
            xt = x.index[0]
            models["PM high rejection"] = mdl(rg, xt, pm_high, xt.hour * 60 + xt.minute, close)
    grp = (mm - 570) // 5
    g5 = rg.groupby(grp).agg(o=("o", "first"), c=("c", "last"))
    red = g5[g5.c < g5.o]
    if len(red):
        k = int(red.index[0])
        aft5 = rg[grp > k]
        if len(aft5):
            models["First red 5m candle"] = mdl(rg, aft5.index[0], float(red.c.iloc[0]), 570 + 5 * (k + 1), close)

    rng = hod - lod
    downlegs = calc_downlegs(rg, hod_ts, th=0.12, min_drop=0.08)
    r.update(
        rg=True, open=o, hod=hod, lod=lod, close=close, hod_min=hod_ts.hour * 60 + hod_ts.minute,
        close_min=int(mm[-1]) + 1,
        cum_to_hod=r["pm_vol"] + float(rg.v.loc[:hod_ts].sum()),
        cum_dv_to_hod=r["pm_dv"] + float((tp.loc[:hod_ts] * rg.v.loc[:hod_ts]).sum()),
        downlegs=downlegs,
        rg_vol=float(rg.v.sum()), rg_dv=float((tp * rg.v).sum()), vwap_close=vwap_close,
        lost_min=lost_min, vwap_never=never,
        vwap_reject=bool(lost_min is not None and lost_min < 660 and vwap_close is not None and close < vwap_close),
        vb_min=vb_min, vb_to_close=vb_to_close, vb_to_high=vb_to_high,
        close_pos=(close - lod) / rng if rng > 0 else None, close_vs_lod=close / lod - 1,
        gap_fill=min(max((o - lod) / (o - pc), 0.0), 1.0) if o > pc else None,
        f2_ret=f2_ret, f2_red=bool(f2_ret < 0), f2_l2c=f2_l2c, f5_ret=f5_ret, f5_l2c=f5_l2c,
        f5_red=bool(f5_ret < 0), f5_rng=float(first5.h.max() / first5.l.min() - 1),
        orl_broke=orl_broke, failed_bo=failed_bo, consol_bd=consol_bd, models=models,
        path=path_full(df, pc, full))
    return r


def chart_pack(df, m, pc, day, float_shares=None):
    """Everything the browser needs for one 2-min chart (bars + VWAP + entry-model signals)."""
    sig = [{"name": k, "m": v["m"], "p": v["p"], "stop_m": v["stop_m"], "pnl": v["pnl"]}
           for k, v in (m.get("models") or {}).items()]
    return {"bars": bars2(df), "date": str(day), "pc": pc, "open": m.get("open"), "pmh": m.get("pm_high"),
            "off": ist_off(day), "stop": STOP_PCT, "sig": sig, "float_shares": float_shares,
            "cover": ({"m": m["close_min"], "p": m["close"]} if m.get("rg") else None)}


def float_at(day, ctx):
    d = dt.date.fromisoformat(day)
    best = None
    for r in ctx["sec"]:
        if r["filed"] <= day:
            best = r
        else:
            break
    if best:
        so = best["val"]
        for s in ctx["splits"]:
            if best["filed"] < s["date"] <= day:
                so *= s["to"] / s["from"]
        age = (d - dt.date.fromisoformat(best["filed"])).days
        return (so * ctx["ratio"] if ctx["ratio"] else so), age > 60
    if ctx["float"]:
        f = ctx["float"]
        for s in ctx["splits"]:
            if s["date"] > day:
                f *= s["from"] / s["to"]
        return f, True
    return None, True


def outcome(h2c, o2c, c, hod):
    if h2c <= -0.15 and o2c < 0:
        return "Fade"
    if o2c > 0.10 or c >= hod * 0.97:
        return "Squeeze"
    return "Mixed"


def analyze_event(t, i, daily, ctx):
    row, pc = daily[i], daily[i - 1]["c"]
    df = minute_df(t, row["date"])
    if df is None or df.empty:
        return None
    m = metrics(df, pc, True)
    if not m["rg"] or m["open"] <= 0 or m["pm_vol"] < MIN_PM_VOL:
        return None
    fl, stale = float_at(row["date"], ctx)
    cf = ctx_feats(daily, i)
    o, hod, lod, close = m["open"], m["hod"], m["lod"], m["close"]
    h2c, o2c = close / hod - 1, close / o - 1
    c0 = row["c"]
    e = {
        "date": row["date"], "pc": pc, "gap": o / pc - 1, "open": o, "hod": hod, "lod": lod, "close": close,
        "volume": row["v"], "hod_min": m["hod_min"], "hod_bkt": hod_bucket(m["hod_min"]),
        "pm_high": m["pm_high"], "pm_hi_min": m.get("pm_hi_min"),
        "pm_high_pct": (m["pm_high"] / pc - 1) if m["pm_high"] else None,
        "o2h": hod / o - 1, "o2l": lod / o - 1, "o2c": o2c, "hod_close": h2c,
        "close_pos": m["close_pos"], "close_vs_lod": m["close_vs_lod"], "gap_fill": m["gap_fill"],
        "lost_min": m["lost_min"], "vb_min": m["vb_min"], "vb_to_close": m["vb_to_close"],
        "vb_to_high": m["vb_to_high"],
        "f2_ret": m["f2_ret"], "f2_red": m["f2_red"], "f2_l2c": m["f2_l2c"],
        "f5_ret": m["f5_ret"], "f5_red": m["f5_red"], "f5_l2c": m["f5_l2c"], "f5_rng": m["f5_rng"],
        "orl_broke": m["orl_broke"],
        "below_pm": (o < m["pm_high"]) if m["pm_high"] else None,
        "pm_vol": m["pm_vol"], "pm_dv": m["pm_dv"], "pm_fade": (o / m["pm_high"] - 1) if m["pm_high"] else None,
        "pm_range": m["pm_range"], "pm_spikes": m["pm_spikes"], "pm_dv_hi": m["pm_dv_hi"],
        "pm_vb": m["pm_vb"], "pm_vr": m["pm_vr"], "pm_vr_all": m["pm_vr_all"],
        "pmb_to_open": (o / m["pm_vb_price"] - 1) if m["pm_vb_price"] else None,
        "early_hod": m["hod_min"] < 600, "faded15": h2c <= -0.15, "below_open": o2c < 0,
        "below_vwap": (close < m["vwap_close"]) if m["vwap_close"] else None,
        "vwap_never": m["vwap_never"], "vwap_reject": m["vwap_reject"],
        "failed_bo": m["failed_bo"], "consol_bd": m["consol_bd"],
        "float": fl, "stale": stale,
        "cum_dv_to_hod": m.get("cum_dv_to_hod"),
        "downlegs": m.get("downlegs", []),
        "rot_hod": (m["cum_to_hod"] / fl) if fl else None,
        "rot_pm": (m["pm_vol"] / fl) if fl else None,
        "pm_rot_hi": (m["pm_cum_hi"] / fl) if (fl and m["pm_cum_hi"]) else None,
        "rot_day": ((m["pm_vol"] + m["rg_vol"]) / fl) if fl else None,
        "pm_rel": (m["pm_vol"] / cf["avgvol20"]) if cf["avgvol20"] else None,
        "bucket": pbucket(o), "models": m["models"], "path": m["path"],
        "chart": chart_pack(df, m, pc, row["date"], fl),
        "outcome": outcome(h2c, o2c, close, hod),
    }
    e.update({k: cf[k] for k in ("runup5", "runup20", "sma20d", "sma50d", "dist52h", "pdv_rvol", "pc_loc", "day2plus")})

    # ---- multi-day.  Day 1 = first session AFTER the gap day.  Day-1 behaviour is judged vs the gap-day close;
    #      D2/D3/D5 and squeeze/drawdown are measured vs the Day-1 close.
    for k in ("d1_ret", "d1_beh", "d1_red", "b_gapdown", "b_inside", "b_cont", "b_fade", "d2_ret", "d3", "d5"):
        e[k] = None
    for k in (1, 3, 5):
        e[f"sq{k}"] = e[f"dd{k}"] = None
    if i + 1 < len(daily):
        d1 = daily[i + 1]
        c1 = d1["c"]
        beh = ("Inside" if d1["h"] <= row["h"] and d1["l"] >= row["l"] else
               "Gap down" if d1["o"] < c0 * 0.97 else "Continuation" if c1 >= c0 else "Fade")
        e.update(d1_ret=c1 / c0 - 1, d1_beh=beh, d1_red=c1 < c0, b_gapdown=beh == "Gap down",
                 b_inside=beh == "Inside", b_cont=beh == "Continuation", b_fade=beh == "Fade")
        for n, key in ((2, "d2_ret"), (3, "d3"), (5, "d5")):
            if i + n < len(daily):
                e[key] = daily[i + n]["c"] / c1 - 1
        for k in (1, 3, 5):
            seg = daily[i + 2:i + 2 + k]
            if len(seg) == k:
                e[f"sq{k}"] = max(r["h"] for r in seg) / c1 - 1
                e[f"dd{k}"] = min(r["l"] for r in seg) / c1 - 1

    # ---- days to fill the gap (daily low <= prev close), day 0 included, up to 30 trading days forward
    avail = min(30, len(daily) - 1 - i)
    fill_days = None
    for k in range(0, avail + 1):
        if daily[i + k]["l"] <= pc:
            fill_days = k
            break
    e["fill_days"], e["fill_avail"] = fill_days, avail
    e["fill_label"] = ("Day 0" if fill_days == 0 else f"{fill_days}d" if fill_days is not None else
                       "Not filled (30d)" if avail >= 30 else f"Not filled yet ({avail}d)")
    e["fill0"] = fill_days == 0
    for n in (5, 30):
        e[f"fill{n}"] = True if (fill_days is not None and fill_days <= n) else (False if avail >= n else None)

    # ---- best gap fill: deepest retrace toward prev close within 30d, and how many days it took
    best_low, best_k = lod, 0                      # day 0 uses the regular-session low
    for k in range(1, avail + 1):
        if daily[i + k]["l"] < best_low:
            best_low, best_k = daily[i + k]["l"], k
    gsz = o - pc
    e["best_fill"] = clamp((o - best_low) / gsz) if gsz > 0 else None
    e["best_fill_day"] = best_k
    e["best_fill_label"] = ("—" if e["best_fill"] is None else
                            f"{e['best_fill']:.0%} in " + ("day 0" if best_k == 0 else f"{best_k}d"))
    return e


def detect_events(daily, splits, session_day):
    sd = {s["date"] for s in splits}
    cutoff = session_day - dt.timedelta(days=LOOKBACK_DAYS)
    out = []
    for i in range(1, len(daily)):
        r = daily[i]
        d = dt.date.fromisoformat(r["date"])
        if d >= session_day or d < cutoff or r["date"] in sd:
            continue
        pc = daily[i - 1]["c"]
        if pc > 0 and r["o"] / pc - 1 >= GAP_MIN and r["o"] >= MIN_PRICE and r["v"] >= MIN_DAY_VOL:
            out.append(i)
    return out[-MAX_EVENTS:]


# ----------------------------------------------------------------------------- stats / grade
def nn(x):
    return x is not None and not (isinstance(x, float) and math.isnan(x))


def vals(S, k):
    return [e[k] for e in S if nn(e.get(k))]


def med(xs):
    return float(np.median(xs)) if xs else None


def pctl(xs, q):
    return float(np.percentile(xs, q)) if xs else None


def frac(S, k):
    v = [e[k] for e in S if e.get(k) is not None]
    return sum(1 for x in v if x), len(v)


def clamp(x):
    return max(0.0, min(1.0, x))


def rate(f):
    return f[0] / f[1] if f[1] else None


def stats(S):
    st = {"n": len(S)}
    for k in ("faded15", "below_open", "below_vwap", "early_hod", "vwap_never", "vwap_reject", "failed_bo",
              "consol_bd", "d1_red", "below_pm", "orl_broke", "f5_red", "pm_vb", "pm_vr", "pm_vr_all"):
        st[k] = frac(S, k)
    for k in ("fill0", "fill5", "fill30"):
        st[k] = frac(S, k)
    st["med_fill"] = med(vals(S, "fill_days"))
    st["hodb"] = {b: sum(1 for e in S if e["hod_bkt"] == b) for b in ("First 5m", "<10:00", "Midday", "Late")}
    for k in ("hod_min", "hod_close", "o2c", "o2h", "o2l", "gap", "gap_fill", "best_fill", "vb_to_close", "vb_to_high"):
        st["med_" + k] = med(vals(S, k))
    st["p75_o2h"] = pctl(vals(S, "o2h"), 75)
    rots = vals(S, "rot_hod")
    st["avg_rot"] = float(np.mean(rots)) if rots else None
    st["p75_rot"] = pctl(rots, 75)
    return st


def grade(st):
    n = st["n"]
    if n < 1:
        return None
    s_fade = (rate(st["below_open"]) or 0) * 100
    s_o2c = clamp(-(st["med_o2c"] or 0) / 0.20) * 100
    s_sq = clamp(1 - (st["p75_o2h"] or 0) / 0.60) * 100
    s_cons = (((rate(st["early_hod"]) or 0) + (rate(st["vwap_never"]) or 0)) / 2) * 100
    s_d2 = (rate(st["d1_red"]) * 100) if rate(st["d1_red"]) is not None else 50
    s_n = min(n / 20, 1) * 100
    score = .30 * s_fade + .20 * s_o2c + .20 * s_sq + .15 * s_cons + .10 * s_d2 + .05 * s_n
    score = round(score)
    letter = "F"
    for th, l in [(85, "A"), (80, "A-"), (75, "B+"), (70, "B"), (65, "B-"), (60, "C+"), (55, "C"), (50, "C-"), (40, "D")]:
        if score >= th:
            letter = l
            break
    comps = [("Fade rate (red close vs open)", 30, s_fade), ("Median open→close", 20, s_o2c),
             ("Squeeze risk (p75 open→HOD)", 20, s_sq), ("Early HOD / VWAP-loss consistency", 15, s_cons),
             ("Day-1 follow-through", 10, s_d2), ("Sample size", 5, s_n)]
    return {"score": score, "letter": letter, "comps": comps}


def summary_text(st):
    fr, early, vr = rate(st["below_open"]), rate(st["early_hod"]), rate(st["vwap_reject"])
    if fr is None:
        return "Not enough comparable setups."
    s = ("Historically tends to fade" if fr >= .6 else
         "Historically tends to hold or squeeze" if fr <= .4 else "Historically mixed")
    if early is not None and early >= .6:
        s += " after early highs"
    if vr is not None and vr >= .6:
        s += ", with frequent VWAP rejection"
    if st["p75_o2h"] is not None and st["p75_o2h"] > 0.30:
        s += f"; squeeze risk is real (p75 open→HOD {st['p75_o2h']*100:+.0f}%)"
    return s + "."


def verdict_text(g, st):
    fr = rate(st["below_open"]) or 0
    kind = "fade" if fr >= .6 else "mixed" if fr > .4 else "squeeze-prone"
    p75 = st["p75_o2h"] or 0
    risk = "low" if p75 < .2 else "moderate" if p75 < .45 else "high"
    return f"{g['letter']} {kind}: {fr:.0%} red, median {pct(st['med_o2c'], 0)}, squeeze risk {risk}"


# ----------------------------------------------------------------------------- formatting
def pct(x, d=1):
    return f"{x*100:+.{d}f}%" if nn(x) else "—"


def usd(x):
    return "—" if not nn(x) else (f"${x:,.2f}" if x >= 1 else f"${x:.3f}")


def big(x):
    if not nn(x):
        return "—"
    for div, s in [(1e9, "B"), (1e6, "M"), (1e3, "K")]:
        if abs(x) >= div:
            return f"{x/div:.1f}{s}"
    return f"{x:.0f}"


def dv(x):
    return "$" + big(x) if nn(x) else "—"


def sc(x):
    return "w" if not nn(x) or x == 0 else ("g" if x > 0 else "r")


def hhmm(m, day=None):
    if not nn(m):
        return "—"
    if day is None:
        day = dt.date.today()
    elif isinstance(day, str):
        try:
            day = dt.date.fromisoformat(day)
        except Exception:
            day = dt.date.today()
    h = int(m // 60)
    mn = int(m % 60)
    try:
        dt_et = dt.datetime(day.year, day.month, day.day, h, mn, tzinfo=ET)
        return dt_et.astimezone(IST).strftime("%H:%M")
    except Exception:
        return f"{h:02d}:{mn:02d}"


def C(text, cls="w", s=None):
    return [text, cls, s if s is not None else text]


def frow(label, f, hi=.6, lo=.4):
    k, n = f
    if not n:
        return [label, "—", "w"]
    r = k / n
    return [label, f"{k} / {n}  ({r:.0%})", "g" if r >= hi else "y" if r >= lo else "r"]


def fmt(kind, x, day=None):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "—"
    if isinstance(x, bool):
        return "Yes" if x else "No"
    if isinstance(x, str):
        return x
    return {"pct0": lambda: pct(x, 0), "pct1": lambda: pct(x, 1), "rate": lambda: f"{x:.0%}",
            "x": lambda: f"{x:.1f}x", "usd": lambda: "$" + big(x), "time": lambda: hhmm(x, day),
            "num": lambda: f"{x:.0f}"}[kind]()


def cndl(red, ret):
    if ret is None or red is None:
        return C("—", "w", "")
    return C(f"{'Red' if red else 'Green'} {pct(ret, 0)}", "r" if red else "g", ret)


def ynC(b, yes_cls="b"):
    if b is None:
        return C("—", "w", "")
    return C("Yes" if b else "No", yes_cls if b else "w", int(bool(b)))


def trip(e, k):
    return "/".join(("—" if e.get(f"{k}{d}") is None else f"{e[f'{k}{d}']*100:+.0f}") for d in (1, 3, 5))


def kv_text(rows, w=None):
    w = w or max(len(l) for l, _, _ in rows) + 2
    return "\n".join(f"{l + ':':<{w}}{v}" for l, v, _ in rows)


def tbl_text(cols, rows):
    ws = [max([len(c)] + [len(str(r[i][0])) for r in rows]) for i, c in enumerate(cols)]
    line = "  ".join(c.ljust(ws[i]) for i, c in enumerate(cols))
    body = ["  ".join(str(r[i][0]).ljust(ws[i]) for i in range(len(cols))) for r in rows]
    return "\n".join([line, "-" * len(line)] + body)


def model_rows(S):
    rows = []
    for name in ["Short at open", "PM high rejection", "5m low break", "First red 5m candle", "VWAP loss (post-HOD)"]:
        ms = [e["models"][name] for e in S if name in e["models"]]
        if not ms:
            continue
        eff = [(-STOP_PCT if x["mae"] >= STOP_PCT else x["pnl"]) for x in ms]
        wins = [p for p in eff if p > 0]
        loss = [p for p in eff if p <= 0]
        wr = len(wins) / len(eff)
        exp = float(np.mean(eff))
        maes = [x["mae"] for x in ms]
        rows.append([C(name), C(str(len(ms))), C(f"{wr:.0%}", "g" if wr >= .55 else "y" if wr >= .45 else "r", wr),
                     C(pct(np.mean(wins)) if wins else "—", "g"), C(pct(np.mean(loss)) if loss else "—", "r"),
                     C(pct(exp), sc(exp), exp), C(f"{exp / STOP_PCT:+.2f}R", sc(exp), exp),
                     C(pct(med(maes), 0), "y", med(maes)), C(pct(max(maes), 0), "r", max(maes))])
    return rows


def fdate(d):
    try:
        return dt.date.fromisoformat(str(d)).strftime("%m/%d/%Y")
    except Exception:
        return str(d)


# ----------------------------------------------------------------------------- report
def build_report(t, live, lm, lpath, lctx, events, S, notes, daily):
    simset = {id(e) for e in S}
    st = stats(S)
    open_hod = recent_open_hod_stats(daily)
    hod_times = recent_hod_times(t, daily)
    g = grade(st)
    pc, cur = live["pc"], live["cur"]
    fl_now = live["float"] or live["so"]
    open_ = lm.get("open")
    vol = (lm.get("pm_vol") or 0.0) + (lm.get("rg_vol") or 0.0)
    dvol = (lm.get("pm_dv") or 0.0) + (lm.get("rg_dv") or 0.0)
    if (not vol or vol == 0) and live.get("session_vol"):
        vol = float(live["session_vol"])
        if not dvol or dvol == 0:
            dvol = vol * cur
    gap_live = ((open_ or cur) / pc - 1)
    rot_live = (vol / fl_now) if (fl_now and vol) else None
    hod_rot_live = (lm["cum_to_hod"] / fl_now) if (fl_now and lm.get("cum_to_hod")) else rot_live
    mcap = live["mcap"] or ((live["so"] or fl_now or 0) * cur) or None
    pm_rel_live = (lm["pm_vol"] / lctx["avgvol20"]) if (lctx.get("avgvol20") and lm.get("pm_vol")) else None

    day_obj = live["date"]
    day_str = str(day_obj)
    day_name = day_obj.strftime("%A") if hasattr(day_obj, "strftime") else ""
    day_display = f"{day_str} ({day_name})" if day_name else day_str
    next_session = day_obj + dt.timedelta(days=1)
    while next_session.weekday() >= 5:
        next_session += dt.timedelta(days=1)

    # ---- [1] current day
    pm_fade = (open_ / lm["pm_high"] - 1) if open_ and lm.get("pm_high") else None
    left = [
        ["Current Day (session)", day_display, "c"],
        ["Gap %", pct(gap_live), sc(gap_live)],
        ["Float Rotation (so far)", f"{rot_live:.2f}x" if (rot_live is not None and rot_live > 0) else "—", "b"],
        ["Volume (so far)", f"{big(vol)} shares" if vol else "0 shares", "b"],
        ["Dollar Volume (so far)", "$" + big(dvol) if dvol else "$0", "b"],
    ]
    right = [
        ["Premarket High → Open Fade", pct(pm_fade) if pm_fade is not None else ("— (premarket)" if not open_ else "—"), sc(pm_fade) if pm_fade is not None else "d"],
        ["Sector", live["sector"], "b"],
        ["Float", f"{big(fl_now)} shares" if fl_now else "—", "b"],
        ["Market Cap", "$" + big(mcap) if mcap else "—", "b"],
    ]

    # ---- [2] [3] [4] [5] per-event tables (all share the same row order / dim flags)
    cols2 = ["#", "Date", "1st 2m Candle", "1st 5m Candle", "2m Low → Close", "5m Low → Close", "Gap %",
             "Premkt High %", "Premkt High → Open %", "HOD", "HOD Time (IST)", "Open → Close %", "High → Close %",
             "Float Rot @ HOD", "Dollar Vol @ HOD", "Gap Fill (≤30d)"]
    cols3 = ["#", "Date", "Pre Mkt High %", "Pre Mkt High → Open", "VWAP Break → Open", "Float Rot @ PM High",
             "$ Vol @ PM High", "$ Vol Total PM", "Vol Total PM", "VWAP Breakdown (PM)", "VWAP Reclaim (PM)",
             "VWAP Reclaim (Pre+Reg)"]
    cols4 = ["#", "Date", "O → HOD", "O → LOD", "1st VWAP Break → Close", "1st VWAP Break → High", "HOD Bucket",
             "VWAP Loss (IST)", "Close Pos (in range)", "Close vs LOD", "Gap Fill", "Best Gap Fill",
             "Downlegs (#)", "Downleg %s"]
    cols5 = ["#", "Date", "Day #", "Day 1", "Day 1 Ret", "D2 ret", "D3", "D5", "Squeeze 1/3/5d", "Drawdn 1/3/5d"]
    R2, R3, R4, R5, dim = [], [], [], [], []
    for i, e in enumerate(events, 1):
        d = fdate(e["date"])
        star = "*" if e["stale"] else ""
        rot = "—" if e["rot_hod"] is None else f"{e['rot_hod']:.1f}x{star}"
        prot = "—" if e["pm_rot_hi"] is None else f"{e['pm_rot_hi']:.2f}x{star}"
        dl = e.get("downlegs", [])
        num = lambda: C(str(i), "w", i)
        R2.append([
            num(), C(d, "w", e["date"]), cndl(e["f2_red"], e["f2_ret"]), cndl(e["f5_red"], e["f5_ret"]),
            C(pct(e["f2_l2c"], 0), sc(e["f2_l2c"]), e["f2_l2c"]), C(pct(e["f5_l2c"], 0), sc(e["f5_l2c"]), e["f5_l2c"]),
            C(pct(e["gap"], 0), "g", e["gap"]), C(pct(e["pm_high_pct"], 0), "g", e["pm_high_pct"]),
            C(pct(e["pm_fade"], 0), sc(e["pm_fade"]), e["pm_fade"]), C(usd(e["hod"]), "w", e["hod"]),
            C(hhmm(e["hod_min"], e["date"]), "w", e["hod_min"]),
            C(pct(e["o2c"], 0), sc(e["o2c"]), e["o2c"]), C(pct(e["hod_close"]), sc(e["hod_close"]), e["hod_close"]),
            C(rot, "b", e["rot_hod"]), C(dv(e.get("cum_dv_to_hod")), "b", e.get("cum_dv_to_hod")),
            C(e["fill_label"], "b" if e["fill_days"] is not None else "y",
              e["fill_days"] if e["fill_days"] is not None else 99)])
        R3.append([
            num(), C(d, "w", e["date"]), C(pct(e["pm_high_pct"], 0), "g", e["pm_high_pct"]),
            C(pct(e["pm_fade"], 0), sc(e["pm_fade"]), e["pm_fade"]),
            C(pct(e["pmb_to_open"], 0), sc(e["pmb_to_open"]), e["pmb_to_open"]),
            C(prot, "b", e["pm_rot_hi"]), C(dv(e["pm_dv_hi"]), "b", e["pm_dv_hi"]), C(dv(e["pm_dv"]), "b", e["pm_dv"]),
            C(big(e["pm_vol"]), "b", e["pm_vol"]), ynC(e["pm_vb"], "r"), ynC(e["pm_vr"], "g"), ynC(e["pm_vr_all"], "g")])
        bf = e["best_fill_label"]
        R4.append([
            num(), C(d, "w", e["date"]), C(pct(e["o2h"], 0), "g", e["o2h"]), C(pct(e["o2l"], 0), "r", e["o2l"]),
            C(pct(e["vb_to_close"], 0), sc(e["vb_to_close"]), e["vb_to_close"]),
            C(pct(e["vb_to_high"], 0), "y", e["vb_to_high"]), C(e["hod_bkt"]),
            C(hhmm(e["vb_min"], e["date"]), "w", e["vb_min"]),
            C(fmt("rate", e["close_pos"]), "w", e["close_pos"]), C(pct(e["close_vs_lod"], 0), "w", e["close_vs_lod"]),
            C(fmt("rate", e["gap_fill"]), "w", e["gap_fill"]), C(bf, "b", e["best_fill"]),
            C(str(len(dl)) if dl else "—", "r" if dl else "w", len(dl)),
            C(" / ".join(f"{x*100:.0f}%" for x in dl) if dl else "—", "r" if dl else "w", len(dl))])
        R5.append([
            num(), C(d, "w", e["date"]), C("2+" if e["day2plus"] else "1" if e["day2plus"] is not None else "—", "w"),
            C(e["d1_beh"] or "—", "w"), C(pct(e["d1_ret"], 0), sc(e["d1_ret"]), e["d1_ret"]),
            C(pct(e["d2_ret"], 0), sc(e["d2_ret"]), e["d2_ret"]), C(pct(e["d3"], 0), sc(e["d3"]), e["d3"]),
            C(pct(e["d5"], 0), sc(e["d5"]), e["d5"]), C(trip(e, "sq"), "y", e["sq5"]), C(trip(e, "dd"), "r", e["dd5"])])
        dim.append(id(e) not in simset)

    foot2 = ("Times in IST. 2m/5m Low → Close = move from the low of the first 2/5 minutes to the day's close. "
             "* float estimate is stale (>60d since filing) or based on today's float. Dim rows = not in the similar set.")
    foot3 = ("VWAP = continuous pre+reg VWAP from 04:00 ET. Breakdown = a premarket close below VWAP after the PM high. "
             "VWAP Break → Open = open vs the price of that first break. Reclaim = a later close back above VWAP "
             "(PM only / anywhere through the close).")
    foot4 = ("1st VWAP Break = first regular-session close below VWAP. Close vs LOD = close/low-of-day − 1. "
             "Best Gap Fill = deepest retrace toward prev close within 30 trading days, and the day it happened (day 0 = gap day).")
    foot5 = ("Day 1 = first session after the gap day. Gap down: opened >3% below gap-day close · Fade: closed below it · "
             "Continuation: closed at/above it · Inside: traded inside the gap day's range. "
             "D2/D3/D5 = cumulative return vs Day-1 close. Squeeze/Drawdn 1/3/5d = max high / low after the Day-1 close within "
             "the next 1/3/5 sessions (e.g. +19/+25/+25). Day #: 1 = first gap day, 2+ = consecutive gapper.")
    tables = [
        {"title": f"[2] HISTORICAL GAP UPS (last 1 year, gap ≥ {GAP_MIN*100:.0f}%) — click headers to sort",
         "cols": cols2, "rows": R2, "dim": dim, "foot": foot2},
        {"title": "[3] PREMARKET ANALYSIS", "cols": cols3, "rows": R3, "dim": dim, "foot": foot3},
        {"title": "[4] INTRADAY STRUCTURE", "cols": cols4, "rows": R4, "dim": dim, "foot": foot4},
        {"title": "[5] MULTIDAY STRUCTURE", "cols": cols5, "rows": R5, "dim": dim, "foot": foot5},
    ]

    # ---- [6] per-event 2-min charts (today first)
    evch = []
    if live["df"] is not None and len(live["df"]):
        evch.append({"title": f"TODAY {day_str}  gap {pct(gap_live, 0)}  (live)", "date": day_str, "sim": True,
                     "chart": chart_pack(live["df"], lm, pc, day_obj, fl_now)})
    for i, e in enumerate(events, 1):
        evch.append({"title": f"#{i}  {fdate(e['date'])}  gap {pct(e['gap'], 0)}  O→C {pct(e['o2c'], 0)}", "date": e["date"],
                     "sim": id(e) in simset, "chart": e["chart"]})

    # ---- extras: stats / patterns / rating
    p3 = [
        frow("Faded ≥ 15% from HOD", st["faded15"]),
        frow("Closed below open (fade rate)", st["below_open"]),
        frow("Below VWAP at close", st["below_vwap"]),
        frow("Lost VWAP, never reclaimed", st["vwap_never"]),
        ["Median HOD time (IST)", hhmm(st["med_hod_min"], live["date"]), "b"],
        ["Median HOD → close", pct(st["med_hod_close"], 0), sc(st["med_hod_close"])],
        ["Median open → close", pct(st["med_o2c"], 0), sc(st["med_o2c"])],
        ["Squeeze open→HOD med / p75", f"{pct(st['med_o2h'], 0)} / {pct(st['p75_o2h'], 0)}", "y"],
        ["Median open → LOD", pct(st["med_o2l"], 0), "r"],
        ["Median gap fill (day)", fmt("rate", st["med_gap_fill"]), "b"],
        ["Median best gap fill (≤30d)", fmt("rate", st["med_best_fill"]), "b"],
        frow("Day-1 closed lower", st["d1_red"]),
        frow("Gap filled same day (day 0)", st["fill0"]),
        frow("Gap filled within 5 days", st["fill5"]),
        frow("Gap filled within 30 days", st["fill30"]),
        ["Median days to fill (filled only)", f"{st['med_fill']:.0f} days" if st["med_fill"] is not None else "—", "b"],
        ["Not filled after 30 days", (f"{st['fill30'][1] - st['fill30'][0]} / {st['fill30'][1]}" if st["fill30"][1] else "—"), "y"],
    ]
    hb = st["hodb"]
    p4 = [
        frow(f"Early HOD (<{hhmm(600, live['date'])} IST / <10:00 ET)", st["early_hod"]),
        ["HOD first 5 min / <30m (Early)", f"{hb['First 5m']} / {hb['<10:00']}  of {st['n']}", "b"],
        [f"HOD midday / late ({hhmm(840, live['date'])}+ IST)", f"{hb['Midday']} / {hb['Late']}  of {st['n']}", "b"],
        frow("VWAP rejection", st["vwap_reject"]),
        frow("VWAP broken in premarket", st["pm_vb"]),
        frow("VWAP reclaimed in premarket", st["pm_vr"]),
        frow("VWAP reclaimed pre+reg", st["pm_vr_all"]),
        ["1st VWAP break → close (median)", pct(st["med_vb_to_close"], 0), sc(st["med_vb_to_close"])],
        ["1st VWAP break → high (median)", pct(st["med_vb_to_high"], 0), "y"],
        frow("Failed breakouts (PM high)", st["failed_bo"]),
        frow("Consolidation → breakdown", st["consol_bd"]),
        frow("Opened below PM high", st["below_pm"]),
        frow("Opening-range (5m) low broke", st["orl_broke"]),
        frow("First 5m candle red", st["f5_red"]),
        ["Average float rotation @ HOD", f"{st['avg_rot']:.1f}x" if st["avg_rot"] else "—", "b"],
        ["Rotation @ HOD p75", f"{st['p75_rot']:.1f}x" if st["p75_rot"] else "—", "b"],
    ]
    summary = summary_text(st)
    if g:
        verdict = verdict_text(g, st)
        gcls = "g" if g["letter"][0] in "AB" else "y" if g["letter"][0] == "C" else "r"
        p5 = [["Chart-Based Fade Grade", f"{g['letter']}  ({g['score']}/100)", gcls],
              ["Sample Size", f"{st['n']} similar of {len(events)} gap-ups", "b"],
              ["Verdict", verdict, gcls]]
        for name, w, s in g["comps"]:
            p5.append([f"  {name} ({w}%)", f"{s:.0f}", "w"])
        head = {"grade": g["letter"], "score": f"{g['score']}/100", "gcls": gcls,
                "verdict": verdict, "total": len(events)}
    else:
        p5 = [["Chart-Based Fade Grade", "N/A (no similar gap-ups found)", "r"],
              ["Sample Size", f"{st['n']} similar of {len(events)} gap-ups", "b"]]
        head = {"grade": "N/A", "score": "—", "gcls": "r", "verdict": "Not enough history.", "total": len(events)}
    p5.append(["Key Takeaway", summary, "w"])
    if hod_rot_live and st["p75_rot"]:
        hot = hod_rot_live > st["p75_rot"]
        p5.append(["Live rotation vs history",
                   f"{hod_rot_live:.1f}x vs p75 {st['p75_rot']:.1f}x" + ("  EXHAUSTION ZONE" if hot else ""),
                   "r" if hot else "w"])

    mcols = ["Entry (short, exit at close / stop)", "Trades", "Win %", "Avg win", "Avg loss", "Expectancy",
             f"Avg R (stop {STOP_PCT:.0%})", "Median squeeze", "Worst squeeze"]
    mrows = model_rows(S)

    # ---- [7] aggregate path chart: premarket -> close, % from prev close, IST axis
    arr = np.array([[np.nan if v is None else v for v in e["path"]] for e in S], dtype=float) if S else np.zeros((0, NPTS + 1))

    def band(q):
        out = []
        for k in range(NPTS + 1):
            col = arr[:, k] if len(arr) else []
            col = col[~np.isnan(col)] if len(arr) else []
            out.append(round(float(np.percentile(col, q)), 4) if len(col) >= 1 else None)
        return out
    labels = [hhmm(PM_START + 5 * k, live["date"]) for k in range(NPTS + 1)]
    # yellow dashed = today's actual path so far, then history re-based at "now" (median of P(t)/P(now))
    now_k = max((k for k, v in enumerate(lpath) if v is not None), default=None)
    proj = [None] * (NPTS + 1)
    if now_k is not None and S:
        base = 1 + lpath[now_k]
        for k in range(now_k, NPTS + 1):
            rs = [(1 + e["path"][k]) / (1 + e["path"][now_k]) for e in S
                  if e["path"][k] is not None and e["path"][now_k] is not None and 1 + e["path"][now_k] > 0.05]
            if rs:
                proj[k] = round(base * float(np.median(rs)) - 1, 4)
    gap_offsets = list(range(GAP_REL_START, GAP_REL_END + 1, 5))
    gap_labels = ["PM high" if offset == 0 else f"{offset:+d}m" for offset in gap_offsets]
    def gap_retention(path, gap_size, pm_high_min):
        aligned = []
        if not gap_size or gap_size <= 0 or pm_high_min is None:
            return [None] * len(gap_offsets)
        for offset in gap_offsets:
            source_pos = (pm_high_min + offset - PM_START) / 5
            if source_pos < 0 or source_pos > NPTS:
                aligned.append(None)
                continue
            left, right = int(math.floor(source_pos)), int(math.ceil(source_pos))
            if path[left] is None or path[right] is None:
                aligned.append(None)
                continue
            fraction = source_pos - left
            value = path[left] + (path[right] - path[left]) * fraction
            aligned.append(round(value / gap_size, 4))
        return aligned

    historical_gap_paths = [gap_retention(e["path"], e.get("pm_high_pct"), e.get("pm_hi_min")) for e in S]
    gap_arr = np.array([[np.nan if v is None else v for v in path]
                        for path in historical_gap_paths], dtype=float) if historical_gap_paths else np.zeros((0, len(gap_offsets)))
    def gap_band(q):
        out = []
        for k in range(len(gap_offsets)):
            col = gap_arr[:, k] if len(gap_arr) else []
            col = col[~np.isnan(col)] if len(gap_arr) else []
            out.append(round(float(np.percentile(col, q)), 4) if len(col) else None)
        return out
    live_gap_size = ((lm.get("pm_high") / live["pc"] - 1)
                     if lm.get("pm_high") and live["pc"] > 0 and lm["pm_high"] > live["pc"] else None)
    live_pm_high_min = lm.get("pm_hi_min")
    gap_today = gap_retention(lpath, live_gap_size, live_pm_high_min)
    gap_now_k = max((k for k, value in enumerate(gap_today) if value is not None), default=None)
    gap_proj = [None] * len(gap_offsets)
    if live_gap_size and gap_now_k is not None:
        gap_proj[gap_now_k] = gap_today[gap_now_k]
        for k in range(gap_now_k + 1, len(gap_offsets)):
            changes = [path[k] - path[gap_now_k] for path in historical_gap_paths
                       if path[k] is not None and path[gap_now_k] is not None]
            if changes:
                gap_proj[k] = round(gap_today[gap_now_k] + float(np.median(changes)), 4)
    gap_open_k = (round((RG_START - live_pm_high_min - GAP_REL_START) / 5)
                  if live_pm_high_min is not None else None)
    chart = {"labels": labels, "paths": [e["path"] for e in S], "med": band(50), "p25": band(25), "p75": band(75),
             "today": lpath, "proj": proj, "gap_labels": gap_labels, "gap_paths": historical_gap_paths,
             "gap_med": gap_band(50), "gap_p25": gap_band(25), "gap_p75": gap_band(75),
             "gap_today": gap_today, "gap_proj": gap_proj, "premarket_gap_pct": live_gap_size,
             "gap_now_k": gap_now_k, "gap_open_k": gap_open_k,
             "normalization": "prev_close", "now_k": now_k, "open_k": (RG_START - PM_START) // 5}
    keyk = [(0, 240), (66, 570), (72, 600), (84, 660), (96, 720), (120, 840), (144, 960)]
    ptext = []
    for k, mn in keyk:
        m_, a_, b_ = chart["med"][k], chart["p25"][k], chart["p75"][k]
        ptext.append([f"{hhmm(mn, live['date'])} IST  history median / p25..p75 / today / proj (% from prev close)",
                      f"{pct(m_, 0)}  /  {pct(a_, 0)}..{pct(b_, 0)}  /  {pct(lpath[k], 0)}  /  {pct(proj[k], 0)}", "w"])

    head.update(ticker=t, n=st["n"], summary=summary,
                sub=f"Chart-Based Gap Up Analysis — Current Day: {day_display} · Times in IST", notes=notes)
    T = {
        "p1": f"[1] CURRENT DAY SETUP ({day_display} — latest data)",
        "p6": "[6] 2-MIN CHARTS — premarket → close, volume + VWAP, SELL = short entry, BUY = cover (stop or close)",
        "p7": "[7] NORMALIZED PRICE PATH",
        "pa": "[A] HISTORICAL STATISTICS (similar)", "pb": "[B] KEY PATTERNS (similar)", "pc": "[C] FINAL RATING",
        "pd": "[D] ENTRY MODELS (similar setups)",
    }
    dilution_url = f"https://finviz.com/stock?b=2&p=m&t={quote(t, safe='')}&ta=1&ty=di"
    text = [f"Ticker: {t}  |  Finviz Dilution: {dilution_url}", f"Current Day: {day_display}", head["sub"], "=" * 42,
            f"Final Fade Grade:  {head['grade']}   ({head['score']})",
            f"Sample Size: {st['n']} similar setups of {len(events)} gap-ups",
            f"Verdict: {head['verdict']}", f"Summary: {summary}", "",
            T["p1"], kv_text(left), kv_text(right)]
    for tb in tables:
        text += ["", tb["title"], tbl_text(tb["cols"], tb["rows"]), tb["foot"]]
    stat_line = lambda x: f"median {pct(x['median'], 0)}  avg {pct(x['avg'], 0)}  ({x['n']} days)"
    text += ["", "Open → HOD (daily bars; 12 mo for gap-up / volume rows)",
             f"Last 30 sessions  {stat_line(open_hod['last30'])}",
             f"Last 90 sessions  {stat_line(open_hod['last90'])}",
             f"Gap-up days ≥5%  {stat_line(open_hod['gapup5'])}",
             f"High-volume days  {stat_line(open_hod['highvol'])}",
             f"HOD time (15m bars, {hod_times['date_from']} to {hod_times['date_to']})",
             f"All sessions  median {hod_times['median_time'] or '—'} IST ({len(hod_times['rows'])} days)",
             "", T["p7"], kv_text(ptext, 40), "",
             T["pa"], kv_text(p3), "", T["pb"], kv_text(p4), "", T["pc"], kv_text(p5)]
    if mrows:
        text += ["", T["pd"], tbl_text(mcols, mrows)]
    if notes:
        text += [""] + [f"Note: {n}" for n in notes]
    return {"head": head, "titles": T, "p1": {"left": left, "right": right},
            "open_hod_stats": open_hod, "hod_times": hod_times, "tables": tables,
            "evcharts": evch, "session_dates": [r["date"] for r in daily] + [day_str, next_session.isoformat()],
            "model_names": ["Short at open", "PM high rejection", "5m low break",
                                              "First red 5m candle", "VWAP loss (post-HOD)"],
            "pa": p3, "pb": p4, "pc": p5, "pd": {"cols": mcols, "rows": mrows},
            "chart": chart, "dilution_url": dilution_url, "text": "\n".join(text)}


def clean(o):
    if isinstance(o, dict):
        return {k: clean(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [clean(v) for v in o]
    if isinstance(o, np.generic):
        o = o.item()
    if isinstance(o, float) and (math.isnan(o) or math.isinf(o)):
        return None
    return o


# ----------------------------------------------------------------------------- job runner
JOBS = {}
from concurrent.futures import ThreadPoolExecutor, as_completed


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


ALLOWED_USERS = {"karthik", "locke", "gunshow"}
ADMIN_USERS = {"karthik", "locke"}
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")


def _supabase_headers(prefer="return=minimal"):
    k = os.getenv("SUPABASE_KEY", "") or SUPABASE_KEY
    h = {
        "apikey": k,
        "Authorization": f"Bearer {k}",
        "Content-Type": "application/json"
    }
    if prefer:
        h["Prefer"] = prefer
    return h


def _db_sync_users():
    """Sync ALLOWED_USERS and ADMIN_USERS from Supabase app_users table."""
    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    if not (sub_url and sub_key):
        return
    try:
        r = requests.get(f"{sub_url}/rest/v1/app_users?select=username,is_admin,can_rename", headers=_supabase_headers(), timeout=5)
        if r.status_code == 200:
            rows = r.json()
            if isinstance(rows, list) and rows:
                global ALLOWED_USERS, ADMIN_USERS
                ALLOWED_USERS = {x["username"].lower() for x in rows if x.get("username")}
                ADMIN_USERS = {x["username"].lower() for x in rows if x.get("is_admin") or x.get("can_rename")}
    except Exception:
        pass


def _db_get_user(username: str):
    u = (username or "").strip().lower()
    if not u:
        return None
    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    if sub_url and sub_key:
        try:
            r = requests.get(f"{sub_url}/rest/v1/app_users?username=eq.{u}&select=*", headers=_supabase_headers(), timeout=5)
            if r.status_code == 200:
                rows = r.json()
                if isinstance(rows, list) and rows:
                    return rows[0]
        except Exception:
            pass
    if u in ALLOWED_USERS:
        is_root = u in {"karthik", "locke"}
        return {"username": u, "is_admin": is_root or u in ADMIN_USERS, "can_rename": is_root}
    return None


def _hash_password(password: str, salt: str = None) -> tuple[str, str]:
    if not salt:
        salt = secrets.token_hex(16)
    pwd_hash = hashlib.sha256((salt + password).encode("utf-8")).hexdigest()
    return pwd_hash, salt


def _verify_password(password: str, pwd_hash: str, salt: str) -> bool:
    if not (password and pwd_hash and salt):
        return False
    calc_hash = hashlib.sha256((salt + password).encode("utf-8")).hexdigest()
    return secrets.compare_digest(calc_hash, pwd_hash)


def _db_set_password(username: str, password: str):
    u = username.strip().lower()
    pwd_hash, salt = _hash_password(password)
    new_token = uuid.uuid4().hex
    now_z = dt.datetime.now(dt.timezone.utc).isoformat()
    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    if sub_url and sub_key:
        payload = {
            "password_hash": pwd_hash,
            "salt": salt,
            "session_token": new_token,
            "session_updated_at": now_z
        }
        requests.patch(f"{sub_url}/rest/v1/app_users?username=eq.{u}", json=payload, headers=_supabase_headers(), timeout=5)
    return new_token


def _db_login(username: str, password: str, force_logout: bool = False):
    u = username.strip().lower()
    user = _db_get_user(u)
    if not user:
        return {"error": "User not authorized"}, 404
    
    if not user.get("password_hash") or not user.get("salt"):
        return {"error": "Password not created yet", "require_create": True}, 400
    
    if not _verify_password(password, user.get("password_hash"), user.get("salt")):
        return {"error": "Incorrect password. Please try again."}, 401
    
    is_exempt = bool(user.get("can_rename")) or (u in {"karthik", "locke"})
    existing_session = user.get("session_token")
    
    # Enforce single browser for non-exempt users
    if not is_exempt and existing_session and not force_logout:
        return {
            "error": "Account is currently logged in on another device/browser.",
            "require_force": True,
            "active_session": True
        }, 409
    
    new_token = uuid.uuid4().hex
    now_z = dt.datetime.now(dt.timezone.utc).isoformat()
    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    if sub_url and sub_key:
        payload = {"session_token": new_token, "session_updated_at": now_z}
        requests.patch(f"{sub_url}/rest/v1/app_users?username=eq.{u}", json=payload, headers=_supabase_headers(), timeout=5)
    
    return {
        "ok": True,
        "username": u,
        "session_token": new_token,
        "is_admin": bool(user.get("is_admin")),
        "can_rename": is_exempt
    }, 200


def _db_validate_session(username: str, token: str) -> bool:
    u = (username or "").strip().lower()
    if not u or not token:
        return False
    user = _db_get_user(u)
    if not user:
        return False
    is_exempt = bool(user.get("can_rename")) or (u in {"karthik", "locke"})
    if is_exempt:
        # Karthik and Locke can be logged in from any number of browsers simultaneously
        return True
    db_token = user.get("session_token")
    return bool(db_token and secrets.compare_digest(db_token, token))


def _db_rename_user(old_name: str, new_name: str):
    old_u = old_name.strip().lower()
    new_u = new_name.strip().lower()
    if not re.fullmatch(r"[a-z0-9_]{2,20}", new_u):
        return {"error": "Username must be 2-20 lowercase letters, numbers, or underscore."}, 400
    
    user = _db_get_user(old_u)
    if not user or not (user.get("can_rename") or old_u in {"karthik", "locke"}):
        return {"error": "Permission denied: Only Karthik and Locke are permitted to change username."}, 403
    
    if old_u == new_u:
        return {"ok": True, "new_username": new_u}, 200
    
    existing = _db_get_user(new_u)
    if existing:
        return {"error": f"Username '{new_u}' is already taken. Please choose another."}, 409
    
    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    if sub_url and sub_key:
        h = _supabase_headers(prefer="return=representation")
        r1 = requests.patch(f"{sub_url}/rest/v1/app_users?username=eq.{old_u}", json={"username": new_u}, headers=h, timeout=10)
        if r1.status_code not in (200, 204):
            return {"error": f"Failed to update database: {r1.text}"}, 500
        
        # Transfer search history in searches table
        requests.patch(f"{sub_url}/rest/v1/searches?user_name=eq.{old_u.upper()}", json={"user_name": new_u.upper()}, headers=h, timeout=10)
    
    global ALLOWED_USERS, ADMIN_USERS
    ALLOWED_USERS.discard(old_u)
    ALLOWED_USERS.add(new_u)
    if old_u in ADMIN_USERS:
        ADMIN_USERS.discard(old_u)
        ADMIN_USERS.add(new_u)
    
    return {"ok": True, "old_name": old_u, "new_name": new_u}, 200


_db_sync_users()


def log_search(user, ticker):
    if not user or not ticker:
        return
    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    if not (sub_url and sub_key):
        return

    now_dt = dt.datetime.now(IST)
    now_ist = now_dt.strftime("%Y-%m-%d %H:%M:%S")
    now_date = now_dt.strftime("%Y-%m-%d")

    try:
        endpoint = f"{sub_url}/rest/v1/searches"
        payload = {
            "user_name": user.upper(),
            "ticker": ticker,
            "time_ist": now_ist,
            "date_ist": now_date
        }
        r = requests.post(endpoint, json=payload, headers=_supabase_headers(), timeout=5)
        if r.status_code not in (200, 201):
            requests.post(endpoint, json={"user_name": user.upper(), "ticker": ticker, "time_ist": now_ist}, headers=_supabase_headers(), timeout=5)
    except Exception:
        pass


def log_keyword_search(user, keyword, start_date, end_date, forms):
    """Record a successful SEC keyword search for the admin activity audit."""
    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    if not (user and keyword and sub_url and sub_key):
        return
    now_dt = dt.datetime.now(IST)
    payload = {
        "user_name": user.upper(),
        "keyword": keyword[:120],
        "start_date": start_date,
        "end_date": end_date,
        "forms": forms or None,
        "time_ist": now_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "date_ist": now_dt.strftime("%Y-%m-%d"),
    }
    try:
        requests.post(f"{sub_url}/rest/v1/keyword_searches", json=payload,
                      headers=_supabase_headers(), timeout=5)
    except Exception:
        app.logger.exception("Could not record keyword search activity")


@app.route("/api/auth/status")
def auth_status():
    u = (request.args.get("user") or "").strip().lower()
    if not u:
        return jsonify({"error": "Username required"}), 400
    user = _db_get_user(u)
    if not user:
        return jsonify({"error": "Username not authorized"}), 404
    is_exempt = bool(user.get("can_rename")) or (u in {"karthik", "locke"})
    has_pwd = bool(user.get("password_hash") and user.get("salt"))
    has_active = bool(user.get("session_token"))
    return jsonify({
        "exists": True,
        "username": u,
        "has_password": has_pwd,
        "is_admin": bool(user.get("is_admin")),
        "can_rename": is_exempt,
        "has_active_session": (has_active and not is_exempt)
    })


@app.route("/api/auth/set-password", methods=["POST"])
def auth_set_password():
    data = request.get_json(silent=True) or request.form
    u = (data.get("user") or "").strip().lower()
    pwd = (data.get("password") or "").strip()
    if not u or not pwd:
        return jsonify({"error": "Username and password required"}), 400
    user = _db_get_user(u)
    if not user:
        return jsonify({"error": "User not authorized"}), 404
    
    token = _db_set_password(u, pwd)
    is_exempt = bool(user.get("can_rename")) or (u in {"karthik", "locke"})
    return jsonify({
        "ok": True,
        "username": u,
        "session_token": token,
        "is_admin": bool(user.get("is_admin")),
        "can_rename": is_exempt
    })


@app.route("/api/auth/login", methods=["POST"])
def auth_login():
    data = request.get_json(silent=True) or request.form
    u = (data.get("user") or "").strip().lower()
    pwd = (data.get("password") or "").strip()
    force = bool(data.get("force_logout") or data.get("force"))
    res, status_code = _db_login(u, pwd, force_logout=force)
    return jsonify(res), status_code


@app.route("/api/auth/verify-session")
def auth_verify_session():
    u = (request.args.get("user") or "").strip().lower()
    token = (request.args.get("token") or "").strip()
    if not u or not token:
        return jsonify({"ok": False, "error": "Missing user or token"}), 400
    ok = _db_validate_session(u, token)
    if not ok:
        return jsonify({"ok": False, "error": "Session terminated: This account logged in from another browser."}), 401
    user = _db_get_user(u)
    is_exempt = bool(user and user.get("can_rename")) or (u in {"karthik", "locke"})
    return jsonify({"ok": True, "can_rename": is_exempt, "is_admin": bool(user and user.get("is_admin"))})


@app.route("/api/auth/rename-user", methods=["POST"])
def auth_rename_user():
    data = request.get_json(silent=True) or request.form
    old_u = (data.get("old_name") or data.get("user") or "").strip().lower()
    new_u = (data.get("new_name") or "").strip().lower()
    token = (data.get("token") or "").strip()
    if not old_u or not new_u:
        return jsonify({"error": "Old and new usernames required"}), 400
    if token and not _db_validate_session(old_u, token):
        return jsonify({"error": "Invalid session"}), 401
    res, code = _db_rename_user(old_u, new_u)
    return jsonify(res), code


@app.route("/api/start")
def start():
    u = (request.args.get("user") or "").strip().lower()
    token = (request.args.get("token") or "").strip()
    if not u or u not in ALLOWED_USERS:
        return jsonify({"error": "Access denied"}), 401
    if token and not _db_validate_session(u, token):
        return jsonify({"error": "Session expired: Logged in on another browser.", "code": "SESSION_REVOKED"}), 401
    t = (request.args.get("ticker") or "").strip().upper()
    if not re.fullmatch(r"[A-Z.\-]{1,10}", t):
        return jsonify({"error": "Invalid ticker"}), 400
    log_search(u, t)
    jid = uuid.uuid4().hex[:10]
    JOBS[jid] = {"status": "running", "msg": "Starting…"}
    threading.Thread(target=run_job, args=(JOBS[jid], t, request.args.get("refresh") == "1"), daemon=True).start()
    return jsonify({"job": jid})


@app.route("/api/admin/searches")
def admin_searches():
    u = (request.args.get("user") or "").strip().lower()
    pwd = (request.args.get("pwd") or "").strip()
    if u not in ADMIN_USERS or pwd != "500":
        return jsonify({"error": "Unauthorized"}), 401

    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    data = []
    err_info = None
    if sub_url and sub_key:
        try:
            endpoint = f"{sub_url}/rest/v1/searches?select=user_name,ticker,time_ist,date_ist&order=id.desc&limit=1000"
            r = requests.get(endpoint, headers=_supabase_headers(), timeout=10)
            if r.status_code == 200:
                rows = r.json()
                if isinstance(rows, list):
                    data = [{"user": (x.get("user_name") or x.get("user") or "").upper(),
                             "ticker": x.get("ticker"), "type": "ticker",
                             "time": x.get("time_ist") or x.get("time"),
                             "date": x.get("date_ist")} for x in rows]
            else:
                err_info = f"Supabase HTTP {r.status_code}: {r.text[:200]}"
            try:
                keyword_endpoint = f"{sub_url}/rest/v1/keyword_searches?select=user_name,keyword,start_date,end_date,forms,time_ist,date_ist&order=id.desc&limit=1000"
                keyword_response = requests.get(keyword_endpoint, headers=_supabase_headers(), timeout=10)
                if keyword_response.status_code == 200:
                    keyword_rows = keyword_response.json()
                    if isinstance(keyword_rows, list):
                        data.extend({"user": (x.get("user_name") or "").upper(),
                                     "ticker": x.get("keyword"), "keyword": x.get("keyword"),
                                     "type": "keyword", "start_date": x.get("start_date"),
                                     "end_date": x.get("end_date"), "forms": x.get("forms"),
                                     "time": x.get("time_ist"), "date": x.get("date_ist")}
                                    for x in keyword_rows)
                        data.sort(key=lambda x: x.get("time") or "", reverse=True)
                elif not err_info:
                    err_info = ("Keyword search audit table unavailable. Apply the migration in "
                                "supabase_keyword_searches.sql. "
                                f"Supabase HTTP {keyword_response.status_code}.")
            except Exception as ex:
                if not err_info:
                    err_info = f"Could not load keyword search history: {ex}"
        except Exception as ex:
            err_info = str(ex)
    else:
        err_info = "SUPABASE_URL or SUPABASE_KEY not configured on server"

    resp = {"searches": data}
    if err_info:
        resp["_warn"] = err_info
    return jsonify(resp)


@app.route("/api/admin/clear-old-searches", methods=["POST", "DELETE", "GET"])
def clear_old_searches():
    u = (request.args.get("user") or request.form.get("user") or "").strip().lower()
    pwd = (request.args.get("pwd") or request.form.get("pwd") or "").strip()
    if u not in ADMIN_USERS or pwd != "500":
        return jsonify({"error": "Unauthorized"}), 401

    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    if not (sub_url and sub_key):
        return jsonify({"error": "Supabase not configured on server"}), 500

    now_ist = dt.datetime.now(IST)
    cutoff_dt = now_ist - dt.timedelta(days=14)
    cutoff_date = cutoff_dt.strftime("%Y-%m-%d")
    cutoff_z = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%SZ")

    deleted_count = 0
    try:
        h = _supabase_headers(prefer="return=representation")

        # Delete records older than 14 days by date_ist
        r1 = requests.delete(f"{sub_url}/rest/v1/searches?date_ist=lt.{cutoff_date}", headers=h, timeout=15)
        if r1.status_code in (200, 204):
            data1 = r1.json() if r1.text else []
            deleted_count += len(data1) if isinstance(data1, list) else 0

        # Delete any rows where created_at is older than 14 days
        r2 = requests.delete(f"{sub_url}/rest/v1/searches?created_at=lt.{cutoff_z}", headers=h, timeout=15)
        if r2.status_code in (200, 204):
            data2 = r2.json() if r2.text else []
            deleted_count += len(data2) if isinstance(data2, list) else 0

        r3 = requests.delete(f"{sub_url}/rest/v1/keyword_searches?date_ist=lt.{cutoff_date}", headers=h, timeout=15)
        if r3.status_code in (200, 204):
            data3 = r3.json() if r3.text else []
            deleted_count += len(data3) if isinstance(data3, list) else 0

        return jsonify({
            "ok": True,
            "deleted": deleted_count,
            "cutoff": cutoff_date,
            "message": f"Successfully deleted {deleted_count} search record(s) older than 2 weeks ({cutoff_date})."
        })
    except Exception as ex:
        return jsonify({"error": f"Failed to delete old records: {ex}"}), 500


DEFAULT_SCANNER_SETTINGS = {
    "version": 4,
    "voice_enabled": False,
    "voice_uri": "",
    "approach_pct": 2.0,
    "vwap_enabled": True,
}
_scanner_user_settings_lock = threading.RLock()
_scanner_user_settings_file = Path(__file__).resolve().parent / ".scanner_user_settings.json"
_APP_UI_VISIBILITY_KEY = "app_ui_visibility"
_APP_UI_VISIBILITY_USER_KEY = "app_ui_visibility_user"
_APP_UI_VISIBILITY_DEFAULT_KEY = "app_ui_visibility_default"
_APP_UI_VISIBILITY_DEFAULTS = {"scanner": False, "runner_stats": True}
_app_ui_visibility_file = Path(__file__).resolve().parent / ".app_ui_visibility.json"
_app_ui_user_visibility_file = Path(__file__).resolve().parent / ".app_ui_user_visibility.json"
_app_ui_visibility_lock = threading.RLock()


def _normalize_scanner_settings(value):
    source = value if isinstance(value, dict) else {}
    try:
        version = int(source.get("version", 0))
    except (TypeError, ValueError):
        version = 0
    try:
        approach_pct = float(source.get("approach_pct", DEFAULT_SCANNER_SETTINGS["approach_pct"]))
    except (TypeError, ValueError):
        approach_pct = DEFAULT_SCANNER_SETTINGS["approach_pct"]
    return {
        "version": 4,
        "voice_enabled": False if version < 4 else bool(source.get("voice_enabled", DEFAULT_SCANNER_SETTINGS["voice_enabled"])),
        "voice_uri": str(source.get("voice_uri") or "")[:300],
        "approach_pct": round(max(0.1, min(10.0, approach_pct)), 2),
        "vwap_enabled": bool(source.get("vwap_enabled", DEFAULT_SCANNER_SETTINGS["vwap_enabled"])),
    }


def _read_local_scanner_settings():
    try:
        with _scanner_user_settings_lock:
            data = json.loads(_scanner_user_settings_file.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_local_scanner_settings(username, settings):
    with _scanner_user_settings_lock:
        data = _read_local_scanner_settings()
        data[username] = settings
        tmp = _scanner_user_settings_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(_scanner_user_settings_file)


def _normalize_app_ui_visibility(value):
    source = value if isinstance(value, dict) else {}
    return {
        "scanner": False,
        "runner_stats": source.get("runner_stats") is not False,
    }


def _parse_admin_scanner_settings(value):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            value = {}
    return value if isinstance(value, dict) else {}


def _read_local_app_ui_visibility():
    try:
        with _app_ui_visibility_lock:
            value = json.loads(_app_ui_visibility_file.read_text(encoding="utf-8"))
        return _normalize_app_ui_visibility(value)
    except Exception:
        return None


def _write_local_app_ui_visibility(visibility):
    with _app_ui_visibility_lock:
        temp_path = _app_ui_visibility_file.with_suffix(".tmp")
        temp_path.write_text(json.dumps(visibility, indent=2), encoding="utf-8")
        temp_path.replace(_app_ui_visibility_file)


def _read_local_user_ui_visibility(username):
    try:
        with _app_ui_visibility_lock:
            saved = json.loads(_app_ui_user_visibility_file.read_text(encoding="utf-8"))
        value = saved.get(username) if isinstance(saved, dict) else None
        return _normalize_app_ui_visibility(value) if isinstance(value, dict) else None
    except Exception:
        return None


def _write_local_user_ui_visibility(username, visibility):
    with _app_ui_visibility_lock:
        try:
            saved = json.loads(_app_ui_user_visibility_file.read_text(encoding="utf-8"))
            if not isinstance(saved, dict):
                saved = {}
        except Exception:
            saved = {}
        saved[username] = _normalize_app_ui_visibility(visibility)
        temp_path = _app_ui_user_visibility_file.with_suffix(".tmp")
        temp_path.write_text(json.dumps(saved, indent=2), encoding="utf-8")
        temp_path.replace(_app_ui_user_visibility_file)


def _get_app_ui_visibility(username=None):
    target = (username or "").strip().lower()
    if target:
        local_user = _read_local_user_ui_visibility(target)
        if local_user is not None:
            return local_user
    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    if sub_url and sub_key:
        try:
            response = requests.get(
                f"{sub_url}/rest/v1/app_users?select=username,scanner_settings,is_admin,can_rename",
                headers=_supabase_headers(), timeout=5
            )
            if response.status_code == 200:
                rows = response.json()
                row = next((item for item in rows if (item.get("username") or "").lower() == target), None)
                if row:
                    saved = _parse_admin_scanner_settings(row.get("scanner_settings")).get(_APP_UI_VISIBILITY_USER_KEY)
                    if isinstance(saved, dict):
                        return _normalize_app_ui_visibility(saved)
                admins = [item for item in rows if item.get("is_admin") or item.get("can_rename")]
                admins.sort(key=lambda item: ((item.get("username") or "").lower() not in ("karthik", "locke"),
                                             (item.get("username") or "").lower()))
                for admin_row in admins:
                    admin_settings = _parse_admin_scanner_settings(admin_row.get("scanner_settings"))
                    saved = admin_settings.get(_APP_UI_VISIBILITY_DEFAULT_KEY)
                    if not isinstance(saved, dict):  # retain settings saved by older versions
                        saved = admin_settings.get(_APP_UI_VISIBILITY_KEY)
                    if isinstance(saved, dict):
                        return _normalize_app_ui_visibility(saved)
        except Exception:
            pass
    return _read_local_app_ui_visibility() or dict(_APP_UI_VISIBILITY_DEFAULTS)


def _save_app_ui_visibility(visibility):
    clean = _normalize_app_ui_visibility(visibility)
    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    if sub_url and sub_key:
        response = requests.get(
            f"{sub_url}/rest/v1/app_users?select=username,scanner_settings,is_admin,can_rename",
            headers=_supabase_headers(), timeout=5
        )
        if response.status_code != 200:
            raise RuntimeError(f"Could not load admin settings (HTTP {response.status_code})")
        rows = response.json()
        admins = [row for row in rows if row.get("is_admin") or row.get("can_rename")]
        if not admins:
            raise RuntimeError("No admin accounts are available to store app visibility settings")
        for row in admins:
            username = (row.get("username") or "").strip().lower()
            if not username:
                continue
            settings = _parse_admin_scanner_settings(row.get("scanner_settings"))
            settings[_APP_UI_VISIBILITY_DEFAULT_KEY] = clean
            result = requests.patch(
                f"{sub_url}/rest/v1/app_users?username=eq.{quote(username, safe='')}",
                json={"scanner_settings": settings}, headers=_supabase_headers(), timeout=6
            )
            if result.status_code not in (200, 204):
                raise RuntimeError(f"Could not save visibility settings for {username} "
                                   f"(HTTP {result.status_code})")
        try:
            _write_local_app_ui_visibility(clean)
        except Exception:
            pass
        return clean, "Supabase"
    _write_local_app_ui_visibility(clean)
    return clean, "local fallback"


def _get_user_scanner_settings(username):
    record = _db_get_user(username) or {}
    remote = record.get("scanner_settings")
    if isinstance(remote, dict) and remote:
        return _normalize_scanner_settings(remote), True, "Supabase"
    local = _read_local_scanner_settings().get(username)
    if isinstance(local, dict):
        return _normalize_scanner_settings(local), True, "local fallback"
    return _normalize_scanner_settings(DEFAULT_SCANNER_SETTINGS), False, "default"


def _save_user_scanner_settings(username, settings):
    clean_settings = _normalize_scanner_settings(settings)
    existing = _db_get_user(username) or {}
    preserved = _parse_admin_scanner_settings(existing.get("scanner_settings"))
    for visibility_key in (_APP_UI_VISIBILITY_KEY, _APP_UI_VISIBILITY_USER_KEY, _APP_UI_VISIBILITY_DEFAULT_KEY,
                           "runner_csv_collections"):
        if isinstance(preserved.get(visibility_key), dict):
            clean_settings[visibility_key] = preserved[visibility_key]
        elif visibility_key == "runner_csv_collections" and isinstance(preserved.get(visibility_key), list):
            clean_settings[visibility_key] = preserved[visibility_key]
    local_error = None
    try:
        _write_local_scanner_settings(username, clean_settings)
    except Exception as exc:
        local_error = str(exc)

    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    if sub_url and sub_key:
        try:
            response = requests.patch(
                f"{sub_url}/rest/v1/app_users?username=eq.{quote(username, safe='')}",
                json={"scanner_settings": clean_settings}, headers=_supabase_headers(), timeout=6
            )
            if response.status_code in (200, 204):
                return clean_settings, "Supabase"
        except Exception:
            pass
    if local_error:
        raise RuntimeError(f"Could not save scanner preferences: {local_error}")
    return clean_settings, "local fallback"


@app.route("/api/ui-visibility")
def app_ui_visibility():
    username = (request.args.get("user") or "").strip().lower()
    token = (request.args.get("token") or "").strip()
    if not username or not token or not _db_validate_session(username, token):
        return jsonify({"error": "Unauthorized"}), 401
    return jsonify(_get_app_ui_visibility(username))


@app.route("/api/admin/users/<target>/ui-visibility", methods=["GET", "POST"])
def admin_user_ui_visibility(target):
    admin = (request.args.get("user") or (request.get_json(silent=True) or {}).get("user") or "").strip().lower()
    token = (request.args.get("token") or (request.get_json(silent=True) or {}).get("token") or "").strip()
    password = (request.args.get("pwd") or (request.get_json(silent=True) or {}).get("pwd") or "").strip()
    if admin not in {"karthik", "locke"} or password != "500" or not token or not _db_validate_session(admin, token):
        return jsonify({"error": "Unauthorized"}), 401
    target = (target or "").strip().lower()
    if not re.fullmatch(r"[a-z0-9_]{2,20}", target) or not _db_get_user(target):
        return jsonify({"error": "User not found"}), 404
    if request.method == "GET":
        return jsonify({"visibility": _get_app_ui_visibility(target)})
    body = request.get_json(silent=True) or {}
    visibility = body.get("visibility")
    if not isinstance(visibility, dict) or any(type(visibility.get(key)) is not bool for key in ("scanner", "runner_stats")):
        return jsonify({"error": "Both visibility options must be true or false."}), 400
    record = _db_get_user(target) or {}
    settings = _parse_admin_scanner_settings(record.get("scanner_settings"))
    settings[_APP_UI_VISIBILITY_USER_KEY] = _normalize_app_ui_visibility(visibility)
    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    if sub_url:
        result = requests.patch(f"{sub_url}/rest/v1/app_users?username=eq.{quote(target, safe='')}",
                                json={"scanner_settings": settings}, headers=_supabase_headers(), timeout=6)
        if result.status_code not in (200, 204):
            return jsonify({"error": f"Could not save access settings (HTTP {result.status_code})"}), 500
    else:
        try:
            _write_local_user_ui_visibility(target, settings[_APP_UI_VISIBILITY_USER_KEY])
        except Exception as exc:
            return jsonify({"error": f"Could not save access settings: {exc}"}), 500
    return jsonify({"ok": True, "visibility": settings[_APP_UI_VISIBILITY_USER_KEY]})


@app.route("/api/admin/ui-visibility", methods=["GET", "POST"])
def admin_app_ui_visibility():
    body = request.get_json(silent=True) or {}
    username = (request.args.get("user") or body.get("user") or "").strip().lower()
    token = (request.args.get("token") or body.get("token") or "").strip()
    password = (request.args.get("pwd") or body.get("pwd") or "").strip()
    if (username not in ADMIN_USERS or password != "500" or not token or
            not _db_validate_session(username, token)):
        return jsonify({"error": "Unauthorized"}), 401
    if request.method == "GET":
        return jsonify({"visibility": _get_app_ui_visibility(), "storage": "Supabase or local fallback"})

    requested = body.get("visibility")
    if (not isinstance(requested, dict) or type(requested.get("scanner")) is not bool or
            type(requested.get("runner_stats")) is not bool):
        return jsonify({"error": "Both visibility options must be true or false."}), 400
    try:
        visibility, storage = _save_app_ui_visibility(requested)
        return jsonify({"ok": True, "visibility": visibility, "storage": storage})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/scanner/settings", methods=["GET", "POST"])
def scanner_user_settings():
    body = request.get_json(silent=True) or {}
    username = (request.args.get("user") or body.get("user") or "").strip().lower()
    token = (request.args.get("token") or body.get("token") or "").strip()
    if not username or not token or not _db_validate_session(username, token):
        return jsonify({"error": "Session expired. Please sign in again."}), 401
    if request.method == "GET":
        settings, configured, storage = _get_user_scanner_settings(username)
        return jsonify({"settings": settings, "configured": configured, "storage": storage})
    try:
        settings, storage = _save_user_scanner_settings(username, body.get("settings"))
        return jsonify({"ok": True, "settings": settings, "storage": storage})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/admin/users")
def admin_get_users():
    u = (request.args.get("user") or "").strip().lower()
    pwd = (request.args.get("pwd") or "").strip()
    if u not in ADMIN_USERS or pwd != "500":
        return jsonify({"error": "Unauthorized"}), 401
    _db_sync_users()
    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    users_data = []
    if sub_url and sub_key:
        try:
            r = requests.get(f"{sub_url}/rest/v1/app_users?select=username,is_admin,can_rename", headers=_supabase_headers(), timeout=5)
            if r.status_code == 200:
                users_data = r.json()
        except Exception:
            pass
    u_list = [x["username"] for x in users_data] if users_data else sorted(list(ALLOWED_USERS))
    admins = [x["username"] for x in users_data if x.get("is_admin") or x.get("can_rename")] if users_data else sorted(list(ADMIN_USERS))
    can_rename = [x["username"] for x in users_data if x.get("can_rename")] if users_data else ["karthik", "locke"]
    return jsonify({"users": sorted(u_list), "admins": sorted(admins), "can_rename": sorted(can_rename)})


@app.route("/api/admin/users/<target>/details")
def admin_user_details(target):
    admin = (request.args.get("user") or "").strip().lower()
    pwd = (request.args.get("pwd") or "").strip()
    if admin not in ADMIN_USERS or pwd != "500":
        return jsonify({"error": "Unauthorized"}), 401
    target = (target or "").strip().lower()
    if not re.fullmatch(r"[a-z0-9_]{2,20}", target):
        return jsonify({"error": "Invalid username"}), 400
    record = _db_get_user(target) or {}
    if target not in ALLOWED_USERS and not record:
        return jsonify({"error": "User not found"}), 404

    profile_fields = ("username", "is_admin", "can_rename", "created_at", "updated_at", "session_updated_at")
    profile = {key: record[key] for key in profile_fields if key in record}
    profile.setdefault("username", target)
    profile["has_password"] = bool(record.get("password_hash") and record.get("salt"))
    profile["active_session"] = bool(record.get("session_token"))

    searches = []
    search_error = None
    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    if sub_url and sub_key:
        try:
            offset = 0
            while offset < 10000:
                endpoint = (f"{sub_url}/rest/v1/searches?user_name=eq.{quote(target, safe='')}"
                            f"&select=ticker,time_ist,date_ist&order=id.desc&limit=1000&offset={offset}")
                response = requests.get(endpoint, headers=_supabase_headers(), timeout=8)
                if response.status_code != 200:
                    search_error = f"Search history unavailable (HTTP {response.status_code})."
                    break
                batch = response.json() if isinstance(response.json(), list) else []
                searches.extend(batch)
                if len(batch) < 1000:
                    break
                offset += len(batch)
        except Exception as exc:
            search_error = f"Search history unavailable: {exc}"
    else:
        search_error = "Supabase is not configured; search history is unavailable."

    return jsonify({"profile": profile,
                    "ui_visibility": _get_app_ui_visibility(target),
                    "recent_searches": searches, "search_error": search_error})


@app.route("/api/admin/users/add", methods=["POST"])
def admin_add_user():
    u = (request.args.get("user") or request.form.get("user") or "").strip().lower()
    pwd = (request.args.get("pwd") or request.form.get("pwd") or "").strip()
    if u not in ADMIN_USERS or pwd != "500":
        return jsonify({"error": "Unauthorized"}), 401
    new_user = (request.args.get("name") or request.form.get("name") or "").strip().lower()
    if not new_user or not re.fullmatch(r"[a-z0-9_]{2,20}", new_user):
        return jsonify({"error": "Invalid username (2-20 lowercase letters/digits/underscore)"}), 400
    if new_user in ALLOWED_USERS:
        return jsonify({"error": f"User '{new_user}' already exists"}), 409
    
    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    if sub_url and sub_key:
        requests.post(f"{sub_url}/rest/v1/app_users", json={"username": new_user, "is_admin": False, "can_rename": False}, headers=_supabase_headers(), timeout=5)
    
    ALLOWED_USERS.add(new_user)
    return jsonify({"ok": True, "added": new_user, "users": sorted(list(ALLOWED_USERS))})


@app.route("/api/admin/users/remove", methods=["POST"])
def admin_remove_user():
    u = (request.args.get("user") or request.form.get("user") or "").strip().lower()
    pwd = (request.args.get("pwd") or request.form.get("pwd") or "").strip()
    if u not in ADMIN_USERS or pwd != "500":
        return jsonify({"error": "Unauthorized"}), 401
    target = (request.args.get("name") or request.form.get("name") or "").strip().lower()
    user_row = _db_get_user(target)
    if user_row and (user_row.get("can_rename") or user_row.get("is_admin")):
        return jsonify({"error": f"Cannot remove admin user '{target}'"}), 403
    if target not in ALLOWED_USERS:
        return jsonify({"error": f"User '{target}' not found"}), 404
    
    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    if sub_url and sub_key:
        requests.delete(f"{sub_url}/rest/v1/app_users?username=eq.{target}", headers=_supabase_headers(), timeout=5)
    
    ALLOWED_USERS.discard(target)
    return jsonify({"ok": True, "removed": target, "users": sorted(list(ALLOWED_USERS))})


@app.route("/api/job/<jid>")
def job(jid):
    j = JOBS.get(jid)
    if not j:
        return jsonify({"status": "error", "error": "Unknown job"}), 404
    return jsonify(j)


@app.route("/api/session-chart")
def session_chart():
    """Load any trading session on demand so chart arrows can visit non-gap-up days."""
    user = (request.args.get("user") or "").strip().lower()
    token = (request.args.get("token") or "").strip()
    ticker = (request.args.get("ticker") or "").strip().upper()
    day = (request.args.get("date") or "").strip()
    if user not in ALLOWED_USERS or not _db_validate_session(user, token):
        return jsonify({"error": "Unauthorized"}), 401
    if not re.fullmatch(r"[A-Z.\-]{1,10}", ticker):
        return jsonify({"error": "Invalid ticker"}), 400
    try:
        dt.date.fromisoformat(day)
    except ValueError:
        return jsonify({"error": "Invalid session date"}), 400
    try:
        daily = load_daily(ticker)
        index = next((i for i, row in enumerate(daily) if row["date"] == day), None)
        if index is None:
            daily = load_daily(ticker, force=True)
            index = next((i for i, row in enumerate(daily) if row["date"] == day), None)
        if index is None or index == 0:
            return jsonify({"error": f"No daily session data for {day}"}), 404
        row, pc = daily[index], float(daily[index - 1]["c"])
        df = minute_df(ticker, day)
        if df is None or df.empty:
            return jsonify({"error": f"No intraday bars are available for {day}"}), 404
        m = metrics(df, pc, True)
        gap = (m.get("open") / pc - 1) if m.get("open") and pc else None
        o2c = (m.get("close") / m.get("open") - 1) if m.get("close") and m.get("open") else None
        title = f"{day}  gap {pct(gap, 0)}  O→C {pct(o2c, 0)}"
        return jsonify({"date": day, "title": title, "sim": False,
                        "chart": chart_pack(df, m, pc, dt.date.fromisoformat(day))})
    except Exception as exc:
        return jsonify({"error": f"{type(exc).__name__}: {exc}"}), 502


@app.route("/api/chart-context")
def chart_context():
    """Lazily load nearby daily or multi-session minute bars for a chart card."""
    user=(request.args.get("user") or "").strip().lower(); token=(request.args.get("token") or "").strip()
    ticker=(request.args.get("ticker") or "").strip().upper(); day=(request.args.get("date") or "").strip()
    tf=(request.args.get("tf") or "15m").strip().lower()
    try: count=int(request.args.get("days", "5"))
    except ValueError: count=5
    if user not in ALLOWED_USERS or not _db_validate_session(user,token): return jsonify({"error":"Unauthorized"}),401
    if not re.fullmatch(r"[A-Z.\-]{1,10}",ticker): return jsonify({"error":"Invalid ticker"}),400
    try: dt.date.fromisoformat(day)
    except ValueError: return jsonify({"error":"Invalid session date"}),400
    if tf not in ("1m","2m","5m","15m","30m","1h","1d"): return jsonify({"error":"Invalid timeframe"}),400
    count=max(1,min(60,count))
    try:
        daily=load_daily(ticker); idx=next((i for i,r in enumerate(daily) if r["date"]==day),None)
        live=None
        if day==dt.date.today().isoformat():
            try:
                candidate=live_snapshot(ticker)
                live_day=candidate["date"].isoformat() if hasattr(candidate["date"],"isoformat") else str(candidate["date"])
                if live_day==day:
                    live=candidate; live_df=live["df"]
                    regular=live_df[(live_df.index.hour*60+live_df.index.minute>=RG_START)&
                                    (live_df.index.hour*60+live_df.index.minute<RG_END)]
                    today_row={"date":day,"o":float(regular["o"].iloc[0]) if not regular.empty else None,
                               "h":float(live_df["h"].max()) if not live_df.empty else None,
                               "l":float(live_df["l"].min()) if not live_df.empty else None,
                               "c":float(live_df["c"].iloc[-1]) if not live_df.empty else None,
                               "v":float(live_df["v"].sum()) if not live_df.empty else None,
                               "vw":float(live_df["vw"].iloc[-1]) if not live_df.empty and "vw" in live_df else None}
                    daily=[row for row in daily if row["date"]<day]+[today_row]
                    idx=len(daily)-1
            except Exception:
                live=None
        if idx is None: return jsonify({"error":f"No daily data for {day}"}),404
        # Keep the requested number of sessions even when the anchor is the
        # newest (or oldest) available session.
        start=max(0,min(idx-count//2,max(0,len(daily)-count))); end=min(len(daily),start+count); rows=daily[start:end]
        daily_metrics={}
        for pos,row in enumerate(rows):
            daily_index=start+pos
            previous=daily[daily_index-1] if daily_index>0 else None
            previous_close=float(previous.get("c") or 0) if previous else 0
            open_price=float(row.get("o") or 0)
            daily_metrics[row["date"]]={
                "gap_pct":(open_price/previous_close-1) if open_price>0 and previous_close>0 else None,
                "dollar_volume":float(row.get("vw") or row.get("c") or 0)*float(row.get("v") or 0),
            }
        if tf=="1d":
            bars={"t":list(range(len(rows))),"d":[r["date"] for r in rows],"o":[r["o"] for r in rows],"h":[r["h"] for r in rows],"l":[r["l"] for r in rows],"c":[r["c"] for r in rows],"v":[r["v"] for r in rows],"vw":[None]*len(rows)}
            return jsonify({"bars":bars,"dates":[r["date"] for r in rows],"daily":True,"metrics":daily_metrics})
        startday,endday=rows[0]["date"],rows[-1]["date"]
        key=f"{ticker}_{startday}_{endday}_context_1m"; raw=cget(key)
        if raw is None:
            raw=(poly_get(f"/v2/aggs/ticker/{ticker}/range/1/minute/{startday}/{endday}",{"adjusted":"false","sort":"asc","limit":50000}).get("results") or [])
            cput(key,raw)
        frame=poly_frame(raw) if raw else pd.DataFrame()
        chunks=[]
        for date in [r["date"] for r in rows]:
            if live is not None and date==day:
                dayframe=live["df"]
            elif not frame.empty:
                dayframe=frame[frame.index.date==dt.date.fromisoformat(date)]
            else:
                continue
            packed=bars2(dayframe)
            if packed:
                packed["d"]=[date]*len(packed["t"]); chunks.append(packed)
        if not chunks: return jsonify({"error":"No intraday bars are available for this range"}),404
        bars={k:sum((p[k] for p in chunks),[]) for k in ("t","d","o","h","l","c","v","vw")}
        return jsonify({"bars":bars,"dates":[p["d"][0] for p in chunks],"daily":False,"metrics":daily_metrics})
    except Exception as exc: return jsonify({"error":f"{type(exc).__name__}: {exc}"}),502


_KW_FINANCING_FORMS = ("8-K,6-K,S-1,S-3,S-4,F-1,F-3,F-4,8-A,424B1,424B2,424B3,424B4,424B5,424B6,424B7,10-K,10-Q,20-F,40-F")
_kw_sec_request_lock = threading.Lock()
_kw_sec_last_request = 0.0


def _keyword_sec_get(url, params=None, timeout=25):
    """Serialize keyword-search SEC traffic to stay comfortably below 10 req/s."""
    global _kw_sec_last_request
    with _kw_sec_request_lock:
        wait = 0.12 - (time.monotonic() - _kw_sec_last_request)
        if wait > 0:
            time.sleep(wait)
        for attempt in range(3):
            try:
                response = requests.get(url, params=params,
                                        headers={"User-Agent": SEC_UA, "Accept-Encoding": "gzip, deflate"},
                                        timeout=timeout)
                _kw_sec_last_request = time.monotonic()
                if response.status_code not in (429, 503) or attempt == 2:
                    return response
            except requests.RequestException:
                if attempt == 2:
                    raise
                _kw_sec_last_request = time.monotonic()
            time.sleep(0.5 * (attempt + 1))
        return response
_KW_PLAYER_ROLES = [
    ("Legal counsel", r"\blegal\s+counsel|\bcounsel\s+to\b|\battorneys?\s+for\b"),
    ("Auditor", r"\bauditors?\b|\bindependent\s+accountants?\b"),
    ("Underwriter", r"\bunderwrit(?:er|ers|ing)\b"),
    ("Placement agent", r"\bplacement\s+agent\b"),
    ("Investment bank", r"\binvestment\s+banks?\b|\binvestment\s+bankers?\b"),
    ("Investor", r"\binvestors?\b"),
    ("Lender", r"\blender|loan\s+agreement\b"),
    ("Equity-line provider", r"\bequity\s+line|\bcommon\s+stock\s+purchase\s+agreement\b"),
    ("Convertible-note holder", r"\bconvertible\s+(?:note|notes|debenture)|\bnoteholder\b"),
    ("Warrant holder", r"\bwarrant\s+(?:holder|holders|exercise)|\bholder\s+of\s+the\s+warrants\b"),
    ("Selling shareholder", r"\bselling\s+stockholder|\bselling\s+shareholder\b"),
]


def _keyword_ticker_map():
    cached = cget("sec_keyword_ticker_map", 86400)
    if cached:
        return cached
    response = _keyword_sec_get("https://www.sec.gov/files/company_tickers.json", timeout=25)
    response.raise_for_status()
    raw = response.json()
    result = {}
    for row in raw.values():
        cik = str(row.get("cik_str") or "").strip()
        if cik:
            result[str(int(cik)).zfill(10)] = {"ticker": str(row.get("ticker") or "").upper(),
                                               "name": str(row.get("title") or "")}
    cput("sec_keyword_ticker_map", result)
    return result


def _keyword_plain_text(value):
    if isinstance(value, dict):
        return " ".join(_keyword_plain_text(v) for v in value.values())
    if isinstance(value, (list, tuple)):
        return " ".join(_keyword_plain_text(v) for v in value)
    if not isinstance(value, str):
        return ""
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", value)).strip()


def _keyword_match_excerpt(hit, keyword, fetch_document=False):
    source = hit.get("_source") or {}
    highlight = hit.get("highlight") or source.get("highlight") or {}
    highlighted = _keyword_plain_text(highlight)
    if highlighted:
        return highlighted[:700]
    if not fetch_document:
        return ""
    try:
        hit_id = str(hit.get("_id") or "")
        accession_match = re.search(r"\d{10}-\d{2}-\d{6}", hit_id)
        if not accession_match:
            return ""
        accession = accession_match.group(0)
        document = hit_id[accession_match.end():].lstrip(":/")
        ciks = source.get("ciks") or source.get("cik") or []
        cik_raw = ciks[0] if isinstance(ciks, list) and ciks else ciks
        cik = int(str(cik_raw).lstrip("0") or "0")
        if not re.fullmatch(r"[\w.\-]+\.(?:htm|html|txt|xml)", document, re.I):
            return ""
        accession_path = accession.replace("-", "")
        url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_path}/{quote(document, safe='.-_')}"
        response = _keyword_sec_get(url, timeout=15)
        response.raise_for_status()
        body = response.text[:8_000_000]
        import html as _html
        body = re.sub(r"(?is)<(script|style).*?</\1>", " ", body)
        body = re.sub(r"<[^>]+>", " ", body)
        plain = _html.unescape(re.sub(r"\s+", " ", body)).strip()
        pos = plain.casefold().find(keyword.casefold())
        if pos < 0:
            words = [w for w in re.findall(r"[\w.-]+", keyword) if len(w) > 2]
            pos = next((plain.casefold().find(w.casefold()) for w in words
                        if plain.casefold().find(w.casefold()) >= 0), -1)
        return ("…" + plain[max(0, pos-220):pos+len(keyword)+420] + "…") if pos >= 0 else ""
    except Exception:
        return ""


def _keyword_classify_role(excerpt):
    text = excerpt or ""
    for role, pattern in _KW_PLAYER_ROLES:
        if re.search(pattern, text, re.I):
            return role
    return "Unverified mention"


def _keyword_share_amount(excerpt):
    text = excerpt or ""
    m = re.search(r"\b(\d[\d,]*(?:\.\d+)?\s*(?:million|billion|thousand|[KMB])?)\s+(?:shares|ordinary shares|common shares)\b", text, re.I)
    if not m:
        return "—"
    return re.sub(r"\s+", " ", m.group(1)).strip()


def _keyword_headline(form, source, excerpt):
    text = f"{source.get('file_description') or ''} {excerpt}".casefold()
    for name, pattern in (("ATM", r"\batm\b|at.the.market"), ("Convertible note", r"convertible.{0,20}note"),
                          ("Registered direct offering", r"registered direct offering"), ("IPO", r"\binitial public offering\b"),
                          ("Private placement", r"private placement"), ("Registered offering", r"registered offering"),
                          ("Equity line", r"equity line|common stock purchase agreement")):
        if re.search(pattern, text, re.I):
            return name
    return str(source.get("file_description") or f"{form} filing")[:120]


def _sec_keyword_search(keyword, start_date, end_date, forms, offset=0, size=100):
    cache_key = "kw_" + hashlib.sha1(json.dumps([keyword, start_date, end_date, forms, offset, size]).encode()).hexdigest()
    cached = cget(cache_key, 3600)
    if cached is not None:
        return cached
    ticker_map = _keyword_ticker_map()
    # Commas let users require multiple names/terms in the same filing, e.g.
    # "MAXIM, BOUTIQUE". SEC EFTS accepts Lucene-style AND queries.
    terms = [part.strip() for part in keyword.split(",") if part.strip()]
    if len(terms) > 1:
        query = " AND ".join(f'"{term}"' for term in terms)
        queries = [query]
    elif len(keyword.split()) > 1:
        cleaned = " ".join(re.findall(r"[\w]+", keyword, re.UNICODE))
        queries = list(dict.fromkeys([f'"{keyword}"', f'"{cleaned}"', keyword]))
    else:
        queries = [keyword]
    response_data = None
    for query in queries:
        params = {"q": query, "dateRange": "custom", "startdt": start_date,
                  "enddt": end_date, "from": max(0, int(offset)), "size": min(100, max(1, int(size)))}
        if forms:
            params["forms"] = forms
        response = _keyword_sec_get("https://efts.sec.gov/LATEST/search-index", params=params, timeout=30)
        response.raise_for_status()
        response_data = response.json()
        total = (response_data.get("hits") or {}).get("total") or 0
        total_value = total.get("value", 0) if isinstance(total, dict) else total
        if total_value:
            break
    hits = ((response_data or {}).get("hits") or {}).get("hits") or []
    rows, seen = [], set()
    for n, hit in enumerate(hits):
        source = hit.get("_source") or {}
        hit_id = str(hit.get("_id") or "")
        accession_match = re.search(r"\d{10}-\d{2}-\d{6}", hit_id)
        accession = accession_match.group(0) if accession_match else hit_id.split(":", 1)[0]
        ciks = source.get("ciks") or source.get("cik") or []
        cik_raw = ciks[0] if isinstance(ciks, list) and ciks else ciks
        display_names = source.get("display_names") or []
        if not cik_raw:
            name_cik = re.search(r"\bCIK\s*0*(\d{1,10})\b", " ".join(map(str, display_names)), re.I)
            cik_raw = name_cik.group(1) if name_cik else ""
        try:
            cik_padded = str(int(str(cik_raw).lstrip("0") or "0")).zfill(10)
        except (TypeError, ValueError):
            cik_padded = ""
        dedup = (cik_padded, accession)
        if dedup in seen:
            continue
        seen.add(dedup)
        company = ticker_map.get(cik_padded) or {}
        entity_name = company.get("name") or (str(display_names[0]).split("  (")[0] if display_names else "Unknown filer")
        ticker = company.get("ticker") or ""
        form = str(source.get("form") or source.get("form_type") or "—")
        excerpt = _keyword_match_excerpt(hit, keyword, fetch_document=(n < 25))
        role = _keyword_classify_role(excerpt)
        filing_url = (f"https://www.sec.gov/Archives/edgar/data/{int(cik_padded)}/{accession.replace('-', '')}/{accession}-index.html"
                      if cik_padded else f"https://www.sec.gov/edgar/search/#/q={quote(accession, safe='')}")
        if ticker and role in ("Investor", "Lender", "Convertible-note holder", "Warrant holder"):
            investor = keyword
        else:
            investor = "—"
        bank = keyword if role in ("Underwriter", "Placement agent", "Investment bank", "Equity-line provider") else "—"
        rows.append({"ticker": ticker, "company": entity_name, "headline": _keyword_headline(form, source, excerpt),
                     "investors": investor, "bank": bank, "form": form,
                     "share_amount": _keyword_share_amount(excerpt), "share_category": "Unverified; inspect filing",
                     "filing_date": source.get("file_date") or source.get("display_date_filed") or "",
                     "cik": cik_padded, "accession": accession, "filing_url": filing_url,
                     "role": role, "excerpt": excerpt or "SEC full-text match; open the filing to inspect the surrounding context.",
                     "verification": "Context classified from filing excerpt" if excerpt else "Unverified mention"})
    total = (response_data.get("hits") or {}).get("total") or 0
    total = total.get("value", 0) if isinstance(total, dict) else total
    result = {"rows": rows, "total": int(total or 0), "from": int(offset), "size": int(size)}
    cput(cache_key, result)
    return result


@app.route("/api/keyword-search", methods=["POST"])
def api_keyword_search():
    data = request.get_json(silent=True) or {}
    user = str(data.get("user") or "").strip().lower()
    token = str(data.get("token") or "").strip()
    keyword = str(data.get("keyword") or "").strip()
    if user not in ALLOWED_USERS or not _db_validate_session(user, token):
        return jsonify({"error": "Session expired. Please sign in again."}), 401
    if not keyword or len(keyword) > 120:
        return jsonify({"error": "Enter a keyword or name up to 120 characters."}), 400
    try:
        start_date = dt.date.fromisoformat(str(data.get("start_date")))
        end_date = dt.date.fromisoformat(str(data.get("end_date")))
        if start_date > end_date or (end_date-start_date).days > 366:
            raise ValueError
    except (TypeError, ValueError):
        return jsonify({"error": "Choose a valid filing date range within one year."}), 400
    requested_forms = data.get("forms") or []
    allowed_forms = set(_KW_FINANCING_FORMS.split(","))
    forms = ",".join(form for form in requested_forms if form in allowed_forms) if isinstance(requested_forms, list) else ""
    try:
        result = _sec_keyword_search(keyword, start_date.isoformat(), end_date.isoformat(), forms,
                                     offset=int(data.get("offset") or 0), size=100)
        if int(data.get("offset") or 0) == 0:
            log_keyword_search(user, keyword, start_date.isoformat(), end_date.isoformat(), forms)
        return jsonify(result)
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else 502
        return jsonify({"error": f"SEC search returned HTTP {status}. Please retry shortly."}), 502
    except Exception as exc:
        app.logger.exception("SEC keyword search failed")
        return jsonify({"error": f"SEC search failed: {type(exc).__name__}: {exc}"}), 502


@app.route("/api/keyword-events")
def api_keyword_events():
    user = (request.args.get("user") or "").strip().lower()
    token = (request.args.get("token") or "").strip()
    ticker = (request.args.get("ticker") or "").strip().upper()
    if user not in ALLOWED_USERS or not _db_validate_session(user, token):
        return jsonify({"error": "Session expired. Please sign in again."}), 401
    if not re.fullmatch(r"[A-Z.\-]{1,10}", ticker):
        return jsonify({"error": "This filing has no valid public ticker to chart."}), 400
    try:
        start = dt.date.fromisoformat(request.args.get("start") or (dt.date.today()-dt.timedelta(days=365)).isoformat())
        end = dt.date.fromisoformat(request.args.get("end") or dt.date.today().isoformat())
        min_dollar_volume = 50_000_000
        require_gap = request.args.get("require_gap") == "1"
        min_gap_pct = float(request.args.get("gap_min_pct") or 10) if require_gap else 0.0
        if require_gap and (not math.isfinite(min_gap_pct) or min_gap_pct < 0 or min_gap_pct > 500):
            raise ValueError("Gap threshold must be between 0 and 500%.")
        max_events = 4
        metadata_only = request.args.get("metadata_only") == "1"
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid historical chart filters."}), 400
    if start > end or (end-start).days > 366:
        return jsonify({"error": "Choose a valid chart date range within one year."}), 400
    try:
        daily = load_daily(ticker)
        splits = load_splits(ticker)
        split_dates = {x["date"] for x in splits}
        def split_adjust_price(value, source_day, target_day):
            adjusted = float(value or 0)
            for split in splits:
                split_day = split.get("date")
                if not split_day: continue
                ratio = float(split.get("to") or 0) / float(split.get("from") or 1)
                if ratio <= 0: continue
                if source_day < split_day <= target_day: adjusted /= ratio
                elif target_day < split_day <= source_day: adjusted *= ratio
            return adjusted
        candidates = []
        for i in range(1, len(daily)):
            row, prev = daily[i], daily[i-1]
            day = dt.date.fromisoformat(row["date"])
            event_start_price = float(row.get("o") or 0)
            if (day < start or day > end or row["date"] in split_dates
                    or prev.get("c", 0) <= 0 or event_start_price <= 0 or event_start_price > 10):
                continue
            previous_close = split_adjust_price(prev["c"], prev["date"], row["date"])
            daily_gap_pct = (float(row["o"]) / previous_close - 1) * 100 if previous_close > 0 else None
            if require_gap and (daily_gap_pct is None or daily_gap_pct <= min_gap_pct):
                continue
            volume = float(row.get("v") or 0)
            price = float(row.get("vw") or row.get("c") or 0)
            dvol = volume * price
            dollar_volume_candidate = dvol >= min_dollar_volume * 0.8
            if not dollar_volume_candidate:
                continue
            candidates.append({"index": i, "date": row["date"],
                               "volume": volume, "dollar_volume": dvol,
                               "score": dvol/min_dollar_volume})
        selected = sorted(candidates, key=lambda x: x["score"], reverse=True)[:20]
        output = []
        for candidate in selected:
            i, row = candidate["index"], daily[candidate["index"]]
            df = minute_df(ticker, row["date"])
            if df is None or df.empty:
                continue
            previous_close = split_adjust_price(daily[i-1]["c"], daily[i-1]["date"], row["date"])
            m = metrics(df, previous_close, True)
            session_dollar_volume=float((((df["h"]+df["l"]+df["c"])/3)*df["v"]).sum())
            if session_dollar_volume <= min_dollar_volume:
                continue
            bar_minutes = df.index.hour*60+df.index.minute
            opening_bars = df[(bar_minutes >= RG_START) & (bar_minutes < RG_START+5)]
            opening_volume = float(opening_bars["v"].sum()) if not opening_bars.empty else None
            opening_dollar_volume = (float((((opening_bars["h"]+opening_bars["l"]+opening_bars["c"])/3)*opening_bars["v"]).sum())
                                     if not opening_bars.empty else None)
            open_price, close_price = m.get("open"), m.get("close")
            # Confirm against the regular-session minute bars as well as the
            # daily aggregate used for the initial candidate scan.
            if open_price is None or float(open_price) > 10:
                continue
            event_gap_pct=(open_price/previous_close-1) if open_price and previous_close>0 else None
            if require_gap and (event_gap_pct is None or event_gap_pct*100 <= min_gap_pct):
                continue
            next_close = float(daily[i+1]["c"]) if i+1 < len(daily) else None
            next_ret = next_close / float(row["c"]) - 1 if next_close and row.get("c") else None
            event = {k:v for k,v in candidate.items() if k not in ("index", "score")}
            event.update({"dollar_volume":session_dollar_volume})
            event["qualifier"] = (f"Open gap >{min_gap_pct:g}% + session dollar volume >$50M + open ≤$10"
                                  if require_gap else "Session dollar volume >$50M + open ≤$10")
            event.update({"signal_models": [{"name": k, "m": v["m"], "p": v["p"],
                                              "stop_m": v["stop_m"], "pnl": v["pnl"]}
                                            for k, v in (m.get("models") or {}).items()],
                          "stop": STOP_PCT,
                          "cover": ({"m": m["close_min"], "p": m["close"]} if m.get("rg") else None)})
            event.update({"prev_close": previous_close, "premarket_high": m.get("pm_high"), "gap_pct": event_gap_pct,
                          "chart_off": ist_off(row["date"]),
                          "premarket_low": m.get("pm_low"), "open": open_price, "high": m.get("hod"),
                          "low": m.get("lod"), "close": close_price, "hod_min": m.get("hod_min"),
                          "open_to_close": (close_price/open_price-1) if close_price and open_price else None,
                          "hod_to_close": (close_price/m["hod"]-1) if close_price and m.get("hod") else None,
                          "premarket_volume": m.get("pm_vol"), "premarket_dollar_volume": m.get("pm_dv"),
                          "opening_5m_volume": opening_volume, "opening_5m_dollar_volume": opening_dollar_volume,
                          "next_session_return": next_ret})
            if not metadata_only:
                event["chart"] = chart_pack(df, m, previous_close, dt.date.fromisoformat(row["date"]))
            output.append(event)
            if len(output) >= max_events:
                break
        output.sort(key=lambda x: x["date"])
        return jsonify({"ticker": ticker, "count": len(output), "shown": len(output),
                        "events": output, "source": "Polygon daily aggregates and 1-minute aggregates"})
    except Exception as exc:
        app.logger.exception("Keyword event loading failed for %s", ticker)
        return jsonify({"error": f"Market data load failed: {type(exc).__name__}: {exc}"}), 502


def _compare_event_features(ticker, day, window, force=False):
    """Reuse live_snapshot for today; use Polygon daily/intraday aggregates for history."""
    today_live = day == dt.date.today().isoformat()
    cache_key = f"kwcompare_v3_{ticker}_{day}_{window}"
    cached = None if force else cget(cache_key, 30 if today_live else 6 * 3600)
    if cached is not None:
        return cached
    live = None
    if today_live:
        live = live_snapshot(ticker)
        live_day = live["date"].isoformat() if hasattr(live["date"], "isoformat") else str(live["date"])
        if live_day != day:
            raise ValueError(f"Live chart data is for {live_day}, not the selected date {day}.")
        daily = [dict(row) for row in live["hist"]]
        live_df = live["df"]
        regular_bars = live_df[(live_df.index.hour * 60 + live_df.index.minute >= RG_START) &
                               (live_df.index.hour * 60 + live_df.index.minute < RG_END)]
        today_row = {"date": day, "o": float(regular_bars["o"].iloc[0]) if not regular_bars.empty else None,
                     "h": float(live_df["h"].max()), "l": float(live_df["l"].min()),
                     "c": float(live_df["c"].iloc[-1]), "v": float(live_df["v"].sum())}
        daily.append(today_row)
        idx = len(daily) - 1
    else:
        daily = load_daily(ticker, force=force)
        idx = next((i for i, row in enumerate(daily) if row["date"] == day), None)
        if idx is None:
            raise ValueError(f"No daily candle for {ticker} on {day}; the date may not be a trading session.")
    if idx < 1:
        raise ValueError(f"Previous close is unavailable for {ticker} on {day}.")
    splits = [] if today_live else load_splits(ticker)
    def split_adjust(row, target_day):
        adjusted = dict(row)
        price_factor = volume_factor = 1.0
        source_day = row.get("date", target_day)
        for split in splits:
            split_day = split.get("date")
            if not split_day: continue
            ratio = float(split.get("to") or 0) / float(split.get("from") or 1)
            if ratio <= 0: continue
            if source_day < split_day <= target_day:
                price_factor /= ratio; volume_factor *= ratio
            elif target_day < split_day <= source_day:
                price_factor *= ratio; volume_factor /= ratio
        for key in ("o", "h", "l", "c", "vw"):
            if adjusted.get(key) is not None: adjusted[key] = float(adjusted[key]) * price_factor
        if adjusted.get("v") is not None: adjusted["v"] = float(adjusted["v"]) * volume_factor
        return adjusted
    previous_row = split_adjust(daily[idx-1], day)
    pc = float((live["pc"] if today_live else previous_row["c"]) or 0)
    if pc <= 0:
        raise ValueError(f"Previous close is invalid for {ticker} on {day}.")
    df = live["df"] if today_live else minute_df(ticker, day, force=force)
    if df is None or df.empty:
        raise ValueError(f"Intraday candles are unavailable for {ticker} on {day}; no daily substitute was used.")
    m = metrics(df, pc, True)
    # Resample one-minute real candles into fixed 5-minute buckets from 04:00 ET.
    mins = df.index.hour * 60 + df.index.minute
    session = df[(mins >= 240) & (mins < 960)].copy()
    session["bucket"] = ((session.index.hour * 60 + session.index.minute - 240) // 5).astype(int)
    intraday = []
    for _, group in session.groupby("bucket", sort=True):
        close = float(group["c"].iloc[-1])
        tp = (group["h"] + group["l"] + group["c"]) / 3
        vwap = float((tp * group["v"]).sum() / group["v"].sum()) if float(group["v"].sum()) else None
        intraday.append({"t": int(group["bucket"].iloc[0]) * 5 + 240,
                         "prev": (close / pc - 1) * 100,
                         "open": ((close / float(m.get("open")) - 1) * 100 if m.get("open") and int(group["bucket"].iloc[0]) * 5 + 240 >= RG_START else None),
                         "high": float(group["h"].max()), "low": float(group["l"].min()),
                         "vwap": vwap, "vol": float(group["v"].sum()),
                         "dollar": float((tp * group["v"]).sum())})
    total_intraday_volume = sum(x["vol"] for x in intraday) or 1.0
    total_intraday_dollars = sum(x["dollar"] for x in intraday) or 1.0
    cumulative_volume = 0.0
    cumulative_dollars = 0.0
    for point in intraday:
        cumulative_volume += point["vol"]
        cumulative_dollars += point["dollar"]
        point["cumvol"] = cumulative_volume / total_intraday_volume
        point["cumdollar"] = cumulative_dollars / total_intraday_dollars
        point["vwap_rel"] = ((point["prev"] is not None and point["vwap"] is not None) and
                             ((pc * (1 + point["prev"] / 100) / point["vwap"] - 1) * 100) or None)
    intraday_by_time = {point["t"]: point for point in intraday}
    intraday = [intraday_by_time.get(minute, {"t": minute, "prev": None, "open": None, "high": None, "low": None,
                                               "vwap": None, "vwap_rel": None, "vol": None, "dollar": None,
                                               "cumvol": None, "cumdollar": None})
                for minute in range(240, 960, 5)]
    regular = session[(session.index.hour * 60 + session.index.minute >= RG_START)]
    op = float(m.get("open") or 0)
    hod = float(m.get("hod") or 0)
    close = float(m.get("close") or 0)
    mark_close = close or (float(session["c"].iloc[-1]) if not session.empty else None)
    day_row = ({"date": day, "o": m.get("open"), "h": m.get("hod"), "l": m.get("lod"), "c": m.get("close"), "v": float(df["v"].sum())}
               if today_live or idx >= len(daily) else daily[idx])
    pre_rows = [split_adjust(row, day) for row in daily[max(0, idx-window-1):idx]]
    prior20 = [split_adjust(row, day) for row in daily[max(0, idx-20):idx]]
    prior60 = [split_adjust(row, day) for row in daily[max(0, idx-60):idx]]
    avg_volume20 = float(np.mean([float(x["v"]) for x in prior20 if x.get("v")])) if prior20 else None
    avg_volume60 = float(np.mean([float(x["v"]) for x in prior60 if x.get("v")])) if prior60 else None
    post_rows = [split_adjust(row, day) for row in daily[idx+1:idx+1+window]] if idx < len(daily) and not today_live else []
    base = [float(x["c"]) for x in pre_rows if x.get("c")]
    baseline = base[0] if base else None
    # D-N-1 is the reference close; score and display only the N sessions D-N..D-1.
    pre_path = [((float(x["c"])/baseline-1)*100 if baseline else None) for x in pre_rows[1:]]
    pre_vol = []
    for j, row in enumerate(pre_rows[1:], start=1):
        prior = daily[max(0, max(0, idx-window-1)+j-20):max(0, idx-window-1)+j]
        avg = float(np.mean([float(x["v"]) for x in prior])) if prior else None
        pre_vol.append(float(row["v"])/avg if avg else None)
    post_path = [((float(x["c"])/float(day_row["c"])-1)*100 if day_row.get("c") else None) for x in post_rows]
    post_open_gap, prior_close = [], float(day_row.get("c") or 0)
    for row in post_rows:
        post_open_gap.append((float(row["o"])/prior_close-1)*100 if prior_close and row.get("o") else None)
        prior_close = float(row.get("c") or prior_close)
    vols = [float(x["v"])/float(post_rows[0]["v"] or 1) for x in post_rows] if post_rows else []
    pre_scoring_rows = pre_rows[1:]
    pre_closes = [float(x["c"]) for x in pre_scoring_rows if x.get("c")]
    pre_ranges = [((float(x["h"])-float(x["l"]))/float(x["c"])*100 if x.get("c") else None) for x in pre_scoring_rows]
    pre_slope = (float(np.polyfit(np.arange(len(pre_path)), pre_path, 1)[0]) if len(pre_path) >= 2 else None)
    last_ten = pre_scoring_rows[-10:]
    pre_dist_high = ((float(pre_scoring_rows[-1]["c"])/max(float(x["h"]) for x in last_ten)-1)*100
                     if last_ten and pre_scoring_rows[-1].get("c") else None)
    pre_dist_low = ((float(pre_scoring_rows[-1]["c"])/min(float(x["l"]) for x in last_ten)-1)*100
                    if last_ten and pre_scoring_rows[-1].get("c") else None)
    pre_streak = 0
    for row in reversed(pre_scoring_rows):
        direction = 1 if float(row.get("c") or 0) > float(row.get("o") or 0) else -1
        if pre_streak and direction != (1 if pre_streak > 0 else -1): break
        pre_streak += direction
    post_drawdown = None
    if post_rows and day_row.get("c"):
        running_peak = float(day_row.get("h") or day_row["c"])
        post_drawdown = 0.0
        for row in post_rows:
            post_drawdown = min(post_drawdown, (float(row["l"])/running_peak-1)*100)
            running_peak = max(running_peak, float(row.get("h") or running_peak))
    post_runup = (max((float(x["h"])/float(day_row["c"])-1)*100 for x in post_rows) if post_rows and day_row.get("c") else None)
    post_low_day = (min(range(len(post_rows)), key=lambda i: float(post_rows[i]["l"]))+1 if post_rows else None)
    follow_through = (sum(1 for i, row in enumerate(post_rows) if float(row.get("c") or 0) > (float(day_row.get("c") or 0) if i == 0 else float(post_rows[i-1].get("c") or 0))) if post_rows else None)
    total_volume = float(df["v"].sum())
    total_dollar_volume = float((((df["h"]+df["l"]+df["c"])/3)*df["v"]).sum())
    pm = session[(session.index.hour * 60 + session.index.minute < RG_START)]
    rth = session[(session.index.hour * 60 + session.index.minute >= RG_START)]
    def first_window(minutes):
        bars = rth[(rth.index.hour * 60 + rth.index.minute < RG_START + minutes)]
        return (float(bars["v"].sum()), float((((bars["h"]+bars["l"]+bars["c"])/3)*bars["v"]).sum())) if not bars.empty else (None, None)
    first5_vol, first5_dv = first_window(5)
    first15_vol, first15_dv = first_window(15)
    first30_vol, first30_dv = first_window(30)
    cf = ctx_feats(daily, idx)
    days_since_50 = None
    for hist_i in range(idx - 1, 19, -1):
        if daily[hist_i - 20].get("c") and daily[hist_i].get("c") / daily[hist_i - 20]["c"] - 1 >= .50:
            days_since_50 = idx - 1 - hist_i
            break
    close_location = ((close - float(m["lod"])) / (float(m["hod"]) - float(m["lod"]))
                      if m.get("hod") is not None and m.get("lod") is not None and m["hod"] > m["lod"] and close else None)
    downlegs = m.get("downlegs") or []
    pullback_count = 0
    pullback_depth = 0.0
    running_peak = 0.0
    in_pullback = False
    for point in intraday:
        if point.get("high") is None or point.get("low") is None: continue
        running_peak = max(running_peak, float(point["high"]))
        depth = (float(point["low"])/running_peak-1)*100 if running_peak else 0.0
        pullback_depth = min(pullback_depth, depth)
        if depth <= -5 and not in_pullback:
            pullback_count += 1; in_pullback = True
        elif in_pullback and point.get("prev") is not None and float(point["prev"]) >= (running_peak/pc-1)*100:
            in_pullback = False
    daily_log_returns = []
    for offset in range(-10, 11):
        row_i = idx + offset
        if offset == 0 and (today_live or row_i >= len(daily)):
            daily_log_returns.append(math.log(mark_close/pc)*100 if mark_close and pc else None)
        elif row_i >= 1 and row_i < len(daily):
            left, right = daily[row_i-1], daily[row_i]
            daily_log_returns.append(math.log(float(right["c"])/float(left["c"]))*100 if right.get("c") and left.get("c") else None)
        else:
            daily_log_returns.append(None)
    daily_path_curve, daily_running = [], 0.0
    for daily_ret in daily_log_returns:
        if daily_ret is None: daily_path_curve.append(None)
        else:
            daily_running += daily_ret
            daily_path_curve.append(daily_running)
    features = {
        "ticker": ticker, "date": day, "pc": pc,
        "as_of_minute": (int(session.index.hour[-1] * 60 + session.index.minute[-1]) if not session.empty else None),
        "intraday": intraday,
        "close": (mark_close or None),
        "hod": (hod or None), "lod": (m.get("lod") or None),
        "rth_open": ((op/pc-1)*100 if op else None),
        "gap": ((op/pc-1)*100 if op else None),
        "hod_min": (m.get("hod_min") - RG_START if m.get("hod_min") is not None else None),
        "hod_fade": ((close/hod-1)*100 if hod and close else None),
        "open_close": ((close/op-1)*100 if op else None),
        "pm_vs_open": ((float(m["pm_high"])/op-1)*100 if m.get("pm_high") and op else None),
        "gap_retention": (((close-pc)/(op-pc)*100) if op and op != pc else None),
        "opening_share": (first5_vol/total_volume if first5_vol is not None and total_volume else None),
        "event_rvol": (total_volume/avg_volume20 if avg_volume20 else None),
        "event_rvol60": (total_volume/avg_volume60 if avg_volume60 else None),
        "day_volume": total_volume, "premarket_volume": float(pm["v"].sum()) if not pm.empty else None,
        "first5_volume": first5_vol, "first15_volume": first15_vol, "first30_volume": first30_vol,
        "premarket_dollar_volume": float((((pm["h"]+pm["l"]+pm["c"])/3)*pm["v"]).sum()) if not pm.empty else None,
        "first5_dollar_volume": first5_dv, "first15_dollar_volume": first15_dv, "first30_dollar_volume": first30_dv,
        "session_dollar_volume": total_dollar_volume,
        "premarket_rvol20": None, "premarket_rvol60": None,
        "first30_rvol20": None, "first30_rvol60": None,
        "float": None, "float_rotation_pm": None, "float_rotation_first30": None,
        "float_rotation_day": None, "float_rotation_curve": [None] * NPTS,
        "dollar_volume_market_cap_fraction": None,
        "premarket_high_pct": ((float(m["pm_high"])/pc-1)*100 if m.get("pm_high") and pc else None),
        "close_vs_vwap": ((close/float(m["vwap_close"])-1)*100 if close and m.get("vwap_close") else None),
        "close_location": close_location,
        "pullback_count": pullback_count if intraday else None,
        "pullback_max_depth": pullback_depth if intraday else None,
        "context_features": cf, "days_since_50pct_move": days_since_50,
        "daily_log_return_curve": daily_log_returns,
        "daily_path_curve": daily_path_curve,
        "volume_curve": [x.get("cumvol") for x in intraday],
        "dollar_volume_curve": [x.get("cumdollar") for x in intraday],
        "vwap_relationship_curve": [x.get("vwap_rel") for x in intraday],
        "pre_path": pre_path, "pre_rvol": pre_vol, "pre_trend_slope": pre_slope,
        "pre_dist_high": pre_dist_high, "pre_dist_low": pre_dist_low, "pre_streak": pre_streak,
        "pre_dollar_volume": [math.log10(max(1.0, float(x.get("v") or 0)*float(x.get("vw") or x.get("c") or 0))) for x in pre_rows[1:]],
        "pre_ranges": pre_ranges,
        "post_path": post_path, "post_open_gap": post_open_gap, "post_vol": vols,
        "post_drawdown": post_drawdown, "post_runup": post_runup,
        "post_low_day": post_low_day, "follow_through": follow_through,
        "pre_coverage": min(window, max(0, len(pre_rows)-1)), "post_coverage": len(post_rows),
        "chart": chart_pack(df, m, pc, dt.date.fromisoformat(day)),
        "source": ("Gap-Up Fade live_snapshot (Yahoo intraday; Alpaca when configured)"
                   if today_live else "Polygon 1-minute and daily aggregate candles")
    }
    features["feature_mask"] = {key: (value is not None) for key, value in features.items()
                                if key not in ("chart", "intraday", "feature_mask")}
    features["feature_masks"] = {
        "intraday": [{key: point.get(key) is not None for key in ("prev", "open", "vwap_rel", "cumvol", "cumdollar", "float_rotation")}
                     for point in intraday],
        "daily": [value is not None for value in daily_log_returns],
        "float_rotation": [False] * NPTS,
        "supply_facts": {"float": False, "registered_shares": False, "atm_capacity": False,
                         "equity_line_capacity": False, "warrants": False, "converts": False}
    }
    cput(cache_key, features)
    return features


def _compare_feature_asof(feature, cutoff_minute):
    """Mask every feature after a shared clock cutoff; retain original outcomes elsewhere."""
    out = dict(feature)
    bars = []
    for point in feature.get("intraday", []):
        p = dict(point)
        if cutoff_minute is None or p["t"] + 4 > cutoff_minute:
            for key in ("prev", "open", "high", "low", "vwap", "vwap_rel", "vol", "dollar", "cumvol", "cumdollar"):
                p[key] = None
        bars.append(p)
    active = [p for p in bars if p.get("vol") is not None]
    total_volume = sum(p["vol"] for p in active)
    total_dollars = sum(p["dollar"] or 0 for p in active)
    cumulative_v = cumulative_d = 0.0
    for p in active:
        cumulative_v += p["vol"]
        cumulative_d += p["dollar"] or 0
        p["cumvol"] = cumulative_v / total_volume if total_volume else None
        p["cumdollar"] = cumulative_d / total_dollars if total_dollars else None
    out["intraday"] = bars
    out["volume_curve"] = [p.get("cumvol") for p in bars]
    out["dollar_volume_curve"] = [p.get("cumdollar") for p in bars]
    out["vwap_relationship_curve"] = [p.get("vwap_rel") for p in bars]
    out["day_volume"] = total_volume if active else None
    out["session_dollar_volume"] = total_dollars if active else None
    average_volume = (feature.get("day_volume") / feature.get("event_rvol")
                      if feature.get("event_rvol") else None)
    out["event_rvol"] = (total_volume / average_volume if average_volume else None)
    opening_volume = sum(p["vol"] for p in active if RG_START <= p["t"] < RG_START+5)
    out["opening_share"] = opening_volume/total_volume if total_volume else None
    out["premarket_volume"] = sum(p["vol"] for p in active if p["t"] < RG_START) if active else None
    out["premarket_dollar_volume"] = sum(p["dollar"] or 0 for p in active if p["t"] < RG_START) if active else None
    if cutoff_minute is not None and cutoff_minute < RG_START:
        for key in ("gap", "rth_open", "pm_vs_open", "gap_retention", "open_close", "hod_min", "hod_fade", "hod", "lod"):
            out[key] = None
    price_points = [p for p in bars if p.get("prev") is not None]
    if price_points:
        last = price_points[-1]
        out["close"] = feature["pc"] * (1 + last["prev"] / 100)
        out["as_of_minute"] = last["t"] + 4
        peak = max((p["high"] for p in price_points if p.get("high") is not None), default=None)
        if peak and peak > 0:
            out["hod"] = peak
            out["hod_min"] = next((p["t"] + 4 - RG_START for p in price_points if p.get("high") == peak), None)
            out["hod_fade"] = (out["close"] / peak - 1) * 100
        low = min((p["low"] for p in price_points if p.get("low") is not None), default=None)
        if low is not None and peak > low:
            out["close_location"] = (out["close"]-low)/(peak-low)
        latest_vwap = next((p["vwap"] for p in reversed(price_points) if p.get("vwap") is not None), None)
        out["close_vs_vwap"] = (out["close"]/latest_vwap-1)*100 if latest_vwap else None
        open_points = [p for p in bars if p.get("open") is not None]
        if open_points:
            out["open_close"] = open_points[-1]["open"]
            opening = feature["pc"] * (1 + out["gap"] / 100) if out.get("gap") is not None else None
            if opening:
                out["gap_retention"] = (out["close"] - feature["pc"]) / (opening-feature["pc"])*100 if opening != feature["pc"] else None
    out["post_coverage"] = 0
    out["post_path"] = []
    out["post_open_gap"] = []
    return out


def _compare_similarity(a, b):
    """Score available families with z-Euclidean curves and bounded scalar distances."""
    def curve_score(x, y):
        pairs = [(float(u), float(v)) for u, v in zip(x or [], y or []) if u is not None and v is not None]
        if len(pairs) < 3:
            return None
        xx, yy = np.array([p[0] for p in pairs]), np.array([p[1] for p in pairs])
        sx, sy = float(np.std(xx)), float(np.std(yy))
        if sx <= 1e-9 or sy <= 1e-9:
            return max(0.0, 100.0 * (1.0 - min(2.0, float(np.mean(np.abs(xx-yy))) / 10.0)))
        distance = float(np.sqrt(np.mean(((xx-xx.mean())/sx-(yy-yy.mean())/sy)**2)))
        return max(0.0, 100.0 * (1.0 - distance/2.0))
    def scalar_score(keys, scale):
        ds = [abs(float(a[k])-float(b[k]))/scale for k in keys if a.get(k) is not None and b.get(k) is not None]
        return max(0.0, 100.0*(1.0-float(np.mean(ds)))) if ds else None
    intraday = [curve_score([x["prev"] for x in a["intraday"]], [x["prev"] for x in b["intraday"]]),
                curve_score([x.get("open") for x in a["intraday"]], [x.get("open") for x in b["intraday"]]),
                curve_score([x.get("cumvol") for x in a["intraday"]], [x.get("cumvol") for x in b["intraday"]]),
                curve_score([x.get("cumdollar") for x in a["intraday"]], [x.get("cumdollar") for x in b["intraday"]]),
                curve_score(a.get("vwap_relationship_curve"), b.get("vwap_relationship_curve")),
                scalar_score(["gap", "gap_retention", "hod_fade", "open_close", "pm_vs_open", "close_vs_vwap"], 50.0),
                scalar_score(["event_rvol", "event_rvol60"], 10.0),
                scalar_score(["opening_share"], 1.0),
                scalar_score(["hod_min"], 120.0)]
    A = float(np.mean([x for x in intraday if x is not None])) if any(x is not None for x in intraday) else None
    pre = [curve_score(a.get("pre_path"), b.get("pre_path")),
           curve_score(a.get("pre_rvol"), b.get("pre_rvol")),
           curve_score(a.get("pre_dollar_volume"), b.get("pre_dollar_volume")),
           curve_score(a.get("pre_ranges"), b.get("pre_ranges")),
           scalar_score(["pre_trend_slope", "pre_dist_high", "pre_dist_low"], 50.0),
           scalar_score(["pre_streak"], 10.0)]
    B = float(np.mean([x for x in pre if x is not None])) if any(x is not None for x in pre) else None
    C = None
    if a.get("post_coverage", 0) and b.get("post_coverage", 0):
        post = [curve_score(a.get("post_path"), b.get("post_path")),
                curve_score(a.get("post_open_gap"), b.get("post_open_gap")),
                curve_score(a.get("post_vol"), b.get("post_vol")),
                scalar_score(["post_drawdown", "post_runup"], 50.0),
                scalar_score(["post_low_day", "follow_through"], 10.0)]
        C = float(np.mean([x for x in post if x is not None])) if any(x is not None for x in post) else None
    return {"A": A, "B": B, "C": C}


def _learn_compare_weights(events):
    """Grid-search A/B/C weights using leave-one-event-out open-to-close RMSE."""
    defaults = {"A": 50.0, "B": 25.0, "C": 25.0}
    usable = [e for e in events if e.get("features", {}).get("open_close") is not None]
    if len(usable) < 30:
        return defaults, {"method": "default", "sample_size": len(usable), "reason": "Fewer than 30 events with outcomes."}
    groups = ("A", "B", "C")
    matrices = {}
    for i, left in enumerate(usable):
        for j in range(i + 1, len(usable)):
            right = usable[j]
            pair = _compare_similarity(left["features"], right["features"])
            matrices[(i, j)] = pair
    grids = [0, 25, 50, 75, 100]
    best = (float("inf"), defaults)
    for a in grids:
        for b in grids:
            for c in grids:
                if a+b+c == 0: continue
                weights = {"A": float(a), "B": float(b), "C": float(c)}
                errors = []
                for held in range(len(usable)):
                    neighbors = []
                    for other in range(len(usable)):
                        if held == other: continue
                        pair = matrices[(min(held, other), max(held, other))]
                        active = [k for k in groups if pair[k] is not None and weights[k] > 0]
                        denom = sum(weights[k] for k in active)
                        if denom:
                            score = sum(pair[k]*weights[k] for k in active)/denom
                            neighbors.append((score, other))
                    if neighbors:
                        nearest = max(neighbors)[1]
                        errors.append((float(usable[held]["features"]["open_close"])-
                                       float(usable[nearest]["features"]["open_close"]))**2)
                rmse = math.sqrt(float(np.mean(errors))) if errors else float("inf")
                if rmse < best[0]: best = (rmse, weights)
    return best[1], {"method": "leave-one-out outcome RMSE", "sample_size": len(usable), "rmse": best[0]}


def _compare_share_context(ticker, day, price, adjust_splits=True):
    facts = load_shares_out(ticker)
    eligible = [row for row in facts if row.get("filed", "") <= day and row.get("end", "") <= day]
    if not eligible:
        return {"shares_out": None, "filed": None, "market_cap": None,
                "market_cap_bucket": "Unavailable", "price_bucket": _compare_price_bucket(price), "exchange": "Unavailable"}
    row = max(eligible, key=lambda item: (item.get("end", ""), item.get("filed", "")))
    shares = float(row["val"])
    for split in (load_splits(ticker) if adjust_splits else []):
        if row.get("end", "") < split.get("date", "") <= day:
            shares *= float(split.get("to") or 1) / float(split.get("from") or 1)
    market_cap = shares * float(price) if price else None
    if market_cap is None: cap_bucket = "Unavailable"
    elif market_cap < 50_000_000: cap_bucket = "< $50M"
    elif market_cap < 300_000_000: cap_bucket = "$50M–$300M"
    elif market_cap < 2_000_000_000: cap_bucket = "$300M–$2B"
    else: cap_bucket = "≥ $2B"
    return {"shares_out": shares, "filed": row.get("filed"), "market_cap": market_cap,
            "market_cap_bucket": cap_bucket, "price_bucket": _compare_price_bucket(price), "exchange": "Unavailable"}


def _compare_price_bucket(price):
    if not price: return "Unavailable"
    if price < 1: return "<$1"
    if price < 5: return "$1–$5"
    if price < 20: return "$5–$20"
    if price < 100: return "$20–$100"
    return "≥$100"


def _compare_validation_metrics(actual, predicted, continuous):
    pairs = [(float(a), float(p)) for a, p in zip(actual, predicted) if a is not None and p is not None]
    if not pairs: return {"n": 0, "rmse": None, "rmse_ci95": None, "hit_rate": None, "hit_ci95": None}
    a = np.array([x[0] for x in pairs]); p = np.array([x[1] for x in pairs])
    errors = a-p
    rmse = float(np.sqrt(np.mean(errors**2))) if continuous else None
    hit = float(np.mean(np.sign(a)==np.sign(p)))
    rng = np.random.default_rng(71027)
    if len(a) > 1:
        samples = rng.integers(0, len(a), size=(500, len(a)))
        rmses = np.sqrt(np.mean(errors[samples]**2, axis=1)) if continuous else None
        hits = np.mean((np.sign(a[samples])==np.sign(p[samples])), axis=1)
        rmse_ci = [float(np.percentile(rmses, 2.5)), float(np.percentile(rmses, 97.5))] if continuous else None
        hit_ci = [float(np.percentile(hits, 2.5)), float(np.percentile(hits, 97.5))]
    else:
        rmse_ci, hit_ci = ([rmse, rmse] if continuous else None), [hit, hit]
    return {"n": len(a), "rmse": rmse, "rmse_ci95": rmse_ci, "hit_rate": hit, "hit_ci95": hit_ci}


@app.route("/api/keyword-validate", methods=["POST"])
def api_keyword_validate():
    data = request.get_json(silent=True) or {}
    user = str(data.get("user") or "").strip().lower()
    token = str(data.get("token") or "").strip()
    if user not in ALLOWED_USERS or not _db_validate_session(user, token):
        return jsonify({"error": "Session expired. Please sign in again."}), 401
    candidates = data.get("candidates") or []
    if not isinstance(candidates, list) or any(not isinstance(x, dict) for x in candidates):
        return jsonify({"error": "Invalid validation event list."}), 400
    mode = "chronological" if data.get("mode") == "chronological" else "leave-one-out"
    weights = data.get("weights") or {"A": 50, "B": 25, "C": 25}
    if not isinstance(weights, dict): weights = {"A": 50, "B": 25, "C": 25}
    weights = {k: max(0.0, min(100.0, float(weights.get(k, 0)))) for k in ("A", "B", "C")}
    pool = sorted(candidates, key=lambda x: float(x.get("dollar_volume") or 0), reverse=True)[:60]
    events = []
    window = max(5, min(20, int(data.get("window") or 10)))
    for item in pool:
        try:
            symbol, day = str(item.get("ticker") or "").strip().upper(), str(item.get("date") or "")
            if not re.fullmatch(r"[A-Z.\-]{1,10}", symbol): continue
            dt.date.fromisoformat(day)
            feat = _compare_event_features(symbol, day, window)
            events.append({"ticker": symbol, "date": day, "features": feat})
        except Exception as exc:
            app.logger.info("Validation event skipped: %s", exc)
    fields = ("hod_fade", "open_close", "post_path", "intraday")
    outcomes = []
    for event in events:
        f = event["features"]
        path = f.get("post_path") or []
        bars = f.get("intraday") or []
        mid = bars[102].get("prev") if len(bars) > 102 else None
        end = bars[143].get("prev") if len(bars) > 143 else None
        outcomes.append({"fade": f.get("hod_fade"), "open_close": f.get("open_close"),
                         "d1": path[0] if path else None,
                         "midday_remaining": end-mid if end is not None and mid is not None else None})
    model_predictions = {key: [] for key in ("fade", "open_close", "d1", "midday_remaining")}
    scalar_predictions = {key: [] for key in model_predictions}
    similar_predictions = {key: [] for key in model_predictions}
    random_predictions = {key: [] for key in model_predictions}
    actuals = {key: [] for key in model_predictions}
    for i, event in enumerate(events):
        others = [j for j in range(len(events)) if j != i and (mode != "chronological" or events[j]["date"] < event["date"])]
        if not others: continue
        target = event["features"]
        sims = []
        for j in others:
            score_parts = _compare_similarity(target, events[j]["features"])
            active = [k for k in ("A", "B", "C") if score_parts[k] is not None and weights[k] > 0]
            denom = sum(weights[k] for k in active)
            score = sum(score_parts[k]*weights[k] for k in active)/denom if denom else 0
            sims.append((score, j))
        nearest = max(sims)[1]
        gap_peers = [j for j in others if target.get("gap") is not None and
                     events[j]["features"].get("gap") is not None and
                     abs(target["gap"]-events[j]["features"]["gap"]) <= 10 and
                     _compare_price_bucket(target.get("close")) == _compare_price_bucket(events[j]["features"].get("close"))]
        baseline_peer_pool = gap_peers or others
        scalar_peer = min(others, key=lambda j:
            abs((target.get("gap") or 0)-(events[j]["features"].get("gap") or 0))/20 +
            abs(math.log1p(target.get("event_rvol") or 0)-math.log1p(events[j]["features"].get("event_rvol") or 0))/3 +
            abs(math.log1p(target.get("session_dollar_volume") or 0)-math.log1p(events[j]["features"].get("session_dollar_volume") or 0))/20)
        rng = random.Random(f"{event['ticker']}:{event['date']}:{mode}")
        random_peer = rng.choice(others)
        for key in model_predictions:
            actual = outcomes[i][key]
            pred = outcomes[nearest][key]
            if actual is None or pred is None: continue
            actuals[key].append(actual); model_predictions[key].append(pred)
            value = [outcomes[j][key] for j in baseline_peer_pool if outcomes[j][key] is not None]
            similar_predictions[key].append(float(np.mean(value)) if value else None)
            scalar_predictions[key].append(outcomes[scalar_peer][key])
            random_predictions[key].append(outcomes[random_peer][key])
    results = {}
    for key in model_predictions:
        continuous = key in ("fade", "midday_remaining")
        results[key] = {"matcher": _compare_validation_metrics(actuals[key], model_predictions[key], continuous),
                        "gap_price_baseline": _compare_validation_metrics(actuals[key], similar_predictions[key], continuous),
                        "random_keyword_baseline": _compare_validation_metrics(actuals[key], random_predictions[key], continuous),
                        "scalar_only_baseline": _compare_validation_metrics(actuals[key], scalar_predictions[key], continuous)}
    wins = sum(1 for key in ("fade", "midday_remaining")
               if results[key]["matcher"]["rmse"] is not None and
               all(results[key][base]["rmse"] is None or results[key]["matcher"]["rmse"] < results[key][base]["rmse"]
                   for base in ("gap_price_baseline", "random_keyword_baseline", "scalar_only_baseline")))
    return jsonify({"mode": mode, "sample_size": len(events), "evaluated": len(actuals["fade"]),
                    "metrics": results, "beats_baseline": wins == 2,
                    "warning": None if wins == 2 else "Matcher did not beat every baseline on both continuous outcomes in this sample.",
                    "note": "Leave-one-out uses only other real SEC-associated qualifying events. Confidence intervals are deterministic bootstrap 95% intervals; random same-keyword baseline uses a seeded one-event draw. Candidate pool is capped at 60 events."})


# ════════════════════════════════════════════════════════════════════════════
#  2-MIN INTRADAY FORECAST  (analog-conditioned, vol-scaled, conformal-calibrated)
#  Not a point prediction: a conditional distribution of the rest of the session
#  built from the SAME clock-time slice of similar historical event days.
# ════════════════════════════════════════════════════════════════════════════
FC_STEP = 2
FC_N = (RG_END - PM_START) // FC_STEP            # 360 two-minute buckets, 04:00 -> 16:00 ET


def _fc_grid(feature):
    """Forward-filled 2-min close grid as ln(price / prev close), plus the 2-min highs."""
    ch = (feature or {}).get("chart") or {}
    b, pc = ch.get("bars"), ch.get("pc") or (feature or {}).get("pc")
    if not b or not b.get("t") or not pc:
        return None
    t = np.asarray(b["t"], float)
    c = np.asarray(b["c"], float)
    h = np.asarray(b["h"], float)
    k = ((t - PM_START) // FC_STEP).astype(int)
    close = np.full(FC_N, np.nan)
    high = np.full(FC_N, np.nan)
    for i in range(len(t)):
        if 0 <= k[i] < FC_N and np.isfinite(c[i]) and c[i] > 0:
            close[k[i]] = c[i]                                    # last close in the bucket wins
            high[k[i]] = h[i] if np.isnan(high[k[i]]) else max(high[k[i]], h[i])
    close = pd.Series(close).ffill().to_numpy()
    if not np.isfinite(close).any():
        return None
    return {"x": np.log(close / pc), "hx": np.log(np.where(np.isfinite(high), high, np.nan) / pc),
            "pc": float(pc), "close": close}


def _fc_ohlc(feature):
    """2-min OHLCV grid (360 buckets). Empty buckets become flat doji with zero volume."""
    ch = (feature or {}).get("chart") or {}
    b = ch.get("bars")
    if not b or not b.get("t"):
        return None
    t = np.asarray(b["t"], float)
    o, h, l, c, v = (np.asarray(b[k], float) for k in ("o", "h", "l", "c", "v"))
    k = ((t - PM_START) // FC_STEP).astype(int)
    O = np.full(FC_N, np.nan); Hh = O.copy(); Ll = O.copy(); Cc = O.copy(); V = np.zeros(FC_N)
    for i in range(len(t)):
        j = k[i]
        if not (0 <= j < FC_N) or not np.isfinite(c[i]) or c[i] <= 0:
            continue
        if np.isnan(O[j]):
            O[j], Hh[j], Ll[j] = o[i], h[i], l[i]
        else:
            Hh[j], Ll[j] = max(Hh[j], h[i]), min(Ll[j], l[i])
        Cc[j] = c[i]
        V[j] += v[i]
    Cc = pd.Series(Cc).ffill().to_numpy()
    gap = np.isnan(O)
    O = np.where(gap, Cc, O); Hh = np.where(gap, Cc, Hh); Ll = np.where(gap, Cc, Ll)
    return {"o": O, "h": Hh, "l": Ll, "c": Cc, "v": V}


def _wq(vals, w, q):
    """Weighted quantile of 1-D vals."""
    vals, w = np.asarray(vals, float), np.asarray(w, float)
    m = np.isfinite(vals) & (w > 0)
    if not m.any():
        return np.nan
    vals, w = vals[m], w[m]
    o = np.argsort(vals)
    vals, w = vals[o], w[o]
    cw = (np.cumsum(w) - 0.5 * w) / w.sum()
    return float(np.interp(q, cw, vals))


def _fc_sigma(x, k, win=30):
    d = np.diff(x[max(0, k - win):k + 1])
    d = d[np.isfinite(d)]
    return float(max(np.std(d) if len(d) >= 8 else 0.0, 1e-4))


def _forecast_2m(target, pool, now_min, tau=10.0, k_shrink=2.0, max_paths=8):
    """
    target  : features dict (target day, bars up to now_min)
    pool    : list of {"ticker","date","score","features"} ranked analogs (use ~15, not 3)
    returns : dict with 2-min price quantile fan from now to 16:00, or None
    """
    T = _fc_grid(target)
    if T is None or now_min is None:
        return None
    kn = int(min(FC_N - 2, max(0, (now_min - PM_START) // FC_STEP)))
    if not np.isfinite(T["x"][kn]):
        return None
    H = FC_N - 1 - kn                                    # horizons 1..H
    sig_t = _fc_sigma(T["x"], kn)
    rows = []
    for ev in pool:
        G = _fc_grid(ev["features"])
        if G is None or not np.isfinite(G["x"][kn]) or not np.isfinite(G["x"][-1]):
            continue
        sig_j = _fc_sigma(G["x"], kn)
        r = G["x"][kn + 1:] - G["x"][kn]
        if len(r) != H or not np.isfinite(r).all():
            continue
        scale = float(np.clip(sig_t / sig_j, 0.5, 2.0)) ** 0.5       # half-strength vol scaling
        rows.append({"G": G, "ev": ev, "scale": scale, "r": r * scale, "raw": r, "sig": sig_j, "score": float(ev.get("score") or 0.0)})
    n = len(rows)
    if n < 3:
        return None
    sc = np.array([x["score"] for x in rows])
    w = np.exp((sc - sc.max()) / tau)
    w /= w.sum()
    ess = float(1.0 / np.sum(w ** 2))
    R = np.vstack([x["r"] for x in rows])                               # n x H
    shrink = ess / (ess + k_shrink)                                      # pull drift toward 0 when evidence is thin
    center = np.array([_wq(R[:, h], w, 0.5) for h in range(H)])
    med = center * shrink

    # ── split-conformal bands: leave-one-analog-out, errors in units of own vol * sqrt(h) ──
    hs = np.sqrt(np.arange(1, H + 1))
    Z = np.full((n, H), np.nan)
    if n >= 6:
        for j in range(n):
            wj = np.delete(w, j)
            wj = wj / wj.sum()
            Rj = np.delete(R, j, axis=0)
            e_shr = ess / (ess + k_shrink)
            mj = np.array([_wq(Rj[:, h], wj, 0.5) for h in range(H)]) * e_shr
            Z[j] = (rows[j]["raw"] - mj) / (rows[j]["sig"] * hs)
    calibrated = n >= 6
    qs = {"p10": 0.10, "p25": 0.25, "p75": 0.75, "p90": 0.90}
    bands = {}
    z_nrm = {0.10: -1.2816, 0.25: -0.6745, 0.75: 0.6745, 0.90: 1.2816}
    for name, q in qs.items():
        arr = np.empty(H)
        for h in range(H):
            if calibrated:
                lo, hi = max(0, h - 10), min(H, h + 11)
                zz = Z[:, lo:hi].ravel()
                ww = np.repeat(w, hi - lo)
                zq = _wq(zz, ww, q)
                zq = z_nrm[q] if not np.isfinite(zq) else zq
                zq = zq if q < 0.5 else max(zq, 0.0)
                zq = zq if q > 0.5 else min(zq, 0.0)
            else:
                zq = z_nrm[q] * 1.15                                      # honest fallback: widened Gaussian
            arr[h] = med[h] + zq * sig_t * hs[h]
        bands[name] = arr
    ordered = np.vstack([bands["p10"], bands["p25"], med, bands["p75"], bands["p90"]])
    ordered = np.sort(ordered, axis=0)
    p10, p25, p50, p75, p90 = ordered

    x0, pc = float(T["x"][kn]), T["pc"]
    price = lambda arr: [round(float(pc * np.exp(x0 + v)), 4) for v in np.concatenate([[0.0], arr])]
    tmin = [PM_START + FC_STEP * (kn + h) for h in range(H + 1)]

    # outcome probabilities from the weighted scaled analog paths
    hod_x = float(np.nanmax(T["hx"][:kn + 1])) if np.isfinite(T["hx"][:kn + 1]).any() else x0
    p_hod = float(np.sum(w * (x0 + R.max(axis=1) > hod_x)))
    p_down = float(np.sum(w * (R[:, -1] < 0)))
    closeq = [round(float(pc * np.exp(x0 + arr[-1])), 4) for arr in (p10, p25, p50, p75, p90)]   # same numbers as the fan end

    order = np.argsort(-w)[:max_paths]
    paths = [{"name": f'{rows[i]["ev"]["ticker"]} {rows[i]["ev"]["date"]}', "w": round(float(w[i]), 3),
              "vals": price(rows[i]["r"])} for i in order]

    # Build synthetic 2-minute OHLCV paths from weighted candle quantiles. Each analog
    # contributes its volatility-scaled intrabar shape, centered on the shrunk forecast
    # median. The 25th/50th/75th paths are quantile scenarios, not one chosen ticker.
    scenarios = []
    try:
        target_ohlc = _fc_ohlc(target)
        target_bar_vol = (float(np.mean(target_ohlc["v"][max(0, kn - 29):kn + 1]))
                          if target_ohlc is not None else 0.0)
        target_price = float(T["close"][kn])
        samples = [[] for _ in range(H)]
        for j, row in enumerate(rows):
            analog = _fc_ohlc(row["ev"].get("features"))
            if analog is None or not np.isfinite(analog["c"][kn]) or analog["c"][kn] <= 0:
                continue
            analog_bar_vol = float(np.mean(analog["v"][max(0, kn - 29):kn + 1]))
            volume_scale = (float(np.clip(target_bar_vol / analog_bar_vol, 0.05, 20.0))
                            if target_bar_vol > 0 and analog_bar_vol > 0 else 1.0)
            previous_predicted_close = target_price
            for h, k in enumerate(range(kn + 1, FC_N)):
                analog_close = float(analog["c"][k])
                if not np.isfinite(analog_close) or analog_close <= 0:
                    continue
                # Keep each analog's residual around the weighted center, while using
                # the forecast's drift-shrunk median as the center of the candle path.
                predicted_close = target_price * math.exp(med[h] + (row["r"][h] - center[h]))
                scale = row["scale"]
                op = predicted_close * math.exp(scale * math.log(max(float(analog["o"][k]), 1e-9) / analog_close))
                hi = predicted_close * math.exp(scale * math.log(max(float(analog["h"][k]), 1e-9) / analog_close))
                low = predicted_close * math.exp(scale * math.log(max(float(analog["l"][k]), 1e-9) / analog_close))
                lo_ = min(low, op, predicted_close)
                hi_ = max(hi, op, predicted_close)
                vol = max(0.0, float(analog["v"][k])) * volume_scale
                samples[h].append({"weight": float(w[j]), "o": op, "h": hi_, "l": lo_,
                                   "c": predicted_close, "v": vol, "real": float(analog["v"][k]) > 0,
                                   "up": predicted_close > previous_predicted_close})
                previous_predicted_close = predicted_close

        for q, name in ((0.50, "Weighted median candle"), (0.25, "Per-candle 25th percentile"),
                        (0.75, "Per-candle 75th percentile")):
            bars = []
            for h, k in enumerate(range(kn + 1, FC_N)):
                bucket = samples[h]
                if not bucket:
                    continue
                weights = [sample["weight"] for sample in bucket]
                values = {field: _wq([sample[field] for sample in bucket], weights, q)
                          for field in ("o", "h", "l", "c")}
                op, close = values["o"], values["c"]
                hi = max(values["h"], op, close)
                low = min(values["l"], op, close)
                volume = _wq([sample["v"] for sample in bucket], weights, 0.50)
                traded = [sample for sample in bucket if sample["real"]]
                traded_weight = sum(sample["weight"] for sample in traded)
                probability_up = (sum(sample["weight"] for sample in traded if sample["up"]) / traded_weight
                                  if traded_weight > 0 else None)
                bars.append([PM_START + FC_STEP * k, round(float(op), 4), round(float(hi), 4),
                             round(float(low), 4), round(float(close), 4),
                             int(max(0.0, volume)), round(float(probability_up), 3)
                             if probability_up is not None else None])
            scenarios.append({"name": name, "analog": f"weighted across {n} analogs", "bars": bars})
    except Exception:
        app.logger.exception("2-min candle scenarios failed")
        scenarios = []

    out = {"step": FC_STEP, "scenarios": scenarios, "as_of_minute": int(now_min), "t": tmin, "now_price": round(float(T["close"][kn]), 4),
           "p10": price(p10), "p25": price(p25), "p50": price(p50), "p75": price(p75), "p90": price(p90),
           "n": n, "ess": round(ess, 1), "calibrated": bool(calibrated), "shrink": round(shrink, 2),
           "sigma_2m_pct": round(sig_t * 100, 2), "p_new_hod": round(p_hod, 3), "p_close_below_now": round(p_down, 3),
           "close_q": closeq, "paths": paths}
    return out


def _fc_actual(target, now_min):
    """Only for backtests on historical days: the real 2-min closes after `now_min`."""
    T = _fc_grid(target)
    if T is None:
        return None
    kn = int(min(FC_N - 2, max(0, (now_min - PM_START) // FC_STEP)))
    return [round(float(v), 4) for v in T["close"][kn:]]



@app.route("/api/keyword-compare", methods=["POST"])
def api_keyword_compare():
    data = request.get_json(silent=True) or {}
    user = str(data.get("user") or "").strip().lower()
    token = str(data.get("token") or "").strip()
    if user not in ALLOWED_USERS or not _db_validate_session(user, token):
        return jsonify({"error": "Session expired. Please sign in again."}), 401
    ticker = str(data.get("ticker") or "").strip().upper()
    if not re.fullmatch(r"[A-Z.\-]{1,10}", ticker):
        return jsonify({"error": "Enter a valid ticker symbol."}), 400
    try:
        day = dt.date.fromisoformat(str(data.get("date") or "" )).isoformat()
        window = max(5, min(20, int(data.get("window") or 10)))
        top_k = 10
        weights = data.get("weights") or {"A": 50, "B": 25, "C": 25}
        if not isinstance(weights, dict): raise ValueError
        weights = {k: max(0.0, min(100.0, float(weights.get(k, 0)))) for k in ("A", "B", "C")}
        manual_weights = bool(data.get("manual_weights"))
        candidates = data.get("candidates") or []
        if not isinstance(candidates, list) or len(candidates) > 1000 or any(not isinstance(x, dict) for x in candidates): raise ValueError
    except (TypeError, ValueError):
        return jsonify({"error": "Choose a valid date, comparison window, and candidate list."}), 400
    try:
        force = bool(data.get("force"))
        target = _compare_event_features(ticker, day, window, force=force)
        partial_day = day == dt.date.today().isoformat()
        asof_arg = data.get("asof_minute")                      # optional: backtest a historical day as-of this ET minute
        cutoff_minute = (target.get("as_of_minute") if partial_day
                         else (int(asof_arg) if asof_arg else None))
        target_for_score = _compare_feature_asof(target, cutoff_minute) if cutoff_minute is not None else target
        compare_own = bool(data.get("include_own"))
        eligible = []
        candidate_pool = sorted(candidates, key=lambda item: max(
            float(item.get("dollar_volume") or 0) / 50_000_000,
            float(item.get("fast_move_multiple") or 0) / 2), reverse=True)[:120]
        for item in candidate_pool:
            symbol = str(item.get("ticker") or "").strip().upper()
            event_day = str(item.get("date") or "")
            if not re.fullmatch(r"[A-Z.\-]{1,10}", symbol) or (symbol == ticker and not compare_own) or event_day == day and symbol == ticker:
                continue
            if asof_arg and event_day >= day:                  # backtest: never use analogs from the future
                continue
            try:
                dt.date.fromisoformat(event_day)
                feat = _compare_event_features(symbol, event_day, window, force=force)
                feature_for_score = _compare_feature_asof(feat, cutoff_minute) if cutoff_minute is not None else feat
                scores = _compare_similarity(target_for_score, feature_for_score)
                active = {k: v for k, v in scores.items() if v is not None and weights[k] > 0}
                total_weight = sum(weights[k] for k in active)
                if not total_weight: continue
                overall = sum(scores[k]*weights[k] for k in active)/total_weight
                eligible.append({"ticker": symbol, "date": event_day, "company": item.get("company") or symbol,
                                 "keyword": item.get("keyword") or "", "role": item.get("role") or "",
                                 "headline": item.get("headline") or "", "filing_url": item.get("filing_url") or "",
                                 "filing_date": item.get("filing_date") or "",
                                 "scores": scores, "score": overall, "features": feat})
            except Exception as exc:
                app.logger.info("Keyword compare candidate %s %s skipped: %s", symbol, event_day, exc)
        weight_report = {"method": "manual slider override", "sample_size": len(eligible)} if manual_weights else None
        if not manual_weights:
            weights, weight_report = _learn_compare_weights(eligible)
            for item in eligible:
                active = [k for k in ("A", "B", "C") if item["scores"].get(k) is not None and weights[k] > 0]
                denominator = sum(weights[k] for k in active)
                item["score"] = (sum(item["scores"][k]*weights[k] for k in active)/denominator if denominator else 0.0)
        eligible.sort(key=lambda x: x["score"], reverse=True)
        sample_matches = eligible[:top_k]
        matches = sample_matches[:3]
        target["context"] = _compare_share_context(ticker, day, target.get("close"), adjust_splits=not partial_day)
        target["dollar_volume_market_cap_fraction"] = (target.get("session_dollar_volume")/target["context"]["market_cap"]
                                                       if target["context"].get("market_cap") else None)
        target["feature_mask"]["dollar_volume_market_cap_fraction"] = target["dollar_volume_market_cap_fraction"] is not None
        for match in matches:
            match["features"]["context"] = _compare_share_context(match["ticker"], match["date"], match["features"].get("close"))
            match["features"]["dollar_volume_market_cap_fraction"] = (match["features"].get("session_dollar_volume")/match["features"]["context"]["market_cap"]
                                                                       if match["features"]["context"].get("market_cap") else None)
            match["features"]["feature_mask"]["dollar_volume_market_cap_fraction"] = match["features"]["dollar_volume_market_cap_fraction"] is not None
        sample_payload = []
        for item in sample_matches:
            feature = dict(item["features"])
            feature.pop("chart", None)  # detailed charts are sent only for the visible top three
            sample_payload.append({"ticker": item["ticker"], "date": item["date"],
                                   "score": item["score"], "features": feature})
        forecast = None
        try:
            fc_now = cutoff_minute
            if fc_now is not None:
                forecast = _forecast_2m(target, eligible[:top_k], fc_now)  # same 10 analogs as the outcome sample
                if forecast is not None and not partial_day:
                    forecast["actual"] = _fc_actual(target, fc_now)
        except Exception:
            app.logger.exception("2-min forecast failed for %s %s", ticker, day)
        return jsonify({"target": target, "matches": matches, "sample_matches": sample_payload,
                        "forecast": forecast, "candidate_count": len(eligible),
                        "partial_day": partial_day, "as_of_minute": cutoff_minute,
        "target_source": ("Gap-Up Fade live_snapshot (Yahoo intraday; Alpaca when configured)"
                                         if partial_day else "Polygon historical daily and 1-minute candles"),
                        "match_source": "Polygon historical daily and 1-minute candles",
                        "weights": weights, "weight_report": weight_report,
                        "window": window, "target_post_coverage": target["post_coverage"],
                        "consensus": {"faded": sum(1 for x in sample_matches if (x["features"].get("hod_fade") or 0) < -20),
                                      "ground_higher": sum(1 for x in sample_matches if (x["features"].get("post_path") or [0])[-1] > 0),
                                      "early_hod": sum(1 for x in sample_matches if x["features"].get("hod_min") is not None and x["features"]["hod_min"] < 90)},
                        "formula": "A averages 5-minute previous-close, open-relative, and cumulative-volume z-curve scores plus event-stat similarities; B averages pre-gap return, relative-volume, log-dollar-volume, range curves and pre-gap slope/high-low/streak similarities; C averages post-gap return/open-gap/volume curves and drawdown/runup/low-day/follow-through similarities. For each curve: max(0,100*(1-RMS(zA-zB)/2)); scalar similarity=max(0,100*(1-mean(abs(diff)/scale))) with 50 percentage points for percent stats, 120 minutes for HOD time, and 10 sessions for streak/day counts. Overall is the weighted mean of available A/B/C, renormalized when a component is unavailable.",
                        "source": "Polygon intraday and daily aggregate candles; SEC keyword-search rows supplied by the current page."})
    except Exception as exc:
        app.logger.exception("Keyword compare failed for %s %s", ticker, day)
        return jsonify({"error": f"Compare data unavailable: {type(exc).__name__}: {exc}"}), 502


_scanner_cache = {"at": 0.0, "rows": []}
_scanner_cache_lock = threading.Lock()
_scanner_scan_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="scanner-universe")
_scanner_scan_state = {"running": False, "session_date": "", "done": 0, "total": 0,
                       "symbols_done": 0, "symbols_total": 0, "universe_total": 0,
                       "phase": "", "error": "", "completed_at": 0.0}
_SCANNER_RESCAN_SECONDS = 60


def _scanner_log(message):
    print(f"[gap-scanner] {message}", flush=True)


def _scanner_session_date(now=None):
    """Return the scanner day, rolling over at 13:30 Asia/Kolkata."""
    now = now or dt.datetime.now(IST)
    if now.tzinfo is None:
        now = now.replace(tzinfo=IST)
    local = now.astimezone(IST)
    session_start = local.replace(hour=13, minute=30, second=0, microsecond=0)
    if local < session_start:
        local -= dt.timedelta(days=1)
    return local.date().isoformat()


def _scanner_is_premarket_window(now=None):
    """Use the premarket mover source from the user's 13:30 IST reset to the open."""
    now = now or dt.datetime.now(IST)
    local = now.astimezone(IST)
    eastern = now.astimezone(ET)
    return (local.time() >= dt.time(13, 30) and
            eastern.weekday() < 5 and eastern.time() < dt.time(9, 30))


def _load_tc2000_scanner_symbols():
    """Load the exact US Common Stocks universe exported from the user's TC2000 list."""
    path = ROOT_DIR / "us_common_stocks.txt"
    if not path.is_file():
        raise FileNotFoundError(f"TC2000 universe file is missing: {path}")
    symbols = []
    seen = set()
    with path.open("r", encoding="utf-8-sig") as source:
        for line in source:
            symbol = line.strip().upper()
            if not symbol or symbol.startswith("#") or symbol == "SYMBOLS FROM TC2000":
                continue
            if not re.fullmatch(r"[A-Z][A-Z0-9.-]*", symbol):
                continue
            if symbol not in seen:
                symbols.append(symbol)
                seen.add(symbol)
    if not symbols:
        raise RuntimeError(f"TC2000 universe file contains no symbols: {path}")
    return symbols


def _alpaca_premarket_volumes(tickers, session_date):
    """Return cumulative 04:00–09:30 ET volume for candidate symbols from Alpaca bars."""
    key = os.getenv("ALPACA_API_KEY")
    secret = os.getenv("ALPACA_API_SECRET")
    if not key or not secret:
        raise RuntimeError("Alpaca API credentials are not configured")

    start_et = dt.datetime.combine(dt.date.fromisoformat(session_date), dt.time(4, 0), tzinfo=ET)
    start_utc = start_et.astimezone(dt.timezone.utc)
    headers = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}
    feed = os.getenv("ALPACA_FEED", "sip")
    volumes = {ticker: 0.0 for ticker in tickers}
    batch_size = 40
    for offset in range(0, len(tickers), batch_size):
        params = {
            "symbols": ",".join(tickers[offset:offset + batch_size]),
            "timeframe": "1Min",
            "start": start_utc.isoformat().replace("+00:00", "Z"),
            "limit": 10000,
            "feed": feed,
            "adjustment": "raw",
            "sort": "asc",
        }
        while True:
            response = requests.get("https://data.alpaca.markets/v2/stocks/bars",
                                    headers=headers, params=params, timeout=20)
            if response.status_code != 200:
                raise RuntimeError(f"Alpaca bars request failed ({response.status_code}): "
                                   f"{response.text[:300]}")
            payload = response.json()
            for ticker, bars in (payload.get("bars") or {}).items():
                for bar in bars:
                    stamp = dt.datetime.fromisoformat(bar["t"].replace("Z", "+00:00")).astimezone(ET)
                    if (stamp.date() == start_et.date() and
                            dt.time(4, 0) <= stamp.time() < dt.time(9, 30)):
                        volumes[ticker] = volumes.get(ticker, 0.0) + max(0.0, float(bar.get("v") or 0))
            page_token = payload.get("next_page_token")
            if not page_token:
                break
            params["page_token"] = page_token
    return volumes


def _yahoo_scanner_rows():
    """Fetch Yahoo's free gainers screener and apply the scanner's local filters."""
    query = yf.EquityQuery("and", [
        yf.EquityQuery("eq", ["region", "us"]),
        yf.EquityQuery("gt", ["percentchange", 40]),
    ])

    rows = []
    offset = 0
    page_size = 250
    while offset < 5000:
        result = yf.screen(query, offset=offset, size=page_size,
                           sortField="percentchange", sortAsc=False)
        quotes = result.get("quotes") or []
        for quote in quotes:
            state = str(quote.get("marketState") or "").upper()
            et_now = dt.datetime.now(ET)
            premarket = (state in ("PRE", "PREPRE") or
                         dt.time(4, 0) <= et_now.time() < dt.time(9, 30))
            postmarket = (not premarket and
                          (state in ("POST", "POSTPOST") or
                           dt.time(16, 0) <= et_now.time() < dt.time(20, 0)))
            if premarket:
                # The screener's percentchange/dayvolume fields describe the
                # regular session and can belong to yesterday. Require a
                # timestamped quote and volume from today's premarket instead.
                price = quote.get("preMarketPrice")
                volume = quote.get("preMarketVolume")
                quote_time = quote.get("preMarketTime")
                if not price or not volume or not quote_time:
                    continue
                try:
                    quote_date = dt.datetime.fromtimestamp(float(quote_time), ET).date()
                except (TypeError, ValueError, OSError, OverflowError):
                    continue
                if quote_date != et_now.date():
                    continue
            elif postmarket:
                price = quote.get("postMarketPrice") or quote.get("regularMarketPrice") or quote.get("intradayprice")
                volume = quote.get("postMarketVolume") or quote.get("regularMarketVolume") or quote.get("dayvolume")
            else:
                price = quote.get("regularMarketPrice") or quote.get("intradayprice")
                volume = quote.get("regularMarketVolume") or quote.get("dayvolume")
            previous_close = quote.get("regularMarketPreviousClose") or quote.get("previousClose")
            try:
                price = float(price)
                previous_close = float(previous_close)
                volume = float(volume)
            except (TypeError, ValueError):
                continue
            if price <= 0 or previous_close <= 0:
                continue

            gap_pct = (price / previous_close - 1) * 100
            dollar_volume = price * volume
            ticker = str(quote.get("symbol") or "").upper()
            if ticker and 0.40 < price < 20 and gap_pct > 40 and dollar_volume >= 500_000:
                rows.append({
                    "ticker": ticker,
                    "price": round(price, 4),
                    "previous_close": round(previous_close, 4),
                    "gap_pct": round(gap_pct, 2),
                    "dollar_volume": round(dollar_volume),
                    "volume": round(volume),
                })

        offset += len(quotes)
        total = result.get("total")
        if not quotes or len(quotes) < page_size or (total is not None and offset >= int(total)):
            break

    rows.sort(key=lambda row: (-row["gap_pct"], -row["dollar_volume"], row["ticker"]))
    return rows


def _yahoo_all_premarket_rows(session_date):
    """Check today's premarket bars for every symbol in the supplied TC2000 universe."""
    scan_started = time.monotonic()
    symbols = _load_tc2000_scanner_symbols()
    total = len(symbols)
    with _scanner_cache_lock:
        _scanner_scan_state.update(done=0, total=1, symbols_done=0,
                                   symbols_total=total, universe_total=total,
                                   phase="history", error="")
    _scanner_log(f"TC2000 US Common Stocks scan started for {session_date}; "
                 f"universe contains {total} unique symbols; testing all symbols via Yahoo 1m history")

    with _scanner_cache_lock:
        _scanner_scan_state["done"] = 0

    today = dt.date.fromisoformat(session_date)
    candidates = {}
    counts = {"history_with_bars": 0, "history_without_today": 0,
              "history_without_previous_close": 0, "history_errors": 0,
              "history_not_gap": 0, "history_error_types": {}, "alpaca_errors": 0}

    def scan_symbol(ticker):
        try:
            yahoo_ticker = ticker.replace(".", "-")
            bars = yf.Ticker(yahoo_ticker).history(period="2d", interval="1m", prepost=True,
                                             auto_adjust=False, timeout=10)
            if bars is None or bars.empty or "Close" not in bars or bars.index.tz is None:
                return ticker, None, "history_without_today"
            eastern_index = bars.index.tz_convert(ET)
            current_mask = eastern_index.date == today
            current = bars.loc[current_mask]
            if current.empty:
                return ticker, None, "history_without_today"
            times = current.index.tz_convert(ET).time
            current = current.loc[[dt.time(4, 0) <= t < dt.time(9, 30) for t in times]]
            if current.empty:
                return ticker, None, "history_without_today"
            previous_mask = [(d < today and dt.time(9, 30) <= t < dt.time(16, 0))
                             for d, t in zip(eastern_index.date, eastern_index.time)]
            previous = bars.loc[previous_mask]
            previous_close = float(previous["Close"].iloc[-1]) if not previous.empty else None
            if previous_close is None or previous_close <= 0:
                return ticker, None, "history_without_previous_close"
            price = float(current["Close"].iloc[-1])
            if price <= 0:
                return ticker, None, "history_without_today"
            gap_pct = (price / previous_close - 1) * 100
            if 0.40 < price < 20 and gap_pct > 40:
                return ticker, {"ticker": ticker, "price": round(price, 4),
                                "previous_close": round(previous_close, 4),
                                "gap_pct": round(gap_pct, 2)}, "candidate"
            return ticker, None, "history_not_gap"
        except Exception as exc:
            return ticker, None, ("history_error", type(exc).__name__)

    batch_size = 240
    workers = 6
    for start in range(0, total, batch_size):
        batch = symbols[start:start + batch_size]
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="scanner-yahoo-history") as pool:
            futures = [pool.submit(scan_symbol, ticker) for ticker in batch]
            for future in as_completed(futures):
                ticker, row, outcome = future.result()
                if row:
                    candidates[ticker] = row
                elif isinstance(outcome, tuple) and outcome[0] == "history_error":
                    counts["history_errors"] += 1
                    error_type = outcome[1]
                    counts["history_error_types"][error_type] = counts["history_error_types"].get(error_type, 0) + 1
                elif outcome == "history_without_today":
                    counts["history_without_today"] += 1
                elif outcome == "history_without_previous_close":
                    counts["history_without_previous_close"] += 1
                else:
                    counts["history_not_gap"] += 1
                counts["history_with_bars"] += outcome in ("history_not_gap", "candidate")
                with _scanner_cache_lock:
                    _scanner_scan_state["symbols_done"] += 1
                    done = _scanner_scan_state["symbols_done"]
                if done % 250 == 0 or done == total:
                    _scanner_log(f"Yahoo history tested {done}/{total} TC2000 tickers; "
                                 f"Yahoo gap candidates={len(candidates)}; "
                                 f"request errors={counts['history_errors']}")

    _scanner_log(f"Yahoo gap scan finished; universe={total}, "
                 f"histories tested={_scanner_scan_state['symbols_done']}, "
                 f"histories with today's PM bars={counts['history_with_bars']}, "
                 f"no current bars={counts['history_without_today']}, "
                 f"no previous close={counts['history_without_previous_close']}, "
                 f"request errors={counts['history_errors']} {counts['history_error_types']}, "
                 f"nonmatching tickers={counts['history_not_gap']}, "
                 f"gap candidates={len(candidates)}")

    with _scanner_cache_lock:
        _scanner_scan_state.update(phase="alpaca", candidates=len(candidates))
    _scanner_log(f"Checking Alpaca {os.getenv('ALPACA_FEED', 'sip')} premarket volume "
                 f"for {len(candidates)} Yahoo gap candidates only")
    rows = []
    if candidates:
        try:
            volumes = _alpaca_premarket_volumes(list(candidates), session_date)
            for ticker, candidate in candidates.items():
                volume = float(volumes.get(ticker, 0.0))
                dollar_volume = candidate["price"] * volume
                if volume > 0 and dollar_volume >= 500_000:
                    rows.append({**candidate, "volume": round(volume),
                                 "dollar_volume": round(dollar_volume)})
                else:
                    _scanner_log(f"Filtered {ticker}: Alpaca premarket volume={volume:,.0f}; "
                                 f"dollar volume=${dollar_volume:,.0f}")
        except Exception as exc:
            counts["alpaca_errors"] = len(candidates)
            _scanner_log(f"Alpaca premarket volume check failed for {len(candidates)} candidates: "
                         f"{type(exc).__name__}: {exc}")
    rows.sort(key=lambda row: (-row["gap_pct"], -row["dollar_volume"], row["ticker"]))
    _scanner_log(f"TC2000 hybrid scan finished in {time.monotonic() - scan_started:.1f}s; "
                 f"universe={total}, Yahoo histories checked={_scanner_scan_state['symbols_done']}, "
                 f"Yahoo candidates={len(candidates)}, Alpaca volume errors={counts['alpaca_errors']}, "
                 f"qualified matches={len(rows)}")
    with _scanner_cache_lock:
        issues = []
        if counts["history_errors"]:
            issues.append(f"Yahoo history failed for {counts['history_errors']}/{total} symbols")
        if counts["alpaca_errors"]:
            issues.append(f"Alpaca volume failed for {counts['alpaca_errors']}/{len(candidates)} candidates")
        _scanner_scan_state["error"] = "; ".join(issues)
        _scanner_scan_state["done"] = 1
    for row in rows:
        _scanner_log(f"MATCH {row['ticker']} gap=+{row['gap_pct']:.1f}% "
                     f"price=${row['price']:.2f} volume={row['volume']:,} "
                     f"dollar_volume=${row['dollar_volume']:,}")
    return rows


def _refresh_yahoo_premarket_universe(session_date):
    """Scan Yahoo's US screener pages and publish the current premarket gap list."""
    try:
        rows = _yahoo_all_premarket_rows(session_date)
        with _scanner_cache_lock:
            if (_scanner_cache.get("session_date") == session_date and
                    _scanner_cache.get("source") == "premarket"):
                _scanner_cache.update(at=time.time(), rows=rows)
    except Exception as exc:
        with _scanner_cache_lock:
            _scanner_scan_state["error"] = str(exc)
        _scanner_log(f"Yahoo premarket scan failed for {session_date}: {type(exc).__name__}: {exc}")
    finally:
        with _scanner_cache_lock:
            if _scanner_scan_state.get("session_date") == session_date:
                _scanner_scan_state["running"] = False
                _scanner_scan_state["completed_at"] = time.time()
                _scanner_log(f"Yahoo premarket scan finished for {session_date}; "
                             f"tickers checked={_scanner_scan_state['symbols_done']}/"
                             f"{_scanner_scan_state['symbols_total']}, "
                             f"matches={len(_scanner_cache['rows'])}, "
                             f"error={_scanner_scan_state['error'] or 'none'}")


_scanner_details = {}
_scanner_details_lock = threading.RLock()
_scanner_details_inflight = set()
_scanner_details_pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="scanner-details")


def _normalize_yahoo_news(article, kind):
    content = article.get("content") if isinstance(article.get("content"), dict) else article
    title = content.get("title") or content.get("storytitle") or article.get("title")
    if not title:
        return None
    link = content.get("canonicalUrl") or content.get("clickThroughUrl") or article.get("link") or article.get("url")
    if isinstance(link, dict):
        link = link.get("url")
    provider = content.get("provider") or article.get("publisher") or {}
    if isinstance(provider, dict):
        provider = provider.get("displayName") or provider.get("name") or "Yahoo Finance"
    published = content.get("pubDate") or content.get("providerPublishTime") or article.get("pubDate") or article.get("providerPublishTime")
    if isinstance(published, (int, float)):
        published = dt.datetime.fromtimestamp(published, dt.timezone.utc).isoformat()
    elif published is not None:
        published = str(published)
    return {"title": str(title).strip(), "url": str(link or ""), "publisher": str(provider),
            "published": published or "", "kind": kind}


def _fetch_yahoo_news(ticker):
    items = []
    errors = []
    press_wire_pattern = re.compile(
        r"globe.?newswire|pr\s*newswire|business\s*wire|newsfile|access\s*newswire|"
        r"the.?newswire|newsdirect|eqs.?news|cnw.?group", re.I
    )
    # Yahoo's ticker search returns issuer wire releases; appending "press
    # release" can produce an empty result even when the ticker search has PRs.
    for query in (ticker, f"{ticker} press release"):
        try:
            search = yf.Search(query=query, news_count=20,
                               enable_fuzzy_query=False, timeout=8)
            for article in (search.news or []):
                content = article.get("content") if isinstance(article.get("content"), dict) else {}
                provider = content.get("provider") or article.get("publisher") or ""
                if isinstance(provider, dict):
                    provider = provider.get("displayName") or provider.get("name") or ""
                if not press_wire_pattern.search(str(provider)):
                    continue
                item = _normalize_yahoo_news(article, "Press release")
                if item:
                    items.append(item)
        except Exception as exc:
            errors.append(f"Yahoo '{query}': {type(exc).__name__}: {exc}")
        if len(items) >= 2:
            break

    # Search may omit the ticker's own Yahoo Finance news feed, so also inspect
    # Yahoo's ticker news endpoint before deciding that no releases are present.
    if len(items) < 2:
        try:
            for article in (yf.Ticker(ticker.replace(".", "-")).news or []):
                content = article.get("content") if isinstance(article.get("content"), dict) else article
                provider = content.get("provider") or article.get("publisher") or ""
                if isinstance(provider, dict):
                    provider = provider.get("displayName") or provider.get("name") or ""
                if not press_wire_pattern.search(str(provider)):
                    continue
                item = _normalize_yahoo_news(article, "Press release")
                if item:
                    items.append(item)
        except Exception as exc:
            errors.append(f"Yahoo ticker news: {type(exc).__name__}: {exc}")

    unique, seen = [], set()
    for item in items:
        # Yahoo search and its ticker feed can return the same wire release;
        # deduplicate on normalized headline, falling back to the URL.
        key = re.sub(r"\W+", " ", item["title"].lower()).strip()
        if not key:
            key = item["url"]
        if key not in seen:
            unique.append(item)
            seen.add(key)
    def published_at(item):
        try:
            stamp = pd.Timestamp(item.get("published"))
            if stamp.tzinfo is None:
                stamp = stamp.tz_localize("UTC")
            return stamp.timestamp()
        except Exception:
            return 0
    unique.sort(key=published_at, reverse=True)
    now_ts = time.time()
    # Show the latest dated releases regardless of age; ignore undated and
    # future-dated items so every displayed release can carry a useful date tag.
    unique = [item for item in unique if 0 < published_at(item) <= now_ts]
    if not unique:
        app.logger.warning("No press releases found for %s. News fetch diagnostics: %s",
                           ticker, " | ".join(errors) if errors else "sources returned no matching press releases")
    return unique[:2]


def _get_scanner_news(ticker):
    now = time.time()
    with _scanner_details_lock:
        cached = _scanner_details.get(ticker) or {}
        # Keep the newest two releases current; a five-minute cache made new
        # releases invisible to the scanner for too long.
        news_ttl = 60
        if now - cached.get("news_at", 0) < news_ttl:
            return list(cached.get("news") or [])
    news = _fetch_yahoo_news(ticker)
    with _scanner_details_lock:
        cached = _scanner_details.setdefault(ticker, {})
        cached.update(news=news, news_at=now,
                      news_status=("Latest press releases found on Yahoo Finance" if news else
                                   "No press releases found on Yahoo Finance"))
    return news


def _scanner_approaches(ticker, previous_close):
    """Return first-pullback approach alerts before price crosses VWAP/21 EMA."""
    bars = yf.Ticker(ticker.replace(".", "-")).history(
        period="1d", interval="1m", prepost=True, auto_adjust=False
    )
    if bars is None or bars.empty:
        return []
    if bars.index.tz is None:
        bars.index = bars.index.tz_localize(ET)
    else:
        bars.index = bars.index.tz_convert(ET)
    bars = bars.rename(columns={"Open": "o", "High": "h", "Low": "l", "Close": "c", "Volume": "v"})
    bars = bars[["o", "h", "l", "c", "v"]].dropna(subset=["c"])
    latest_day = bars.index[-1].date()
    minute_of_day = bars.index.hour * 60 + bars.index.minute
    bars = bars[(bars.index.date == latest_day) & (minute_of_day >= 240) & (minute_of_day < 1200)]
    if len(bars) < 3:
        return []
    price = float(bars.c.iloc[-1])
    high_since_open = float(bars.h.max())
    try:
        previous_close = float(previous_close)
    except (TypeError, ValueError):
        previous_close = float(bars.o.iloc[0])
    close_values = bars.c.to_numpy()
    rally_pullback = (previous_close > 0 and high_since_open >= previous_close * 1.02
                      and price < high_since_open * 0.999
                      and close_values[-1] <= close_values[-2])
    if not rally_pullback:
        return []

    previous_price = float(close_values[-2])
    levels = []
    typical = (bars.h + bars.l + bars.c) / 3
    total_volume = bars.v.clip(lower=0).cumsum()
    weighted_volume = (typical * bars.v.clip(lower=0)).cumsum()
    if total_volume.iloc[-1] > 0:
        vwap_series = weighted_volume / total_volume.replace(0, np.nan)
        levels.append(("VWAP", "approaching VWAP from above",
                       float(vwap_series.iloc[-1]), float(vwap_series.iloc[-2])))

    alerts = []
    for code, label, level, previous_level in levels:
        if level > 0 and previous_level > 0 and price > level and previous_price > previous_level:
            distance_pct = (price - level) / price * 100
            previous_distance_pct = (previous_price - previous_level) / previous_price * 100
            # Alert only on a genuine pullback that is getting closer while still
            # holding above support; the browser applies each user's threshold.
            if 0 <= distance_pct <= 10.0 and previous_distance_pct > distance_pct:
                alerts.append({"code": code, "label": label, "level": round(level, 4),
                               "distance_pct": round(distance_pct, 2),
                               "previous_distance_pct": round(previous_distance_pct, 2)})
    return alerts


def _refresh_scanner_details(ticker, previous_close):
    try:
        now = time.time()
        with _scanner_details_lock:
            cached = _scanner_details.setdefault(ticker, {})
            technical_at = cached.get("technical_at", 0)
            news_at = cached.get("news_at", 0)
            news_ttl = 60
        if now - technical_at >= 20:
            try:
                approaches = _scanner_approaches(ticker, previous_close)
            except Exception:
                approaches = []
            with _scanner_details_lock:
                _scanner_details.setdefault(ticker, {}).update(approaches=approaches, technical_at=now)
        if now - news_at >= news_ttl:
            try:
                _get_scanner_news(ticker)
            except Exception:
                pass
    finally:
        with _scanner_details_lock:
            _scanner_details_inflight.discard(ticker)


def _queue_scanner_details(row):
    ticker = row["ticker"]
    now = time.time()
    with _scanner_details_lock:
        cached = _scanner_details.setdefault(ticker, {})
        due = (now - cached.get("technical_at", 0) >= 20
               or now - cached.get("news_at", 0) >= 60)
        if not due or ticker in _scanner_details_inflight:
            return
        _scanner_details_inflight.add(ticker)
    _scanner_details_pool.submit(_refresh_scanner_details, ticker, row.get("previous_close"))


@app.route("/api/scanner")
def scanner_market_scan():
    return jsonify({"error": "Scanner has been removed."}), 404
    user = (request.args.get("user") or "").strip().lower()
    token = (request.args.get("token") or "").strip()
    if not user or not token or not _db_validate_session(user, token):
        return jsonify({"error": "Session expired. Please sign in again."}), 401
    if not _get_app_ui_visibility(user).get("scanner", True):
        return jsonify({"error": "Scanner access is disabled for this account."}), 403

    now = time.time()
    session_date = _scanner_session_date()
    premarket_scan = _scanner_is_premarket_window()
    with _scanner_cache_lock:
        if _scanner_cache.get("session_date") != session_date:
            _scanner_cache.update(at=0.0, rows=[], session_date=session_date, source="")
        if premarket_scan:
            scan_due = (_scanner_scan_state.get("session_date") != session_date or
                        (not _scanner_scan_state.get("running") and
                         now - _scanner_scan_state.get("completed_at", 0) >= _SCANNER_RESCAN_SECONDS))
            _scanner_cache["source"] = "premarket"
            if scan_due:
                _scanner_scan_state.update(running=True, session_date=session_date,
                                           done=0, total=0, symbols_done=0, symbols_total=0,
                                           universe_total=0, phase="history", candidates=0, error="")
                _scanner_scan_pool.submit(_refresh_yahoo_premarket_universe, session_date)
        elif (_scanner_cache.get("source") != "regular" or
              now - _scanner_cache["at"] >= 25):
            try:
                rows = _yahoo_scanner_rows()
                _scanner_cache.update(at=now, rows=rows, session_date=session_date, source="regular")
            except Exception as exc:
                return jsonify({"error": str(exc)}), 502
        output = []
        for row in _scanner_cache["rows"]:
            _queue_scanner_details(row)
            with _scanner_details_lock:
                details = dict(_scanner_details.get(row["ticker"]) or {})
            output.append({**row,
                           "news": details.get("news") or [],
                           "news_status": details.get("news_status"),
                           "approaches": details.get("approaches") or []})
        return jsonify({"tickers": output, "updated": _scanner_cache["at"],
                        "session_date": session_date,
                        "scanning": premarket_scan and _scanner_scan_state.get("running", False),
                        "scan_done": _scanner_scan_state.get("done", 0),
                        "scan_total": _scanner_scan_state.get("total", 0),
                        "scan_symbols_done": _scanner_scan_state.get("symbols_done", 0),
                        "scan_symbols_total": _scanner_scan_state.get("symbols_total", 0),
                        "scan_universe_total": _scanner_scan_state.get("universe_total", 0),
                        "scan_phase": _scanner_scan_state.get("phase", ""),
                        "scan_candidates": _scanner_scan_state.get("candidates", 0),
                        "scan_error": _scanner_scan_state.get("error", "")})


HTML = r"""<!doctype html>
<html><head><meta charset="utf-8"><title>Gap-Up Fade Analyzer</title>
<link rel="icon" type="image/svg+xml" href="/favicon.svg?app=gapup">
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
:root{--g:#3ddc84;--r:#ff5a5a;--y:#ffd23f;--b:#6cc4ff;--c:#29a8ff;--w:#e8e8e8;--d:#8a8a8a;--line:#333}
*{box-sizing:border-box}
body{margin:0;background:#000;color:var(--w);font:15px/1.45 Consolas,"SF Mono","Roboto Mono",Menlo,monospace}
.bar{display:flex;gap:12px;padding:20px 34px;flex-wrap:wrap;align-items:center}
input{background:#0d0d0d;border:1px solid var(--line);color:#fff;font:inherit;font-size:20px;padding:0 14px;height:48px;width:300px}
button{background:#0d0d0d;border:1px solid var(--line);color:#fff;font:inherit;font-size:18px;padding:0 16px;height:48px;cursor:pointer}
button:hover{background:#1a1a1a}
select{background:#0d0d0d;border:1px solid var(--line);color:#fff;font:inherit;font-size:13px;padding:2px 8px;margin-left:14px}
#out{margin:0 34px 40px;border:1px solid #222;padding:20px 22px}
.g{color:var(--g)}.r{color:var(--r)}.y{color:var(--y)}.b{color:var(--b)}.c{color:var(--c)}.w{color:var(--w)}.d{color:var(--d)}
.big{font-size:20px;font-weight:bold}
.badge{display:inline-block;min-width:54px;text-align:center;border:2px solid currentColor;padding:0 10px;font-size:24px;font-weight:bold;margin:0 6px}
.ttl{color:var(--c);margin-bottom:6px}
.panel{border:1px solid var(--line);padding:10px 14px;margin-top:18px}
.two{display:grid;grid-template-columns:1fr 1fr;padding:0}
.two>div{padding:10px 14px}.two>div+div{border-left:1px solid var(--line);padding-left:60px}
.three{display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px;margin-top:18px}
.three .panel{margin-top:0}
.kv{display:grid;grid-template-columns:max-content 1fr;column-gap:34px;row-gap:2px}
.kv span:nth-child(odd){white-space:pre}
.scroll{overflow-x:auto}
table{border-collapse:collapse;width:100%}
th{text-align:left;font-weight:normal;color:var(--w);padding:3px 14px 5px 0;border-bottom:1px solid var(--line);white-space:nowrap}
table.sort th{cursor:pointer}table.sort th:hover{color:var(--c)}
td{padding:2px 14px 2px 0;white-space:nowrap}
tr.dim td{opacity:.4}
.foot{color:var(--d);font-size:12px;margin-top:6px}
canvas{display:block;width:100%}
.evgrid{display:grid;grid-template-columns:1fr 1fr;gap:14px}
.evc{border:1px solid #222;padding:8px 10px 30px;min-width:0;position:relative;cursor:pointer;transition:border-color 0.15s, background 0.15s}
.evc:hover{border-color:#29a8ff;background:#0d0d0d}
.ev_zoom_hint{position:absolute;top:6px;right:8px;background:rgba(18,18,18,0.92);border:1px solid #29a8ff;color:#29a8ff;font-size:11px;padding:2px 7px;border-radius:3px;opacity:0;transition:opacity 0.15s;pointer-events:none;z-index:2}
.evc:hover .ev_zoom_hint{opacity:1}
.eventDateTag{display:inline-flex;align-items:center;border:1px solid #34526a;border-radius:3px;background:#0d151c;color:#a9d8f4;padding:1px 6px;font-size:10px;line-height:16px;white-space:nowrap}
.evt{font-size:13px;margin-bottom:2px}
.evhead{display:flex;align-items:center;justify-content:space-between;gap:6px;min-height:24px;margin-bottom:2px}
.evhead .evt{margin:0;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.evcontrols{display:flex;align-items:center;gap:3px;flex:none}
.evnav{width:22px;height:21px;padding:0;font-size:15px;line-height:18px;border:1px solid #333;color:#aaa;background:#101010}
.evnav:hover{color:#6cc4ff;border-color:#29a8ff}
.evtf{height:21px;margin:0 0 0 2px;padding:0 3px;font-size:11px;border-color:#333;color:#aaa;background:#101010}
.tag{border:1px solid var(--y);color:var(--y);padding:0 6px;font-size:11px;margin-left:8px}
@media(max-width:1100px){.three{grid-template-columns:1fr}.two{grid-template-columns:1fr}.two>div+div{border-left:0;padding-left:14px}.evgrid{grid-template-columns:1fr}}
#ag_tip{position:fixed;z-index:2147483647;background:#111;border:1px solid #444;color:#e0e0e0;font-size:12px;padding:7px 11px;border-radius:4px;max-width:300px;line-height:1.5;pointer-events:none;opacity:0;transition:opacity 0.15s;box-shadow:0 4px 16px rgba(0,0,0,0.7);white-space:pre-wrap;display:block}
#ag_tip.visible{opacity:1}
[data-tip]{cursor:help}
</style>
</style></head><body>
<div id="ag_tip"></div>
<!-- Access Gate Modal -->
<div id="gate" style="position:fixed;inset:0;background:#000;z-index:9999;display:flex;align-items:center;justify-content:center;padding:20px">
  <div style="background:#0c0c0c;border:1px solid #2a2a2a;padding:36px 32px;width:420px;max-width:92vw;text-align:center;box-shadow:0 12px 40px rgba(0,0,0,0.95)">
    <div style="font-size:22px;font-weight:bold;color:#29a8ff;margin-bottom:4px">Gap-Up Fade Terminal</div>
    <div id="gate_subtitle" style="color:#8a8a8a;font-size:14px;margin-bottom:24px">Authorized access only. Enter your name:</div>

    <!-- Step 1: Name -->
    <div id="gate_step_name">
      <input id="gate_name" placeholder="Enter your name" style="width:100%;height:46px;font-size:17px;margin-bottom:18px;padding:0 14px;background:#000;border:1px solid #444;color:#fff;text-align:center;box-sizing:border-box" autofocus spellcheck="false" autocomplete="off">
      <button id="gate_btn" style="width:100%;height:46px;font-size:16px;background:#141414;border:1px solid #29a8ff;color:#29a8ff;cursor:pointer;font-weight:bold">Continue →</button>
    </div>

    <!-- Step 2: Create Password -->
    <div id="gate_step_create" style="display:none">
      <div style="font-size:13px;color:#aaa;margin-bottom:14px">Welcome! Create a password for your account:</div>
      <input id="gate_new_pwd" type="password" placeholder="New password" style="width:100%;height:44px;font-size:15px;margin-bottom:10px;padding:0 14px;background:#000;border:1px solid #444;color:#fff;text-align:center;box-sizing:border-box" autocomplete="new-password">
      <input id="gate_new_pwd2" type="password" placeholder="Confirm password" style="width:100%;height:44px;font-size:15px;margin-bottom:18px;padding:0 14px;background:#000;border:1px solid #444;color:#fff;text-align:center;box-sizing:border-box" autocomplete="new-password">
      <button id="gate_create_btn" style="width:100%;height:46px;font-size:16px;background:#0a1a0a;border:1px solid #3ddc84;color:#3ddc84;cursor:pointer;font-weight:bold">Create Password & Enter</button>
    </div>

    <!-- Step 3: Enter Password -->
    <div id="gate_step_login" style="display:none">
      <div style="font-size:13px;color:#aaa;margin-bottom:14px">Enter your password to continue:</div>
      <input id="gate_pwd" type="password" placeholder="Password" style="width:100%;height:44px;font-size:18px;margin-bottom:18px;padding:0 14px;background:#000;border:1px solid #444;color:#fff;text-align:center;box-sizing:border-box" autocomplete="current-password">
      <!-- Force logout banner (shown when another session is active) -->
      <div id="gate_force_box" style="display:none;background:#1a1008;border:1px solid #ff9900;padding:12px 14px;margin-bottom:14px;text-align:left;font-size:12px;color:#ffb84d;line-height:1.5">
        ⚠ This account is already logged in on another browser.<br>
        <label style="display:flex;align-items:center;gap:8px;margin-top:8px;cursor:pointer;color:#fff;font-size:13px">
          <input type="checkbox" id="gate_force_chk" style="width:16px;height:16px">
          Log out from all other browsers and continue here
        </label>
      </div>
      <button id="gate_login_btn" style="width:100%;height:46px;font-size:16px;background:#141414;border:1px solid #29a8ff;color:#29a8ff;cursor:pointer;font-weight:bold">Enter Terminal</button>
      <div style="margin-top:10px">
        <span style="font-size:12px;color:#555;cursor:pointer" onclick="gateBack()">← Back</span>
      </div>
    </div>

    <div id="gate_spinner" style="display:none;color:#888;font-size:13px;margin-top:14px">Checking…</div>
    <div id="gate_err" style="color:#ff5a5a;font-size:13px;margin-top:14px;display:none"></div>
  </div>
</div>

<!-- Main Application -->
<div id="app_main" style="display:none">
  <div class="bar">
    <input id="t" placeholder="ticker" autocomplete="off" spellcheck="false">
    <button id="go">Get notes</button>
    <button id="rf">Refresh</button>
    <button id="cp">Copy all</button>

    <div style="margin-left:auto;display:flex;align-items:center;gap:10px;font-size:13px;color:#8a8a8a">
      <button id="scanner_open_btn" style="display:none;height:36px;padding:0 14px;border:2px solid #29a8ff;background:#071a28;color:#6cc4ff;font-weight:bold;cursor:pointer;border-radius:4px;font-size:13px" title="Live gap-up scanner">🔎 Scanner</button>
      <button id="nav_multiday_btn" style="height:36px;padding:0 14px;border:2px solid #00c9a7;background:#002b24;color:#00c9a7;font-weight:bold;cursor:pointer;border-radius:4px;font-size:13px" title="Multi-day Rally & First Red Day (FRD) Structure Tool">📊 Runner Stats</button>
      <button id="nav_keyword_btn" style="height:36px;padding:0 14px;border:2px solid #8b74ff;background:#120f24;color:#b7a8ff;font-weight:bold;cursor:pointer;border-radius:4px;font-size:13px" title="Search SEC filings by financing player">⌕ Search Keyword</button>
      <span>User: <span id="u_name" style="color:#29a8ff;font-weight:bold"></span><span id="u_badge"></span></span>
      <button id="logout_btn" style="height:34px;font-size:12px;padding:0 12px;border-color:#444">Exit</button>
    </div>
  </div>
  <!-- Multi-Ticker Tab Bar -->
  <div id="tab_bar" style="display:none;padding:8px 24px 0;background:#050505;border-bottom:1px solid #222;gap:6px;overflow-x:auto;align-items:center;flex-wrap:nowrap"></div>
  <div id="tabs_view">
    <div id="out" style="padding:24px 34px"><span class="d">Enter a ticker that is gapping up 30%+ and press Get notes.</span></div>
  </div>
  <!-- Multiday Runner Stats Embedded View on Main Screen -->
  <div id="multiday_view" style="display:none;width:100%;background:#000;position:relative">
    <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 34px;background:#08080a;border-bottom:1px solid #222">
      <div style="font-size:14px;color:#00c9a7;font-weight:bold;display:flex;align-items:center;gap:8px">
        <span>📊 Multiday Rally & First Red Day (FRD) Stats</span>
        <span style="color:#888;font-weight:normal;font-size:12px">— 15min Interactive Structure & Leg Drawing</span>
      </div>
      <button id="open_multiday_newtab" style="height:30px;font-size:12px;padding:0 12px;border:1px solid #444;background:#111;color:#ccc;cursor:pointer;border-radius:2px" title="Open in separate browser window">⛶ Open in New Tab</button>
    </div>
    <iframe id="runner_iframe" src="/runner" style="width:100%;height:calc(100vh - 120px);border:0;background:#000;display:block"></iframe>
  </div>
</div>

<!-- Live Gap-Up Scanner -->
<div id="scanner_modal" style="position:fixed;top:96px;right:24px;z-index:90000;display:none;width:min(1280px,calc(100vw - 48px));pointer-events:none">
  <section style="width:100%;max-height:82vh;display:flex;flex-direction:column;background:#090b0e;border:1px solid #29a8ff;border-radius:6px;box-shadow:0 12px 40px rgba(0,0,0,.8);pointer-events:auto;overflow:hidden">
    <header style="display:flex;align-items:center;gap:18px;flex-wrap:wrap;padding:15px 18px;border-bottom:1px solid #252b31">
      <div style="font-size:17px;font-weight:bold;color:#6cc4ff">🔎 Live Gap-Up Scanner</div>
      <div style="font-size:12px;color:#999">Price &gt;$0.40 and &lt;$20 · Gap &gt;40% · Dollar volume ≥$500K</div>
      <div style="margin-left:auto;display:flex;align-items:center;gap:16px;flex-wrap:wrap">
        <label style="display:flex;align-items:center;gap:8px;color:#ffd23f;font-size:13px;cursor:pointer"><input id="scanner_alarm" type="checkbox" style="width:17px;height:17px"> Voice alerts</label>
      </div>
      <button id="scanner_refresh_btn" style="height:32px;font-size:13px;padding:0 12px">Refresh</button>
      <button id="scanner_close_btn" style="height:32px;font-size:13px;padding:0 12px" onclick="closeScanner()">Close</button>
    </header>
    <div style="display:flex;align-items:center;gap:16px;flex-wrap:wrap;padding:10px 18px;background:#0d1014;border-bottom:1px solid #252b31;font-size:12px">
      <label style="display:flex;align-items:center;gap:5px"><input id="scanner_vwap_alert" type="checkbox" style="width:15px;height:15px"> VWAP</label>
      <span id="scanner_settings_status" style="margin-left:auto;color:#777">Settings save per user</span>
    </div>
    <div style="padding:10px 18px;color:#888;font-size:12px;display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap">
      <span id="scanner_status">Waiting for scan…</span><span>Yahoo premarket prices · Alpaca SIP volume (up to 15-min delay) · Click a ticker to load its notes</span>
    </div>
    <div style="overflow:auto;padding:0 18px 18px">
      <table style="width:100%;border-collapse:collapse"><thead><tr style="color:#aaa;text-align:left;border-bottom:1px solid #333">
        <th style="padding:8px">Ticker</th><th style="padding:8px;text-align:right">Price</th><th style="padding:8px;text-align:right">Gap</th><th style="padding:8px;text-align:right">Volume</th><th style="padding:8px;text-align:right">$ Volume</th>
      </tr></thead><tbody id="scanner_rows"><tr><td colspan="5" style="padding:18px;color:#777;text-align:center">No scan yet</td></tr></tbody></table>
    </div>
  </section>
</div>

<!-- Admin Search History View -->
<div id="admin_view" style="display:none;padding:24px 34px">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;flex-wrap:wrap;gap:16px;border-bottom:1px solid #333;padding-bottom:16px">
    <div>
      <div class="big" style="color:#29a8ff;font-size:20px">Admin Panel</div>
      <div style="color:#8a8a8a;font-size:13px;margin-top:4px">Search history audit &amp; user access management</div>
    </div>
    <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap">
      <div style="display:flex;align-items:center;gap:8px">
        <label for="admin_view_mode" style="font-size:13px;color:#aaa">View:</label>
        <select id="admin_view_mode" style="height:36px;padding:0 12px;font-size:13px;background:#111;color:#fff;border:1px solid #444;border-radius:3px;cursor:pointer">
          <option value="user" selected>User-wise (Click user to view)</option>
          <option value="ticker">Ticker-wise (Recent-wise)</option>
        </select>
      </div>
      <button id="admin_rf_btn" style="height:36px;font-size:13px;padding:0 14px">Refresh</button>
      <button id="admin_clear_btn" style="height:36px;font-size:13px;padding:0 14px;background:#1a0808;border:1px solid #ff5a5a;color:#ff5a5a;cursor:pointer;border-radius:3px;font-weight:bold;display:none" title="Delete all search logs older than 2 weeks">🗑 Clear data &gt; 2 weeks</button>
      <button id="multiday_stats_btn" style="height:36px;font-size:13px;padding:0 18px;background:#0a1a2a;border:1px solid #00c9a7;color:#00c9a7;cursor:pointer;border-radius:3px;font-weight:bold" title="Open Multi-day Rally &amp; FRD stats tool">📊 Multiday Stats</button>
      <button id="manage_users_btn" style="height:36px;font-size:13px;padding:0 18px;background:#1a1008;border:1px solid #ffd23f;color:#ffd23f;cursor:pointer;border-radius:3px;font-weight:bold" title="Add or remove authorized users">👥 Manage Users</button>
      <button id="admin_rename_btn" style="height:36px;font-size:13px;padding:0 18px;background:#1a102a;border:1px solid #a855f7;color:#d8b4fe;cursor:pointer;border-radius:3px;font-weight:bold" title="Rename your username (karthik/locke only)">✏️ Rename Username</button>
      <button id="admin_back_btn" style="height:36px;font-size:13px;padding:0 14px;border-color:#29a8ff;color:#29a8ff">Back to Terminal</button>
    </div>
  </div>
  <div id="admin_list" style="margin-top:10px"></div>
</div>

<!-- Manage Users Modal -->
<div id="users_modal" style="position:fixed;inset:0;background:rgba(0,0,0,0.9);z-index:99999;display:none;align-items:center;justify-content:center;padding:20px" onclick="if(event.target===this)closeUsersModal()">
  <div style="background:#0c0c0c;border:1px solid #ffd23f44;padding:0;width:720px;max-width:94vw;max-height:90vh;box-shadow:0 16px 50px rgba(0,0,0,0.95);border-radius:6px;overflow:auto">
    <div style="display:flex;justify-content:space-between;align-items:center;padding:16px 20px;border-bottom:1px solid #222;background:#111">
      <div style="font-size:16px;font-weight:bold;color:#ffd23f">👥 Manage Authorized Users</div>
      <button onclick="closeUsersModal()" style="background:transparent;border:none;color:#888;font-size:18px;cursor:pointer;padding:2px 6px" onmouseover="this.style.color='#fff'" onmouseout="this.style.color='#888'">✕</button>
    </div>
    <div style="padding:20px">
      <div style="font-size:12px;color:#8a8a8a;margin-bottom:14px">Admins (karthik, locke) cannot be removed. Click a username to inspect account status, alert preferences, recent searches, and set that user’s tool access.</div>
      <div id="users_list" style="margin-bottom:20px;min-height:60px"></div>
      <div id="user_details" style="display:none;margin:0 0 20px;padding:14px;background:#080808;border:1px solid #333;border-radius:4px;max-height:42vh;overflow:auto"></div>
      <div style="display:flex;gap:10px;align-items:center;border-top:1px solid #222;padding-top:16px">
        <input id="new_user_input" placeholder="New username (a-z, 0-9, _)" style="flex:1;height:40px;font-size:14px;padding:0 12px;background:#000;border:1px solid #444;color:#fff" autocomplete="off" spellcheck="false" maxlength="20">
        <button id="add_user_btn" style="height:40px;padding:0 18px;font-size:14px;background:#0a2a0a;border:1px solid #3ddc84;color:#3ddc84;cursor:pointer;border-radius:3px;font-weight:bold">+ Add</button>
      </div>
      <div id="users_modal_err" style="color:#ff5a5a;font-size:12px;margin-top:10px;min-height:18px"></div>
    </div>
  </div>
</div>

<!-- Rename Username Modal -->
<div id="rename_modal" style="position:fixed;inset:0;background:rgba(0,0,0,0.9);z-index:99999;display:none;align-items:center;justify-content:center;padding:20px" onclick="if(event.target===this)closeRenameModal()">
  <div style="background:#0c0c0c;border:1px solid #a855f7;padding:0;width:400px;max-width:92vw;box-shadow:0 16px 50px rgba(0,0,0,0.95);border-radius:6px;overflow:hidden">
    <div style="display:flex;justify-content:space-between;align-items:center;padding:16px 20px;border-bottom:1px solid #222;background:#160926">
      <div style="font-size:16px;font-weight:bold;color:#d8b4fe">✏️ Rename Username</div>
      <button onclick="closeRenameModal()" style="background:transparent;border:none;color:#888;font-size:18px;cursor:pointer;padding:2px 6px" onmouseover="this.style.color='#fff'" onmouseout="this.style.color='#888'">✕</button>
    </div>
    <div style="padding:22px 20px">
      <div style="font-size:12px;color:#8a8a8a;margin-bottom:10px">Current username: <span id="rename_curr_user" style="color:#29a8ff;font-weight:bold"></span></div>
      <div style="font-size:12px;color:#777;margin-bottom:14px">Only Karthik and Locke can rename their username. All search history and permissions will be migrated.</div>
      <input id="rename_input" placeholder="New username (a-z, 0-9, _)" style="width:100%;height:44px;font-size:15px;padding:0 14px;background:#000;border:1px solid #444;color:#fff;box-sizing:border-box;margin-bottom:14px;text-align:center" autocomplete="off" spellcheck="false" maxlength="20">
      <div style="display:flex;gap:10px">
        <button onclick="closeRenameModal()" style="flex:1;height:42px;font-size:13px;background:#1a1a1a;border:1px solid #444;color:#aaa;cursor:pointer;border-radius:3px">Cancel</button>
        <button id="rename_submit_btn" onclick="submitRename()" style="flex:1;height:42px;font-size:13px;background:#240038;border:1px solid #a855f7;color:#d8b4fe;cursor:pointer;font-weight:bold;border-radius:3px">Save &amp; Rename</button>
      </div>
      <div id="rename_err" style="color:#ff5a5a;font-size:12px;margin-top:12px;min-height:18px;text-align:center"></div>
    </div>
  </div>
</div>

<!-- Password Verification Modal -->
<div id="pwd_modal" style="position:fixed;inset:0;background:rgba(0,0,0,0.88);z-index:99999;display:none;align-items:center;justify-content:center;padding:20px">
  <div style="background:#0c0c0c;border:1px solid #333;padding:32px 28px;width:340px;max-width:92vw;text-align:center;box-shadow:0 12px 40px rgba(0,0,0,0.95)">
    <div style="font-size:18px;font-weight:bold;color:#29a8ff;margin-bottom:8px">Password Required</div>
    <div id="pwd_prompt" style="color:#8a8a8a;font-size:13px;margin-bottom:18px">Enter password:</div>
    <input type="password" id="pwd_input" placeholder="Password" style="width:100%;height:44px;font-size:18px;margin-bottom:14px;padding:0 14px;background:#000;border:1px solid #444;color:#fff;text-align:center" autocomplete="off">
    <div style="display:flex;gap:10px">
      <button id="pwd_cancel" style="flex:1;height:42px;font-size:14px;background:#1a1a1a;border:1px solid #444;color:#aaa;cursor:pointer">Cancel</button>
      <button id="pwd_submit" style="flex:1;height:42px;font-size:14px;background:#141414;border:1px solid #29a8ff;color:#29a8ff;cursor:pointer;font-weight:bold">Submit</button>
    </div>
    <div id="pwd_err" style="color:#ff5a5a;font-size:13px;margin-top:14px;display:none"></div>
  </div>
</div>

<!-- User Details Popup Modal -->
<div id="user_popup" style="position:fixed;inset:0;background:rgba(0,0,0,0.85);z-index:99999;display:none;align-items:center;justify-content:center;padding:20px" onclick="if(event.target===this)closeUserPopup()">
  <div style="background:#0e0e0e;border:1px solid #333;width:600px;max-width:94vw;max-height:85vh;display:flex;flex-direction:column;box-shadow:0 16px 50px rgba(0,0,0,0.95);border-radius:4px;overflow:hidden">
    <div style="display:flex;justify-content:space-between;align-items:center;padding:16px 20px;border-bottom:1px solid #222;background:#141414">
      <div>
        <div id="up_name" style="font-size:18px;font-weight:bold;color:#fff"></div>
        <div id="up_summary" style="font-size:12px;color:#8a8a8a;margin-top:2px"></div>
      </div>
      <button onclick="closeUserPopup()" style="background:transparent;border:none;color:#888;font-size:20px;cursor:pointer;padding:4px 8px;line-height:1" onmouseover="this.style.color='#fff'" onmouseout="this.style.color='#888'">✕</button>
    </div>
    <div id="up_content" style="padding:20px;overflow-y:auto;flex:1"></div>
    <div style="padding:12px 20px;border-top:1px solid #222;display:flex;justify-content:flex-end;background:#141414">
      <button onclick="closeUserPopup()" style="height:34px;padding:0 18px;font-size:13px;background:#202020;border:1px solid #444;color:#ddd;cursor:pointer">Close</button>
    </div>
  </div>
</div><!-- end #user_popup -->

<!-- Big Chart Zoom Modal -->
<div id="chart_modal" style="position:fixed;inset:0;background:rgba(0,0,0,0.92);z-index:99999;display:none;align-items:center;justify-content:center;padding:16px;backdrop-filter:blur(4px)" onclick="if(event.target===this)closeChartModal()">
  <div style="background:#0c0c0c;border:1px solid #333;width:98vw;max-width:1900px;height:96vh;max-height:1000px;display:flex;flex-direction:column;box-shadow:0 20px 60px rgba(0,0,0,0.95);border-radius:6px;overflow:hidden">
    <!-- Header -->
    <div style="display:flex;justify-content:space-between;align-items:center;padding:12px 18px;border-bottom:1px solid #222;background:#131313;flex-wrap:wrap;gap:10px">
      <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap">
        <span id="cm_title" style="font-size:16px;font-weight:bold;color:#29a8ff"></span>
        <span id="cm_event_date" class="eventDateTag"></span>
        <span id="cm_badge" style="font-size:12px"></span>
      </div>
      <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap">
        <select id="cm_tf" title="Chart timeframe" style="height:32px;font-size:12px;padding:0 8px;background:#181818;border:1px solid #444;color:#fff;margin:0">
          <option>1m</option><option selected>2m</option><option>5m</option><option>15m</option><option>30m</option><option value="1d">Day</option>
        </select>
        <select id="cm_days" title="Sessions shown" style="height:32px;font-size:12px;padding:0 8px;background:#181818;border:1px solid #444;color:#fff;margin:0"><option>1</option><option>3</option><option selected>5</option><option>10</option><option>20</option><option>60</option></select>
        <button id="cm_zoom_out" style="height:32px;width:32px;background:#1a1a1a;border:1px solid #444;color:#ddd;cursor:pointer">−</button><button id="cm_zoom_in" style="height:32px;width:32px;background:#1a1a1a;border:1px solid #444;color:#ddd;cursor:pointer">+</button>

        <div style="width:1px;height:20px;background:#333;margin:0 4px"></div>

        <select id="cm_sig" style="height:32px;font-size:12px;padding:0 8px;background:#181818;border:1px solid #444;color:#fff;margin:0">
          <option value="none">No signals</option>
          <option value="all">All entry models</option>
        </select>
        <label style="font-size:12px;color:#ddd;display:flex;align-items:center;gap:6px;cursor:pointer;user-select:none">
          <input type="checkbox" id="cm_bz" style="width:14px;height:14px;margin:0"> Bone zone
        </label>

        <div style="width:1px;height:20px;background:#333;margin:0 4px"></div>

        <button onclick="closeChartModal()" style="height:32px;width:34px;display:flex;align-items:center;justify-content:center;font-size:18px;background:#202020;border:1px solid #444;color:#aaa;cursor:pointer;border-radius:3px;padding:0" title="Close (Esc)">✕</button>
      </div>
    </div>

    <!-- Canvas Body -->
    <div id="cm_body" style="flex:1;padding:8px 10px;position:relative;display:flex;align-items:center;justify-content:center;overflow:hidden;background:#050505">
      <canvas id="cm_canvas" style="display:block;width:100%;height:100%"></canvas>
    </div>

    <!-- Footer with Legend -->
    <div style="padding:10px 18px;border-top:1px solid #222;background:#0d0d0d;font-size:12px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px">
      <div class="foot" style="margin:0">
        <span class="c" data-tip="VWAP — Volume Weighted Average Price. Key intraday support/resistance level. Price above VWAP = bullish bias; below = bearish.">━ VWAP</span> &nbsp;
        <span class="y" data-tip="Pre-market High — the highest price reached before the US market open (9:30 ET / 19:00 IST). Often acts as the first major resistance target in gap-up fades.">┅ PM high</span> &nbsp;
        <span class="d" data-tip="Previous Close — yesterday's closing price. The base from which the gap % is measured. Price returning here = full gap fill.">┅ prev close</span> &nbsp;
        <span style="color:#d7ff6e" data-tip="EMA 9 — 9-period Exponential Moving Average on 2-min closes. Fast-moving trend line. Breaks above/below signal short-term momentum shifts.">━ EMA9</span> &nbsp;
        <span style="color:#ffa500" data-tip="EMA 20 — 20-period Exponential Moving Average on 2-min closes. Slower trend line. When EMA9 crosses below EMA20, bearish momentum is strengthening.">━ EMA20</span> &nbsp;
        <span class="g" data-tip="Bone Zone cloud — shaded area between EMA9 and EMA20. Green = EMA9 above EMA20 (bullish). Red = EMA9 below EMA20 (bearish). Width shows momentum strength.">▒</span><span class="r">▒</span> cloud &nbsp;
        <span class="d" data-tip="Bollinger Bands (20-period, 2 std deviations). Upper/lower bands show the expected price range. Price touching/breaking the lower band can signal exhaustion of a move.">━ BB(20,2)</span> &nbsp;
        <span data-tip="SELL signal — short entry triggered by an entry model (e.g. short at open, PM high rejection, VWAP loss). Triangle points down at the entry price.">▼ SELL (entry)</span> &nbsp;
        <span data-tip="BUY signal — cover the short position. Either hit the stop loss or target. Percentage shown is the P&amp;L of that trade.">▲ BUY (cover)</span>. <span data-tip="Shaded (lighter) background area = pre-market session (before 9:30 ET / 19:00 IST). Price action here happens before regular market hours.">Shaded = premarket</span>. Times in IST.
      </div>
      <div style="color:#666;font-size:11px">Drag chart to pan · drag or scroll the price axis to scale · <b>Esc</b> to close</div>
    </div>
  </div>
</div>
<script>
const $=s=>document.querySelector(s);
// ---- Tooltip system ----
(function(){
  const box = document.getElementById('ag_tip');
  let tid = null;
  document.addEventListener('mouseover', e => {
    const el = e.target.closest('[data-tip]');
    if(!el){ clearTimeout(tid); box.classList.remove('visible'); return; }
    clearTimeout(tid);
    tid = setTimeout(() => {
      box.textContent = el.getAttribute('data-tip');
      box.classList.add('visible');
    }, 800);
  });
  document.addEventListener('mousemove', e => {
    let x = e.clientX + 14, y = e.clientY + 14;
    if(x + 310 > window.innerWidth) x = e.clientX - 14 - Math.min(300, box.offsetWidth);
    if(y + box.offsetHeight + 10 > window.innerHeight) y = e.clientY - box.offsetHeight - 10;
    box.style.left = x + 'px';
    box.style.top = y + 'px';
  });
  document.addEventListener('mouseout', e => {
    const el = e.target.closest('[data-tip]');
    if(el){ clearTimeout(tid); box.classList.remove('visible'); }
  });
  document.addEventListener('click', () => { clearTimeout(tid); box.classList.remove('visible'); });
})();
const PWD_USERS = ['karthik', 'locke'];   // can rename; session-exempt
let currentUser = localStorage.getItem('gap_user') || '';
let sessionToken = localStorage.getItem('gap_token') || '';
let _canRename = false;

/* ---- Gate helpers ---- */
function gateErr(msg){ $('#gate_err').textContent=msg; $('#gate_err').style.display= msg?'block':'none'; }
function gateSpinner(on){ $('#gate_spinner').style.display=on?'block':'none'; }
function gateShowStep(step){
  ['name','create','login'].forEach(s=>{
    const el = $('#gate_step_'+s);
    if(el) el.style.display = (s===step)?'block':'none';
  });
  gateErr('');
  if(step==='name') setTimeout(()=>{ const el=$('#gate_name'); if(el) el.focus(); },50);
  if(step==='create') setTimeout(()=>{ const el=$('#gate_new_pwd'); if(el) el.focus(); },50);
  if(step==='login') setTimeout(()=>{ const el=$('#gate_pwd'); if(el) el.focus(); },50);
}
function gateBack(){ gateShowStep('name'); }

async function submitGate(){
  const val = ($('#gate_name').value||'').trim().toLowerCase();
  if(!val){ gateErr('Enter your name.'); return; }
  gateErr('');
  gateSpinner(true);
  try{
    const res = await fetch('/api/auth/status?user='+encodeURIComponent(val));
    const data = await res.json();
    gateSpinner(false);
    if(!data.exists || data.error){
      const errStr = (data.error || 'Username not authorized.').replace(/^access denied:?\s*/i, '');
      gateErr('Access denied: ' + errStr);
      return;
    }
    // Store pending username
    currentUser = val;
    const sub = $('#gate_subtitle');
    if(sub) sub.textContent = 'Welcome, '+val.toUpperCase()+'!';
    if(!data.has_password){
      gateShowStep('create');
    } else {
      // Check if another session is active (only relevant for non-exempt users)
      if(data.has_active_session && !data.can_rename){
        $('#gate_force_box').style.display='block';
        $('#gate_force_chk').checked=false;
      } else {
        $('#gate_force_box').style.display='none';
      }
      gateShowStep('login');
    }
  } catch(e){
    gateSpinner(false);
    gateErr('Network error. Try again.');
  }
}

async function submitCreate(){
  const pwd  = ($('#gate_new_pwd').value||'').trim();
  const pwd2 = ($('#gate_new_pwd2').value||'').trim();
  if(!pwd){ gateErr('Please enter a password.'); return; }
  if(pwd !== pwd2){ gateErr('Passwords do not match.'); return; }
  gateErr('');
  gateSpinner(true);
  try{
    const res = await fetch('/api/auth/set-password',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({user: currentUser, password: pwd})
    });
    const data = await res.json();
    gateSpinner(false);
    if(!data.ok){ gateErr(data.error || 'Failed to create password.'); return; }
    sessionToken = data.session_token || '';
    _canRename = !!data.can_rename;
    _finishLogin(data);
  } catch(e){
    gateSpinner(false);
    gateErr('Network error. Try again.');
  }
}

async function submitLogin(){
  const pwd = ($('#gate_pwd').value||'').trim();
  if(!pwd){ gateErr('Enter your password.'); return; }
  const forceLogout = !!(document.getElementById('gate_force_chk') && $('#gate_force_chk').checked);
  gateErr('');
  gateSpinner(true);
  try{
    const res = await fetch('/api/auth/login',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({user: currentUser, password: pwd, force_logout: forceLogout})
    });
    const data = await res.json();
    gateSpinner(false);
    if(res.status === 409 && data.require_force){
      // Another session active, show the force-logout checkbox
      $('#gate_force_box').style.display='block';
      gateErr('Account is already logged in elsewhere. Check the box above to continue here.');
      return;
    }
    if(!data.ok){ gateErr(data.error || 'Login failed.'); return; }
    sessionToken = data.session_token || '';
    _canRename = !!data.can_rename;
    _finishLogin(data);
  } catch(e){
    gateSpinner(false);
    gateErr('Network error. Try again.');
  }
}

function _finishLogin(data){
  // Persist
  localStorage.setItem('gap_user', currentUser);
  if(sessionToken) localStorage.setItem('gap_token', sessionToken);
  else localStorage.removeItem('gap_token');
  checkAuth();
}

function checkAuth(){
  if(currentUser && sessionToken){
    _showApp();
    return true;
  }
  // Try auto-login with stored token
  if(currentUser && !sessionToken){
    // No token – need to re-login
  }
  _showGate();
  return false;
}

function _showApp(){
  const cur = currentUser.toLowerCase();
  $('#gate').style.display='none';
  $('#admin_view').style.display='none';
  $('#app_main').style.display='block';
  $('#u_name').textContent=currentUser.toUpperCase();
  // Show admin affordance
  if(_canRename || PWD_USERS.includes(cur)){
    $('#u_name').style.cursor='pointer';
    $('#u_name').style.textDecoration='underline dotted';
    $('#u_name').title = 'Click to open Admin Panel';
  } else {
    $('#u_name').style.cursor='default';
    $('#u_name').style.textDecoration='none';
    $('#u_name').title = '';
  }
  loadAppUiVisibility();
  if(appUiVisibilityTimer) clearInterval(appUiVisibilityTimer);
  appUiVisibilityTimer = setInterval(loadAppUiVisibility, 30000);
  $('#t').focus();
  loadScannerSettings().then(()=>startScannerPolling());
  // Periodically verify session (every 90s) for non-exempt users
  if(!_canRename && !PWD_USERS.includes(cur)){
    _startSessionPoll();
  }
}

function _showGate(){
  $('#gate').style.display='flex';
  $('#app_main').style.display='none';
  $('#admin_view').style.display='none';
  gateShowStep('name');
}

let _sessionPollTimer = null;
function _startSessionPoll(){
  if(_sessionPollTimer) clearInterval(_sessionPollTimer);
  _sessionPollTimer = setInterval(async ()=>{
    if(!currentUser || !sessionToken) return;
    try{
      const res = await fetch('/api/auth/verify-session?user='+encodeURIComponent(currentUser)+'&token='+encodeURIComponent(sessionToken));
      if(res.status === 401){
        clearInterval(_sessionPollTimer);
        alert('Your session was terminated because this account logged in from another browser.');
        doLogout();
      }
    } catch(e){}
  }, 90000);
}

function doLogout(){
  if(_sessionPollTimer){ clearInterval(_sessionPollTimer); _sessionPollTimer=null; }
  if(appUiVisibilityTimer){ clearInterval(appUiVisibilityTimer); appUiVisibilityTimer=null; }
  localStorage.removeItem('gap_user');
  localStorage.removeItem('gap_token');
  currentUser='';
  sessionToken='';
  _canRename=false;
  adminPwd='';
  if($('#gate_name')) $('#gate_name').value='';
  if($('#gate_pwd')) $('#gate_pwd').value='';
  if($('#gate_new_pwd')) $('#gate_new_pwd').value='';
  if($('#gate_new_pwd2')) $('#gate_new_pwd2').value='';
  if($('#u_badge')) $('#u_badge').innerHTML='';
  const sub = $('#gate_subtitle');
  if(sub) sub.textContent='Authorized access only. Enter your name:';
  _showGate();
}

/* Rename modal ---- */
function openRenameModal(){
  const cur = (currentUser||'').toLowerCase();
  if(!PWD_USERS.includes(cur) && !_canRename) return;
  $('#rename_curr_user').textContent = currentUser.toUpperCase();
  $('#rename_input').value = currentUser;
  $('#rename_err').textContent = '';
  $('#rename_modal').style.display = 'flex';
  setTimeout(()=>{ const inp=$('#rename_input'); if(inp){ inp.focus(); inp.select(); } }, 50);
}

function closeRenameModal(){
  $('#rename_modal').style.display = 'none';
  $('#rename_err').textContent = '';
}

async function submitRename(){
  const cur = (currentUser||'').toLowerCase();
  if(!PWD_USERS.includes(cur) && !_canRename){
    $('#rename_err').textContent = 'Only karthik and locke can rename their username.';
    return;
  }
  const nm = ($('#rename_input').value||'').trim().toLowerCase();
  if(!nm || nm === cur){
    $('#rename_err').textContent = 'Enter a different username.';
    return;
  }
  if(!/^[a-z0-9_]{2,20}$/.test(nm)){
    $('#rename_err').textContent = 'Username must be 2-20 chars (a-z, 0-9, _).';
    return;
  }
  $('#rename_err').textContent = '';
  const btn = $('#rename_submit_btn');
  btn.disabled = true;
  btn.textContent = 'Saving...';
  try {
    const res = await fetch('/api/auth/rename-user',{
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({old_name: currentUser, new_name: nm, token: sessionToken})
    });
    const data = await res.json();
    btn.disabled = false;
    btn.textContent = 'Save & Rename';
    if(data.error){
      $('#rename_err').textContent = data.error;
      return;
    }
    const newName = data.new_name || nm;
    currentUser = newName;
    if(!PWD_USERS.includes(newName)) PWD_USERS.push(newName);
    _canRename = true;
    localStorage.setItem('gap_user', newName);
    $('#u_name').textContent = newName.toUpperCase();
    closeRenameModal();
    alert('Username successfully changed to: ' + newName.toUpperCase());
    if($('#users_modal').style.display === 'flex') loadUsersModal();
    if($('#admin_view').style.display === 'block') loadAdminSearches();
  } catch(e){
    btn.disabled = false;
    btn.textContent = 'Save & Rename';
    $('#rename_err').textContent = 'Network error: ' + e;
  }
}

/* ---- Auto re-auth on load ---- */
async function initAuth(){
  const savedUser = localStorage.getItem('gap_user') || '';
  const savedToken = localStorage.getItem('gap_token') || '';
  if(!savedUser || !savedToken){ _showGate(); return; }
  // Verify saved session
  try{
    const res = await fetch('/api/auth/verify-session?user='+encodeURIComponent(savedUser)+'&token='+encodeURIComponent(savedToken));
    const data = await res.json();
    if(res.ok && data.ok){
      currentUser = savedUser;
      sessionToken = savedToken;
      _canRename = !!data.can_rename;
      _showApp();
    } else {
      localStorage.removeItem('gap_token');
      currentUser = savedUser;
      sessionToken = '';
      _showGate();
      // Pre-fill name in gate
      if($('#gate_name')) $('#gate_name').value = savedUser;
    }
  } catch(e){
    // Network error – allow offline? show gate
    _showGate();
    if($('#gate_name')) $('#gate_name').value = savedUser;
  }
}

let lastText="", timer=null, lastChart=null, EV=[];
const esc=s=>String(s??"").replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
const kv=rows=>'<div class="kv">'+rows.map(([l,v,c])=>`<span>${esc(l)}:</span><span class="${c}">${esc(v)}</span>`).join('')+'</div>';
const table=(cols,rows,dim,sortable)=>`<div class="scroll"><table class="${sortable?'sort':''}"><thead><tr>`+cols.map(c=>`<th>${esc(c)}</th>`).join('')+'</tr></thead><tbody>'+
  rows.map((r,i)=>`<tr class="${dim&&dim[i]?'dim':''}">`+r.map(([x,c,s])=>`<td class="${c}" data-s="${esc(s)}">${esc(x)}</td>`).join('')+'</tr>').join('')+'</tbody></table></div>';

function makeSortable(tb){
  const body=tb.tBodies[0];
  tb.querySelectorAll('th').forEach((th,ci)=>{
    th.onclick=()=>{
      const dir=th.dataset.dir==='1'?-1:1; tb.querySelectorAll('th').forEach(h=>delete h.dataset.dir); th.dataset.dir=dir===1?'1':'-1';
      const rows=[...body.rows], val=r=>{const v=r.cells[ci].dataset.s; const n=parseFloat(v); return (v!==''&&!isNaN(n)&&/^-?[0-9.e+-]+$/i.test(v))?n:v;};
      rows.sort((a,b)=>{const x=val(a),y=val(b);
        if(x===''&&y==='')return 0; if(x==='')return 1; if(y==='')return -1;
        return (typeof x==='number'&&typeof y==='number'?x-y:String(x).localeCompare(String(y)))*dir;});
      rows.forEach(r=>body.appendChild(r));
    };
  });
}

let openTabs = {}; // ticker -> { ticker, result, lastText, lastChart, EV, container, timer, isRunning }
let activeTab = null;

function renderTabBar(){
  const bar = $('#tab_bar');
  const tickers = Object.keys(openTabs);
  if(!tickers.length){
    bar.style.display = 'none';
    $('#out').style.display = 'block';
    return;
  }
  bar.style.display = 'flex';
  $('#out').style.display = 'none';

  bar.innerHTML = tickers.map(t => {
    const isActive = t === activeTab;
    const tabObj = openTabs[t];
    const isRunning = tabObj && tabObj.isRunning;
    const bg = isActive ? '#141414' : '#080808';
    const borderTop = isActive ? '2px solid #29a8ff' : '2px solid transparent';
    const color = isActive ? '#fff' : '#888';
    const border = isActive ? '1px solid #333' : '1px solid #1f1f1f';

    return `<div class="ticker_tab" onclick="switchTab('${esc(t)}')" style="background:${bg};border:${border};border-top:${borderTop};color:${color};padding:8px 14px;border-radius:4px 4px 0 0;font-size:13px;cursor:pointer;display:flex;align-items:center;gap:8px;user-select:none;transition:background 0.15s">`+
      `<span style="font-weight:bold">${esc(t)}</span>`+
      (isRunning ? '<span style="color:#ffd23f;font-size:11px" title="Analyzing...">●</span>' : '')+
      `<span onclick="event.stopPropagation(); closeTab('${esc(t)}')" style="font-size:14px;color:#666;line-height:1;padding:0 2px" onmouseover="this.style.color='#ff5a5a'" onmouseout="this.style.color='#666'">✕</span>`+
    `</div>`;
  }).join('');
}

function switchTab(t){
  if(!openTabs[t]) return;
  activeTab = t;
  $('#t').value = t;

  Object.keys(openTabs).forEach(k => {
    const el = openTabs[k].container;
    if(el) el.style.display = (k === t) ? 'block' : 'none';
  });

  renderTabBar();

  const tab = openTabs[t];
  if(tab){
    if(tab.lastChart){
      const cv = tab.container.querySelector('#cv');
      if(cv) drawChart(tab.lastChart, cv);
    }
    if(tab.EV && tab.EV.length){
      drawEvAll(tab.EV, tab.container);
    }
  }
}

function closeTab(t){
  if(!openTabs[t]) return;
  if(openTabs[t].timer) clearInterval(openTabs[t].timer);
  if(openTabs[t].container) openTabs[t].container.remove();
  delete openTabs[t];

  if(activeTab === t){
    const remaining = Object.keys(openTabs);
    activeTab = remaining.length ? remaining[remaining.length - 1] : null;
    if(activeTab){
      switchTab(activeTab);
    } else {
      $('#t').value = '';
      $('#out').style.display = 'block';
      renderTabBar();
    }
  } else {
    renderTabBar();
  }
}

async function run(refresh){
  if(!checkAuth()) return;
  let t = ($('#t').value || '').trim().toUpperCase();
  if(!t && activeTab) t = activeTab;
  if(!t) return;

  if(!openTabs[t]){
    const cont = document.createElement('div');
    cont.id = 'tab_content_' + t;
    cont.style.padding = '24px 34px';
    $('#tabs_view').appendChild(cont);

    openTabs[t] = {
      ticker: t,
      container: cont,
      isRunning: true,
      lastText: '',
      lastChart: null,
      pathNormalization: 'prev_close',
      EV: [],
      timer: null
    };
  }

  const tab = openTabs[t];
  tab.isRunning = true;
  switchTab(t);

  const statusMsg = refresh 
    ? "Refreshing today's live stats (Volume, Float Rotation, Price, Gap)..." 
    : 'Starting analysis for ' + t + '…';

  if(!tab.result){
    tab.container.innerHTML = `<span class="d">${statusMsg}</span>`;
  } else {
    const banner = tab.container.querySelector('.refresh_banner');
    if(banner){
      banner.style.display = 'block';
      banner.textContent = statusMsg;
    }
  }

  if(tab.timer) clearInterval(tab.timer);

  try {
    const r = await fetch('/api/start?ticker=' + encodeURIComponent(t) + '&user=' + encodeURIComponent(currentUser) + '&token=' + encodeURIComponent(sessionToken) + (refresh ? '&refresh=1' : ''));
    const j = await r.json();
    if(j.error){
      tab.isRunning = false;
      renderTabBar();
      tab.container.innerHTML = `<span class="r">${esc(j.error)}</span>`;
      return;
    }

    const poll = async () => {
      try {
        const s = await (await fetch('/api/job/' + j.job)).json();
        if(s.status === 'running'){
          if(!tab.result){
            tab.container.innerHTML = `<span class="d">${esc(s.msg)}</span>`;
          }
          return;
        }
        clearInterval(tab.timer);
        tab.isRunning = false;
        renderTabBar();
        if(s.status === 'error'){
          tab.container.innerHTML = `<span class="r">${esc(s.error)}</span>`;
          return;
        }
        renderTab(tab, s.result, refresh);
      } catch(e){
        clearInterval(tab.timer);
        tab.isRunning = false;
        renderTabBar();
      }
    };

    tab.timer = setInterval(poll, 1500);
    poll();
  } catch(e) {
    tab.isRunning = false;
    renderTabBar();
    tab.container.innerHTML = `<span class="r">Network error: ${esc(e)}</span>`;
  }
}

function renderTab(tab, r, isRefresh){
  tab.lastText = r.text;
  tab.lastChart = r.chart;
  tab.pathNormalization = tab.pathNormalization || 'prev_close';
  tab.lastChart.normalization = tab.pathNormalization;
  tab.EV = r.evcharts || [];
  tab.result = r;
  tab.sessionDates = r.session_dates || [];
  tab.sessionChartCache = tab.sessionChartCache || {};
  tab.EV.forEach(item=>{if(item.date) tab.sessionChartCache[item.date]=item;});

  const h = r.head, T = r.titles;
  let o = '';

  const nowIstStr = new Date().toLocaleTimeString('en-US', {timeZone: 'Asia/Kolkata', hour12: false});
  const refreshBadge = isRefresh
    ? `<span style="background:#161616;border:1px solid #333;color:#3ddc84;font-size:11px;padding:3px 9px;border-radius:3px;margin-left:12px;font-weight:normal">● LIVE DAY STATS UPDATED: ${nowIstStr} IST</span>`
    : '';

  o += `<div class="refresh_banner" style="display:none;background:#141414;border:1px solid #29a8ff;padding:8px 12px;margin-bottom:14px;color:#29a8ff;font-size:13px"></div>`;
  o += `<div style="display:flex;align-items:center;flex-wrap:wrap;gap:14px"><span class="big">Ticker: <span class="c">${esc(h.ticker)}</span></span><a href="${esc(r.dilution_url)}" target="_blank" rel="noopener noreferrer" style="color:#d8b4fe;font-size:13px;font-weight:normal">Finviz Dilution ↗</a>${refreshBadge}</div><div class="w">${esc(h.sub)}</div><div>==========================================</div><br>`;
  o += `<div>Final Fade Grade:<span class="badge ${h.gcls}">${esc(h.grade)}</span>(${esc(h.score)})</div>`;
  o += `<div>Similar Setups: <span class="c">${h.n}</span> (${h.total ? 'of ' + h.total + ' gap-ups' : ''})</div>`;
  o += `<div>Verdict: <span class="${h.gcls}">${esc(h.verdict)}</span></div>`;
  o += `<div>Summary: ${esc(h.summary)}</div>`;
  (h.notes||[]).forEach(n=>o+=`<div class="y">Note: ${esc(n)}</div>`);
  const newsItems = r.news || [];
  o += `<div class="panel"><div class="ttl">Latest Press Releases · Yahoo Finance</div>`+
    (newsItems.length ? newsItems.map(item=>{
      const link = /^https?:\/\//i.test(item.url || '')
        ? `<a href="${esc(item.url)}" target="_blank" rel="noopener noreferrer" style="color:#6cc4ff">${esc(item.title)}</a>`
        : esc(item.title);
      let publishedDate='';
      if(item.published){
        const raw=String(item.published), parsed=new Date(raw);
        const iso=/^(\d{4})-(\d{2})-(\d{2})/.exec(raw);
        publishedDate=iso?`${iso[2]}/${iso[3]}/${iso[1]}`:(!Number.isNaN(parsed.getTime())?`${String(parsed.getUTCMonth()+1).padStart(2,'0')}/${String(parsed.getUTCDate()).padStart(2,'0')}/${parsed.getUTCFullYear()}`:'');
      }
      const dateTag=publishedDate?`<span style="display:inline-block;margin-right:8px;padding:1px 6px;border:1px solid #294458;border-radius:3px;background:#0b151c;color:#9fc5dc;font-size:11px;white-space:nowrap">${esc(publishedDate)}</span>`:'';
      return `<div style="padding:7px 0;border-bottom:1px solid #222">${dateTag}${link}<span style="color:#777;font-size:12px"> — ${esc(item.publisher || 'Yahoo Finance')}</span></div>`;
    }).join('') : `<div class="foot">${esc(r.news_status || 'No press releases found on Yahoo Finance.')}</div>`) + `</div>`;
  o += `<div class="panel two"><div><div class="ttl">${esc(T.p1)}</div>${kv(r.p1.left)}</div><div>${kv(r.p1.right)}</div></div>`;
  if(r.open_hod_stats){
    const s=r.open_hod_stats, fmt=x=>x==null?'—':`${x>=0?'+':''}${(x*100).toFixed(0)}%`, line=x=>`${fmt(x.median)}   avg ${fmt(x.avg)}   (${x.n} ${x.n===1?'day':'days'})`;
    o += `<div class="panel"><div class="ttl">Open → HOD (daily bars; 12 mo for gap-up / volume rows)</div>`+
      `<div class="kv"><span>Last 30 sessions</span><span>${line(s.last30)}</span><span>Last 90 sessions</span><span>${line(s.last90)}</span>`+
      `<span>Gap-up days ≥5%</span><span>${line(s.gapup5)}</span><span data-tip="High-volume days have daily volume at least 10× the average of the prior 20 sessions.">High-volume days</span><span>${line(s.highvol)}</span></div>`+
      `<div class="foot">${esc(s.date_from||'')} to ${esc(s.date_to||'')} · high volume = ≥${s.volume_multiple}× prior 20-session average</div></div>`;
  }
  if(r.hod_times){
    const h=r.hod_times;
    o += `<div class="panel"><div class="ttl">HOD time (from chart, 15m bars, ${esc(h.date_from||'')} to ${esc(h.date_to||'')})</div>`+
      `<div class="kv"><span>All sessions</span><span>median ${esc(h.median_time||'—')} IST (${(h.rows||[]).length} days)</span>`+
      `${(h.rows||[]).length?`<span>Daily HOD times</span><span>${h.rows.map(x=>`${esc(x.date)} ${esc(x.time)}`).join(' · ')}</span>`:''}</div>`+
      `${(h.rows||[]).length?'<div class="foot">Approximate HOD time is the timestamp of the 15-minute bar containing the session high.</div>':'<div class="foot">15-minute HOD bars are unavailable for this range.</div>'}</div>`;
  }
  (r.tables||[]).forEach(t=>{
    o += `<div class="panel"><div class="ttl">${esc(t.title)}</div>${table(t.cols,t.rows,t.dim,true)}<div class="foot">${esc(t.foot||'')}</div></div>`;
  });
  o += `<div class="panel"><div class="ttl" style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px">`+
     `<div style="display:flex;align-items:center;flex-wrap:wrap;gap:7px">${esc(T.p6)}<label style="font-size:11px;color:#aaa">All charts <select id="ev_all_tf" class="evtf"><option>1m</option><option selected>2m</option><option>5m</option><option>15m</option><option>30m</option><option value="1d">Day</option></select></label><select id="ev_all_days" class="evtf" title="Days for all charts"><option value="1" selected>1 day</option><option value="3">3 days</option><option value="5">5 days</option><option value="10">10 days</option><option value="20">20 days</option><option value="60">60 days</option></select><select id="sig"><option value="none" selected>No signals</option><option value="all">All entry models</option>`+
     (r.model_names||[]).map(n=>`<option value="${esc(n)}">${esc(n)}</option>`).join('')+`</select><label style="margin-left:14px;font-size:13px;color:#e8e8e8"><input type="checkbox" id="bz" style="width:auto;height:auto;vertical-align:middle"> Bone zone (9/20 EMA cloud + BB)</label></div>`+
     `<span style="font-size:12px;color:#29a8ff;font-weight:normal">🔍 Click any chart to enlarge</span></div>`+
  `<div class="evgrid">`+tab.EV.map((c,i)=>`<div class="evc" data-ev-slot="${i}" onclick="openChartModal(${i})"><div class="evhead"><div class="evt" id="evtitle${i}"></div><span class="eventDateTag" data-event-date></span><div class="evcontrols"><select class="evtf" title="Timeframe"><option>1m</option><option selected>2m</option><option>5m</option><option>15m</option><option>30m</option><option value="1d">Day</option></select><select class="evdays" title="Sessions shown"><option value="1" selected>1 day</option><option value="3">3 days</option><option value="5">5 days</option><option value="10">10 days</option><option value="20">20 days</option><option value="60">60 days</option></select><button class="evzoom" data-zoom="out" title="Zoom out">−</button><button class="evzoom" data-zoom="in" title="Zoom in">+</button><button class="evmeasure" type="button" title="Drag across bars to measure volume, dollar volume, and float rotation">Measure</button><button class="evtoday" type="button" title="Return to today's live chart">Today</button></div></div><canvas id="ev${i}"></canvas></div>`).join('')+
     `<\/div><div class="foot"><span class="c" data-tip="VWAP — Volume Weighted Average Price. Key intraday support/resistance.">━ VWAP<\/span>  <span class="y" data-tip="Pre-market High — highest price before the US market open. Common fade target.">┅ PM high<\/span>  <span class="d" data-tip="Previous Close — yesterday’s close. Gap % is measured from here.">┅ prev close<\/span> &nbsp; <span style="color:#d7ff6e" data-tip="EMA 9 — 9-period EMA on 2-min closes. Fast trend line.">━ EMA9<\/span> <span style="color:#ffa500" data-tip="EMA 20 — 20-period EMA on 2-min closes. Slower trend line. Bearish when EMA9 drops below it.">━ EMA20<\/span> <span class="g" data-tip="Bone Zone cloud — shaded between EMA9 &amp; EMA20. Green = bullish. Red = bearish.">▒<\/span><span class="r">▒<\/span> cloud <span class="d" data-tip="Bollinger Bands (20-period, 2 std dev). Shows expected volatility range on the 2-min chart.">━ BB(20,2)<\/span> &nbsp; <span data-tip="SELL signal — short entry triggered by an entry model.">▼ SELL<\/span> &nbsp; <span data-tip="BUY / Cover signal — close the short here. P&amp;L % shown next to it.">▲ BUY<\/span>. <span data-tip="Lighter shaded background = pre-market session (before 9:30 ET / 19:00 IST).">Shaded = premarket<\/span>. X axis = IST. Measure: activate Measure, then drag across bars for volume, estimated dollar volume, and float rotation.<\/div><\/div>`;

  const pmGapText = Number.isFinite(r.chart.premarket_gap_pct)
    ? `${(r.chart.premarket_gap_pct*100).toFixed(1)}% (PM high vs prev close)` : 'Not available';
  o += `<div class="panel"><div class="ttl" style="display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap"><span>${esc(T.p7)}</span><span style="display:flex;align-items:center;gap:12px;font-size:12px;font-weight:normal;color:#aaa"><span>Premarket gap: <b style="color:#e8e8e8">${esc(pmGapText)}</b></span><label for="path_norm">View <select id="path_norm" style="margin-left:5px"><option value="prev_close">% from previous close</option><option value="gap">% of premarket gap retained</option></select></label></span></div><canvas id="cv"></canvas>`+
     `<div class="foot" id="path_norm_foot"><span class="b" data-tip="Historical median across similar gap-up setups.">━ history median<\/span> &nbsp; <span style="color:#6c8" data-tip="Middle 50% range of similar setups.">▒ p25–p75<\/span> &nbsp; <span class="y">━ today<\/span> &nbsp; <span class="y">┅ today projected<\/span> &nbsp; <span class="d">━ each similar setup<\/span> &nbsp; Y = % from previous close<\/div><\/div>`;

  o += `<div class="three"><div class="panel"><div class="ttl">${esc(T.pa)}</div>${kv(r.pa)}</div>`+
     `<div class="panel"><div class="ttl">${esc(T.pb)}</div>${kv(r.pb)}</div>`+
     `<div class="panel"><div class="ttl">${esc(T.pc)}</div>${kv(r.pc)}</div></div>`;
  if(r.pd.rows.length) o += `<div class="panel"><div class="ttl">${esc(T.pd)}</div>${table(r.pd.cols,r.pd.rows,null,false)}</div>`;

  tab.container.innerHTML = o;
  tab.container.querySelectorAll('table.sort').forEach(makeSortable);

  const sigSelect = tab.container.querySelector('#sig');
  const bzCheck = tab.container.querySelector('#bz');
  const allTf=tab.container.querySelector('#ev_all_tf'),allDays=tab.container.querySelector('#ev_all_days');
  const applyAllChartSettings=()=>{tab.EV.forEach((_,i)=>{const card=tab.container.querySelector(`[data-ev-slot="${i}"]`);if(!card)return;card._chartTf=allTf.value;card._chartDays=Number(allDays.value)||1;card._panOffset=0;card._yZoom=1;const tf=card.querySelector('.evtf'),days=card.querySelector('.evdays');if(tf)tf.value=card._chartTf;if(days)days.value=String(card._chartDays);});drawEvAll(tab.EV,tab.container);};
  if(allTf)allTf.onchange=()=>{allDays.value=allTf.value==='1d'?'60':'1';applyAllChartSettings();};
  if(allDays)allDays.onchange=applyAllChartSettings;
  if(sigSelect) sigSelect.onchange = () => drawEvAll(tab.EV, tab.container);
  if(bzCheck) bzCheck.onchange = () => drawEvAll(tab.EV, tab.container);
  const pathNormSelect = tab.container.querySelector('#path_norm');
  if(pathNormSelect){
    pathNormSelect.value = tab.pathNormalization;
    pathNormSelect.onchange = ()=>{
      tab.pathNormalization = pathNormSelect.value;
      tab.lastChart.normalization = tab.pathNormalization;
      const foot = tab.container.querySelector('#path_norm_foot');
      if(foot) foot.lastChild.textContent = tab.pathNormalization === 'gap'
        ? ' X = minutes from each day’s PM high · Y = % of that day’s gap retained · 100% = PM high · 0% = previous close'
        : ' Y = % from previous close';
      const canvas = tab.container.querySelector('#cv');
      if(canvas) drawChart(tab.lastChart, canvas);
    };
  }

  drawEvAll(tab.EV, tab.container);
  const cv = tab.container.querySelector('#cv');
  if(cv) drawChart(r.chart, cv);
}

const MC={"Short at open":"#ff9f43","PM high rejection":"#e056fd","5m low break":"#ff5a5a","First red 5m candle":"#ffd23f","VWAP loss (post-HOD)":"#6cc4ff"};
const MA={"Short at open":"Open","PM high rejection":"PMH","5m low break":"5mL","First red 5m candle":"R5","VWAP loss (post-HOD)":"VWAP"};
const hm=(m,off)=>{m=((m+off)%1440+1440)%1440;return String(Math.floor(m/60)).padStart(2,'0')+':'+String(m%60).padStart(2,'0')};
function tri(x,px,py,dir,col){x.fillStyle=col;x.beginPath();
  if(dir==='down'){x.moveTo(px-5,py-9);x.lineTo(px+5,py-9);x.lineTo(px,py-1);}
  else{x.moveTo(px-5,py+9);x.lineTo(px+5,py+9);x.lineTo(px,py+1);}
  x.closePath();x.fill();}

function bone(b){
  const n=b.c.length,a9=2/10,a20=2/21,e9=[],e20=[],bu=[],bl=[];
  for(let i=0;i<n;i++){
    e9.push(i?a9*b.c[i]+(1-a9)*e9[i-1]:b.c[i]); e20.push(i?a20*b.c[i]+(1-a20)*e20[i-1]:b.c[i]);
    if(i>=19){let s=0;for(let k=i-19;k<=i;k++)s+=b.c[k];const m=s/20;let v=0;for(let k=i-19;k<=i;k++)v+=(b.c[k]-m)**2;const sd=Math.sqrt(v/20);bu.push(m+2*sd);bl.push(m-2*sd);}
    else{bu.push(null);bl.push(null);}
  }
  return {e9,e20,bu,bl};
}
function drawEvAll(evList, containerEl){
  const cEl = containerEl || (activeTab && openTabs[activeTab] ? openTabs[activeTab].container : document);
  const list = evList || (activeTab && openTabs[activeTab] ? openTabs[activeTab].EV : []);
  const selEl = cEl.querySelector('#sig');
  const sel = selEl ? selEl.value : 'none';
  const bzEl = cEl.querySelector('#bz');
  const bzChecked = bzEl ? bzEl.checked : false;
  const tabForCharts=Object.values(openTabs).find(tab=>tab.container===cEl);
  if(!window.evLazyObserver)window.evLazyObserver=new IntersectionObserver(entries=>entries.forEach(entry=>{if(entry.isIntersecting){entry.target._chartVisible=true;const t=Object.values(openTabs).find(x=>x.container.contains(entry.target));if(t)drawEvAll(t.EV,t.container);window.evLazyObserver.unobserve(entry.target);}}),{rootMargin:'450px 0px'});
  list.forEach((c,i)=>{
    const card=cEl.querySelector(`[data-ev-slot="${i}"]`), cv=cEl.querySelector('#ev'+i);
    if(!card || !cv) return;
    if(!card._chartItem) card._chartItem=c;
    if(!card._sessionDate) card._sessionDate=card._chartItem.date||c.date;
    if(!card._chartTf) card._chartTf='2m';
    if(!card._chartDays) card._chartDays=1;
    const item=card._chartItem || c;
    if(!card._eventDate)card._eventDate=item.date||c.date;
    const dateList=tabForCharts?tabForCharts.sessionDates:[];
    card._sessionPos=dateList.indexOf(card._sessionDate);
    const title=card.querySelector(`#evtitle${i}`), tf=card.querySelector('.evtf');
    if(title) title.innerHTML=esc(item.title)+(item.sim?'':'<span class="d"> (not in similar set)</span>');
    const eventDateTag=card.querySelector('[data-event-date]');if(eventDateTag){const parts=String(card._eventDate||'').slice(0,10).split('-');eventDateTag.textContent=parts.length===3?`Event ${parts[1]}/${parts[2]}/${parts[0]}`:'Event date';}
    if(tf){tf.value=card._chartTf;tf.onclick=e=>e.stopPropagation();tf.onchange=e=>{e.stopPropagation();const previousTf=card._chartTf;card._chartTf=tf.value;card._panOffset=0;card._yZoom=1;if(tf.value==='1d')card._chartDays=60;else if(previousTf==='1d')card._chartDays=1;const daysSelect=card.querySelector('.evdays');if(daysSelect)daysSelect.value=String(card._chartDays);if(card._chartDays===1&&tf.value!=='1d')loadChartContext(card,tabForCharts);else if((tf.value==='1d')!==!!card._contextDaily||!card._contextChart)loadChartContext(card,tabForCharts);else drawEvAll(tabForCharts.EV,tabForCharts.container);};}
    const days=card.querySelector('.evdays');if(days){days.value=String(card._chartDays);days.onclick=e=>e.stopPropagation();days.onchange=e=>{e.stopPropagation();card._chartDays=Number(days.value);card._panOffset=0;loadChartContext(card,tabForCharts);};}
    card.querySelectorAll('.evzoom').forEach(btn=>btn.onclick=e=>{e.stopPropagation();card._zoom=Math.max(.5,Math.min(8,(card._zoom||1)*(btn.dataset.zoom==='in'?1.25:.8)));drawEvAll(list,cEl);});
    const measureBtn=card.querySelector('.evmeasure');if(measureBtn){measureBtn.onclick=e=>{e.stopPropagation();cv._measureMode=!cv._measureMode;measureBtn.classList.toggle('active',cv._measureMode);measureBtn.textContent=cv._measureMode?'Cancel measure':'Measure';cv.style.cursor=cv._measureMode?'crosshair':'grab';};}
    const todayBtn=card.querySelector('.evtoday');if(todayBtn){todayBtn.onclick=e=>{e.stopPropagation();const liveItem=(tabForCharts?.EV||[]).find(x=>String(x.title||'').startsWith('TODAY '));if(!liveItem)return;card._chartItem=liveItem;card._eventDate=liveItem.date;card._sessionDate=liveItem.date;card._contextChart=null;card._contextDates=[];card._panOffset=0;card._zoom=1;card._yZoom=1;cv._measureStartKey=cv._measureEndKey=null;drawEvAll(tabForCharts.EV,tabForCharts.container);if($('#chart_modal').style.display==='flex'&&currentModalSlot===i)renderModalChart();};}
    card._onPanEdge=side=>{
      if(card._contextLoading||!tabForCharts)return;
      const contextDates=card._contextDates?.length?card._contextDates:null;
      const dates=contextDates||tabForCharts.sessionDates||[];
      const eventPos=(tabForCharts.sessionDates||[]).indexOf(card._sessionDate||item.date);
      const edge=contextDates?dates[side>0?dates.length-1:0]:dates[eventPos+side];
      if(!edge)return;
      card._chartDays=[1,3,5,10,20,60].find(value=>value>card._chartDays)||60;
      const daysSelect=card.querySelector('.evdays');if(daysSelect)daysSelect.value=String(card._chartDays);
      card._panOffset=0;loadChartContext(card,tabForCharts,edge);
    };
    const renderItem=card._contextChart?{...item,chart:{...item.chart,bars:card._contextChart}}:item;
    cv._anchorDate=card._sessionDate;cv._panOffset=card._panOffset||0;cv._onPanChange=value=>card._panOffset=value;cv._onPanEdge=card._onPanEdge;
    if(!card._chartVisible&&$('#chart_modal').style.display!=='flex'){window.evLazyObserver.observe(card);return;}
    if((card._chartDays>1||card._chartTf==='1d')&&!card._contextChart&&!card._contextLoading&&!card._contextError)loadChartContext(card,tabForCharts);
    drawEv(cv,renderItem.chart,sel,bzChecked,undefined,card._chartTf,card._zoom||1);
  });
}
async function loadChartContext(card,tab,anchorDate){
  if(!card||!tab)return;
  const days=card._chartDays||1, tf=card._chartTf||'2m';
  if(days===1&&tf!=='1d'){card._contextChart=null;card._contextDates=[];card._contextDaily=false;card._contextError='';card._contextLoading=false;drawEvAll(tab.EV,tab.container);return;}
  card._contextLoading=true;card._contextError='';
  const requestId=(card._contextRequestId||0)+1;card._contextRequestId=requestId;
  const query=new URLSearchParams({ticker:tab.result?.head?.ticker||'',date:anchorDate||card._sessionDate,days:String(days),tf,user:currentUser||'',token:sessionToken||''});
  try{const response=await fetch('/api/chart-context?'+query.toString()),data=await response.json();if(requestId!==card._contextRequestId)return;if(!response.ok)throw new Error(data.error||'Could not load chart range');card._contextChart=data.bars;card._contextDates=data.dates;card._contextDaily=!!data.daily;if(anchorDate)card._sessionDate=anchorDate;}
  catch(error){card._contextChart=null;card._contextError=error.message;}
  card._contextLoading=false;
  drawEvAll(tab.EV,tab.container);
  if($('#chart_modal').style.display==='flex'&&currentModalSlot===Number(card.dataset.evSlot))renderModalChart();
}
async function navigateEvCard(card, step, tab){
  if(!tab || !card || card._sessionPos==null || card._sessionPos<0) return;
  const nextPos=card._sessionPos+step, dates=tab.sessionDates||[];
  if(nextPos<0||nextPos>=dates.length) return;
  const day=dates[nextPos];
  card._sessionPos=nextPos;card._sessionDate=day;
  const list=tab.EV||[];
  let item=tab.sessionChartCache[day] || list.find(x=>x.date===day);
  const requestId=(card._loadRequestId||0)+1;card._loadRequestId=requestId;
  if(!item){
    const title=card.querySelector('.evt');if(title) title.textContent=`${day} · loading…`;
    const query=new URLSearchParams({ticker:tab.result?.head?.ticker||'',date:day,user:currentUser||'',token:sessionToken||''});
    try{
      const response=await fetch('/api/session-chart?'+query.toString()), data=await response.json();
      if(requestId!==card._loadRequestId)return;
      if(!response.ok) throw new Error(data.error||'Could not load session chart');
      item={date:data.date,title:data.title,sim:false,chart:data.chart};
      tab.sessionChartCache[day]=item;
    }catch(error){
      if(requestId!==card._loadRequestId)return;
      item={date:day,title:`${day} · ${error.message}`,sim:false,chart:{bars:{t:[]}}};
    }
  }
  if(requestId!==card._loadRequestId)return;
  card._chartItem=item;
  card._contextChart=null;
  drawEvAll(tab.EV,tab.container);
  if((card._chartDays||1)>1||card._chartTf==='1d')loadChartContext(card,tab);
  if($('#chart_modal').style.display==='flex'&&currentModalSlot===Number(card.dataset.evSlot))renderModalChart();
}
function aggregateEvBars(source, tf){
  if(!source || !source.t || tf===1 || tf==='1d') return source;
  const out={t:[],d:[],o:[],h:[],l:[],c:[],v:[],vw:[]}, groups=new Map();
  for(let i=0;i<source.t.length;i++){
    const start=Math.floor(source.t[i]/tf)*tf,day=source.d?.[i]||'';
    const key=day+'|'+start;
    if(!groups.has(key)) groups.set(key,[]);
    groups.get(key).push(i);
  }
  for(const [key,ids] of groups){
    const start=Number(key.split('|').pop());out.t.push(start);out.d.push(source.d?.[ids[0]]||'');out.o.push(source.o[ids[0]]);out.h.push(Math.max(...ids.map(i=>source.h[i])));
    out.l.push(Math.min(...ids.map(i=>source.l[i])));out.c.push(source.c[ids[ids.length-1]]);
    out.v.push(ids.reduce((sum,i)=>sum+(source.v[i]||0),0));out.vw.push(source.vw[ids[ids.length-1]]);
  }
  return out;
}
function drawEv(cv, c, sel, bzChecked, targetH, timeframe='2m', zoom=1){
  const parentW = (cv.parentElement && cv.parentElement.clientWidth) ? cv.parentElement.clientWidth : 0;
  const dpr = window.devicePixelRatio || 1;
  const W = Math.max(300, parentW - (targetH ? 0 : 16));
  const H = targetH || 390;
  cv.style.width = W + 'px';
  cv.style.height = H + 'px';
  cv.width = W * dpr;
  cv.height = H * dpr;
  const x = cv.getContext('2d');
  x.setTransform(dpr, 0, 0, dpr, 0, 0);
  x.clearRect(0, 0, W, H);
  const isLarge = H > 450;
  x.font = (isLarge ? '13px' : '11px') + ' Consolas,monospace';
  const daily=timeframe==='1d', tf=parseInt(timeframe,10)||2, source=c.bars;
  let b,dates,multi;const cached=cv._seriesCache;
  if(cached?.source===source&&cached.tf===timeframe){b=cached.bars;dates=cached.dates;multi=cached.multi;}
  else{
    b=daily?source:aggregateEvBars(source,tf);
    dates=[...new Set(b?.d||[])];multi=!daily&&dates.length>1;
    if(multi){let last='',n=-1;b={...b,t:b.t.map((t,i)=>{if(b.d[i]!==last){last=b.d[i];n++;}return 240+n*720+t-240;})};}
    cv._seriesCache={source,tf:timeframe,bars:b,dates,multi};
  }
  if(!b || !b.t.length){
    x.fillStyle = '#8a8a8a';
    x.fillText('No intraday data', 10, 20);
    return;
  }
  const L = isLarge ? 64 : 50, R = 12, T = 10;
  const VH = Math.round(H * 0.18), B = isLarge ? 24 : 18;
  const ph = H - T - VH - B - 6, pw = W - L - R, vy0 = T + ph + 6, vy1 = vy0 + VH;
  const sg = sel === 'none' ? [] : (c.sig || []).filter(s => sel === 'all' || s.name === sel);
  const span=daily?Math.max(1,b.t.length):multi?dates.length*720:720, fullStart=daily?-.5:240;
  // Keep intraday zoom centered on the selected session.  Do not clamp zoom
  // to 1x: that made every zoom-out click a no-op.  The loaded date range is
  // still the upper limit, so zooming out reveals neighboring sessions.
  const baseViewSpan=daily?span:720;
  const viewSpan=Math.min(span,baseViewSpan/Math.max(.125,zoom)),panLimit=Math.max(0,span-viewSpan)/2;
  let viewBase=fullStart+panLimit,panMin=-panLimit,panMax=panLimit;
  if(multi){const anchorIndex=dates.indexOf(cv._anchorDate||c.date);if(anchorIndex>=0){viewBase=fullStart+anchorIndex*720+360-viewSpan/2;panMin=fullStart-viewBase;panMax=fullStart+span-viewSpan-viewBase;}}
  cv._panMinOffset=panMin;cv._panMaxOffset=panMax;
  cv._panOffset=Math.max(panMin,Math.min(panMax,cv._panOffset||0));
  const viewStart=viewBase+cv._panOffset,X=m=>L+pw*(m-viewStart)/viewSpan;
  let visible=Array.from({length:b.t.length},(_,i)=>i).filter(i=>b.t[i]>=viewStart-tf&&b.t[i]<=viewStart+viewSpan+tf);
  if(!visible.length){let nearest=0,distance=Infinity;for(let i=0;i<b.t.length;i++){const d=Math.abs(b.t[i]-(viewStart+viewSpan/2));if(d<distance){nearest=i;distance=d;}}visible=[nearest];}
  let lo=Math.min(...visible.map(i=>b.l[i])),hi=Math.max(...visible.map(i=>b.h[i]));
  const pad=(hi-lo)*0.05||hi*0.02;lo-=pad;hi+=pad;
  const yMid=(lo+hi)/2,yHalf=(hi-lo)/(2*Math.max(.5,cv._yZoom||1));lo=yMid-yHalf;hi=yMid+yHalf;
  const Y=v=>T+ph*(1-(v-lo)/(hi-lo));
  const hoverOffsets=new Map(),etClock=new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',timeZoneName:'shortOffset'}),istClock=new Intl.DateTimeFormat('en-US',{timeZone:'UTC',month:'2-digit',day:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit',hourCycle:'h23'});
  const hoverOffset=day=>{if(hoverOffsets.has(day))return hoverOffsets.get(day);let offset=Number(c.off)||0;try{const parts=etClock.formatToParts(new Date(day+'T12:00:00Z')),name=parts.find(p=>p.type==='timeZoneName')?.value||'',match=name.match(/GMT([+-])(\d{1,2})(?::?(\d{2}))?/);if(match){const et=(match[1]==='-'?-1:1)*(Number(match[2])*60+Number(match[3]||0));offset=330-et;}}catch{}hoverOffsets.set(day,offset);return offset;};
  const hoverStamp=(day,minute)=>{const [yy,mm,dd]=String(day||'').split('-').map(Number);if(!yy||!mm||!dd)return 'time unavailable';const at=new Date(Date.UTC(yy,mm-1,dd,0,Math.round(minute+hoverOffset(day)))),parts=istClock.formatToParts(at),v=Object.fromEntries(parts.map(p=>[p.type,p.value]));return `${v.month}/${v.day}/${v.year} ${v.hour}:${v.minute} IST`;};
  cv._hoverItems=visible.map(i=>{const day=b.d?.[i]||c.date||'',dayIndex=multi?dates.indexOf(day):-1,minute=daily?0:b.t[i]-(dayIndex>=0?dayIndex*720:0);return {key:`${day}|${b.t[i]}`,index:i,x:X(b.t[i]+(daily?.5:tf/2)),label:daily?`${hoverStamp(day,0).split(' ').slice(0,1)[0]} · Daily IST`:hoverStamp(day,minute),o:b.o[i],h:b.h[i],l:b.l[i],c:b.c[i],v:b.v[i]||0};});
  cv._showChartHover=(px,py)=>{if(px<L||px>W-R||!cv._hoverItems?.length){if(cv._hoverTip)cv._hoverTip.style.display='none';return}let item=cv._hoverItems.reduce((best,p)=>Math.abs(p.x-px)<Math.abs(best.x-px)?p:best,cv._hoverItems[0]);const tip=cv._hoverTip;if(!tip)return;const fmt=n=>Number.isFinite(Number(n))?Number(n).toFixed(Number(n)<1?4:2):'—';tip.textContent=item.label;tip.style.display='block';if(cv.id==='kwModalCanvas'){tip.style.left='' ;tip.style.top='' ;}else{tip.style.left=Math.max(4,cv.offsetLeft+8)+'px';tip.style.top=(cv.offsetTop+cv.offsetHeight+2)+'px';}cv.title=item.label;};
  cv._viewSpan=viewSpan;cv._panSpan=span;cv._plotWidth=pw;cv._drawEvArgs=[c,sel,bzChecked,targetH,timeframe,zoom];
  if(!cv._panBound){
    cv._panBound=true;cv.style.cursor='grab';
    const hoverParent=cv.parentElement;if(hoverParent){if(getComputedStyle(hoverParent).position==='static')hoverParent.style.position='relative';const tip=document.createElement('div');tip.className='chartHoverTip';tip.style.cssText='display:none;position:absolute;z-index:9;pointer-events:none;background:rgba(8,12,16,.96);border:1px solid #465362;color:#e7eef5;padding:5px 7px;border-radius:3px;font:11px Consolas,monospace;white-space:nowrap;box-shadow:0 2px 8px #000';if(cv.id==='kwModalCanvas'){tip.style.position='static';tip.style.background='transparent';tip.style.border='0';tip.style.padding='0';document.querySelector('.kwModalLegend')?.appendChild(tip)}else hoverParent.appendChild(tip);cv._hoverTip=tip;}
    cv.addEventListener('pointerdown',e=>{if(e.button!==0)return;if(cv._measureMode&&e.offsetX>L&&e.offsetX<W-R){const p=cv._hoverItems?.reduce((a,z)=>Math.abs(z.x-e.offsetX)<Math.abs(a.x-e.offsetX)?z:a,cv._hoverItems?.[0]);if(p){cv._measureActive=true;cv._measureStartKey=p.key;cv._measureEndKey=p.key;cv.setPointerCapture(e.pointerId);drawEv(cv,...cv._drawEvArgs);}return;}cv._panActive=true;cv._panMode=e.offsetX<=L?'price':'time';cv._panStartX=e.clientX;cv._panStartY=e.clientY;cv._panStartOffset=cv._panOffset||0;cv._panStartYZoom=cv._yZoom||1;cv._panDidMove=false;cv.setPointerCapture(e.pointerId);cv.style.cursor=cv._panMode==='price'?'ns-resize':'grabbing';});
    cv.addEventListener('pointermove',e=>{if(cv._measureActive){const p=cv._hoverItems?.reduce((a,z)=>Math.abs(z.x-e.offsetX)<Math.abs(a.x-e.offsetX)?z:a,cv._hoverItems?.[0]);if(p){cv._measureEndKey=p.key;drawEv(cv,...cv._drawEvArgs);}return;}if(!cv._panActive){cv.style.cursor=e.offsetX<=L?'ns-resize':'grab';cv._showChartHover?.(e.offsetX,e.offsetY);return;}if(cv._hoverTip)cv._hoverTip.style.display='none';const dx=e.clientX-cv._panStartX,dy=e.clientY-cv._panStartY;if(Math.abs(dx)>3||Math.abs(dy)>3)cv._panDidMove=true;if(cv._panMode==='price'){cv._yZoom=Math.max(.5,Math.min(20,cv._panStartYZoom*Math.exp(-dy/140)));cv._onYZoomChange?.(cv._yZoom);}else{cv._panOffset=Math.max(cv._panMinOffset,Math.min(cv._panMaxOffset,cv._panStartOffset-dx*cv._viewSpan/Math.max(1,cv._plotWidth)));cv._onPanChange?.(cv._panOffset);}if(cv._drawEvArgs)drawEv(cv,...cv._drawEvArgs);});
    cv.addEventListener('pointerleave',()=>{if(cv._hoverTip)cv._hoverTip.style.display='none';});
    cv.addEventListener('pointerup',e=>{if(cv._measureActive){cv._measureActive=false;return;}if(!cv._panActive)return;cv._panActive=false;cv.style.cursor='grab';if(cv._panDidMove&&cv._panMode==='time'){const dx=e.clientX-cv._panStartX;if(cv._panMinOffset===cv._panMaxOffset&&Math.abs(dx)>20)cv._onPanEdge?.(dx<0?1:-1);else if(cv._panOffset>=cv._panMaxOffset-1)cv._onPanEdge?.(1);else if(cv._panOffset<=cv._panMinOffset+1)cv._onPanEdge?.(-1);}});
    cv.addEventListener('pointercancel',()=>{cv._panActive=false;cv.style.cursor='grab';});
    cv.addEventListener('wheel',e=>{if(e.offsetX>L)return;e.preventDefault();cv._yZoom=Math.max(.5,Math.min(20,(cv._yZoom||1)*Math.exp(-Math.sign(e.deltaY)*.15)));cv._onYZoomChange?.(cv._yZoom);if(cv._drawEvArgs)drawEv(cv,...cv._drawEvArgs);},{passive:false});
    cv.addEventListener('click',e=>{if(cv._panDidMove||cv._measureMode){e.preventDefault();e.stopPropagation();cv._panDidMove=false;}},true);
  }
  // Premarket light grey background
  x.fillStyle = 'rgba(255, 255, 255, 0.085)';
  if(!daily)for(let day=0;day<(multi?dates.length:1);day++)x.fillRect(X(240+day*720),T,X(570+day*720)-X(240+day*720),ph+6+VH);
  x.fillStyle = '#8e8e9e';
  x.font = 'bold 10px Consolas,monospace';
  if(!daily){const anchorIndex=multi?Math.max(0,dates.indexOf(cv._anchorDate||c.date)):0;x.fillText('PRE-MARKET', X(240+anchorIndex*720) + 6, T + 13);}
  x.lineWidth = 1;
  x.strokeStyle = '#292d32';
  x.fillStyle = '#b3b8be';
  for(let k = 0; k <= 5; k++){
    const v = lo + (hi - lo) * k / 5, y = Y(v);
    x.beginPath();
    x.moveTo(L, y);
    x.lineTo(W - R, y);
    x.stroke();
    x.fillText(v >= 1 ? v.toFixed(2) : v.toFixed(3), 2, y + 4);
  }
  const tickStep=daily?(b.t.length>12?Math.ceil(b.t.length/10):1):multi?180:60;
  for(let m = daily?0:240; m <= (daily?b.t.length-1:240+span); m += tickStep){
    const xx = X(daily?m+.5:m);
    x.beginPath();
    x.moveTo(xx, T);
    x.lineTo(xx, vy1);
    x.stroke();
    x.fillText(daily?(b.d?.[m]||'').slice(5):multi?((m-240)%720===0?(dates[Math.min(dates.length-1,Math.floor((m-240)/720))]||'').slice(5):''):hm(m,c.off), xx - 14, H - 5);
  }
  x.strokeStyle = '#777';
  x.setLineDash([2, 3]);
  x.beginPath();
  if(!daily&&!multi){x.moveTo(X(570), T);x.lineTo(X(570), vy1);}
  x.stroke();
  x.setLineDash([]);
  const dash = (v, col) => {
    if(v == null || v < lo || v > hi) return;
    x.strokeStyle = col;
    x.setLineDash([4, 4]);
    x.beginPath();
    x.moveTo(L, Y(v));
    x.lineTo(W - R, Y(v));
    x.stroke();
    x.setLineDash([]);
  };
  if(!daily){dash(c.pc, '#888');dash(c.pmh, '#ffd23f');}
  // ---- bone zone: 9/20 EMA cloud + Bollinger Bands (20, 2) on the 2-min closes
  const BZ = bzChecked ? bone(b) : null;
  const clipP = () => { x.save(); x.beginPath(); x.rect(L, T, pw, ph); x.clip(); };
  const poly = (a, col, w) => {
    x.strokeStyle = col;
    x.lineWidth = w;
    x.beginPath();
    let on = false;
    for(let i = 0; i < a.length; i++){
      if(a[i] == null){ on = false; continue; }
      const xx = X(b.t[i] + tf / 2), yy = Y(a[i]);
      if(!on){ x.moveTo(xx, yy); on = true; } else x.lineTo(xx, yy);
    }
    x.stroke();
    x.lineWidth = 1;
  };
  if(BZ){
    clipP();
    for(let i = 1; i < b.t.length; i++){
      x.fillStyle = BZ.e9[i] > BZ.e20[i] ? 'rgba(0,255,0,0.30)' : 'rgba(255,0,0,0.30)';
      x.beginPath();
      x.moveTo(X(b.t[i-1] + 1), Y(BZ.e9[i-1]));
      x.lineTo(X(b.t[i] + 1), Y(BZ.e9[i]));
      x.lineTo(X(b.t[i] + 1), Y(BZ.e20[i]));
      x.lineTo(X(b.t[i-1] + 1), Y(BZ.e20[i-1]));
      x.closePath();
      x.fill();
    }
    poly(BZ.bu, '#7a7a7a', 1);
    poly(BZ.bl, '#7a7a7a', 1);
    x.restore();
  }
  const bw = daily?Math.max(3,Math.min(24,pw/b.t.length*.65)):Math.max(1.4, pw / (viewSpan / tf) * 0.84), vmax = Math.max(...visible.map(i=>b.v[i]||0)) || 1;
  for(let i = 0; i < b.t.length; i++){
    if(b.t[i]<viewStart-tf||b.t[i]>viewStart+viewSpan+tf)continue;
    const xx = X(b.t[i] + (daily?.5:tf / 2)), up = b.c[i] >= b.o[i], col = up ? '#3ddc84' : '#ff5a5a';
    x.fillStyle = up ? 'rgba(61,220,132,0.55)' : 'rgba(255,90,90,0.55)';
    const vh = VH * b.v[i] / vmax;
    x.fillRect(xx - bw / 2, vy1 - vh, bw, vh);
    x.strokeStyle = col;
    x.lineWidth = Math.max(1.1, Math.min(1.8, tf / 2));
    x.beginPath();
    x.moveTo(xx, Y(b.h[i]));
    x.lineTo(xx, Y(b.l[i]));
    x.stroke();
    x.fillStyle = col;
    const yt = Y(Math.max(b.o[i], b.c[i]));
    x.fillRect(xx - bw / 2, yt, bw, Math.max(1.5, Math.abs(Y(b.o[i]) - Y(b.c[i]))));
    x.lineWidth = 1;
  }
  if(BZ){
    clipP();
    poly(BZ.e9, 'rgb(215,255,110)', isLarge ? 1.6 : 1.2);
    poly(BZ.e20, 'rgb(255,165,0)', isLarge ? 1.6 : 1.2);
    x.restore();
  }
  x.strokeStyle = '#29a8ff';
  x.lineWidth = isLarge ? 1.6 : 1.3;
  x.beginPath();
  let on = false, lastVwapDate=null;
  for(let i = 0; i < b.t.length; i++){
    const v = b.vw[i];
    if(v == null){ on = false; continue; }
    if(multi&&b.d?.[i]!==lastVwapDate)on=false;
    const xx = X(b.t[i] + tf / 2), yy = Y(Math.min(hi, Math.max(lo, v)));
    if(!on){ x.moveTo(xx, yy); on = true; } else x.lineTo(xx, yy);
    lastVwapDate=b.d?.[i]||null;
  }
  x.stroke();
  x.lineWidth = 1;
  x.fillStyle = '#8a8a8a';
  x.fillText('vol', L + 2, vy0 + 10);
  if(!multi&&!daily)sg.forEach((s, idx) => {
    const col = MC[s.name] || '#fff', xs = X(s.m), ys = Y(s.p);
    let xm, ym, pl;
    if(s.stop_m != null){
      xm = X(s.stop_m);
      ym = Y(s.p * (1 + c.stop));
      pl = -c.stop;
    } else if(c.cover){
      xm = X(c.cover.m);
      ym = Y(c.cover.p);
      pl = s.pnl;
    } else return;
    x.strokeStyle = col;
    x.setLineDash([3, 3]);
    x.beginPath();
    x.moveTo(xs, ys);
    x.lineTo(xm, ym);
    x.stroke();
    x.setLineDash([]);
    tri(x, xs, ys, 'down', col);
    tri(x, xm, ym, 'up', '#3ddc84');
    x.fillStyle = col;
    x.fillText('SELL ' + (MA[s.name] || ''), xs + 6, ys - 8 - idx * 0);
    x.fillStyle = pl >= 0 ? '#3ddc84' : '#ff5a5a';
    x.fillText('BUY ' + (pl * 100 >= 0 ? '+' : '') + (pl * 100).toFixed(0) + '%', xm + 6, ym + 16);
  });
  if(cv._measureStartKey&&cv._measureEndKey){
    const a=cv._hoverItems?.find(p=>p.key===cv._measureStartKey),z=cv._hoverItems?.find(p=>p.key===cv._measureEndKey);
    const ia=cv._hoverItems?.findIndex(p=>p.key===cv._measureStartKey),iz=cv._hoverItems?.findIndex(p=>p.key===cv._measureEndKey);
    if(a&&z&&ia>=0&&iz>=0){const left=Math.min(a.x,z.x),right=Math.max(a.x,z.x),from=Math.min(a.index,z.index),to=Math.max(a.index,z.index);let vol=0,dv=0;for(let i=from;i<=to;i++){const v=Number(b.v[i])||0;vol+=v;dv+=((Number(b.h[i])+Number(b.l[i])+Number(b.c[i]))/3)*v;}
      x.save();x.fillStyle='rgba(72,183,255,.14)';x.fillRect(left,T,Math.max(2,right-left),vy1-T);x.strokeStyle='#62c8ff';x.lineWidth=2;x.beginPath();x.moveTo(left,T);x.lineTo(left,vy1);x.moveTo(right,T);x.lineTo(right,vy1);x.stroke();
      const rot=Number(c.float_shares)>0?`${(vol/Number(c.float_shares)).toFixed(2)}x`:'Unavailable';const lines=[`${a.label} to ${z.label}`,`Volume ${vol.toLocaleString('en-US',{maximumFractionDigits:0})}`,`Dollar volume ~$${dv.toLocaleString('en-US',{maximumFractionDigits:0})}`,`Float rotation ${rot}${Number(c.float_shares)>0?' (float may be stale)':''}`];
      x.font='bold 11px Consolas,monospace';const boxW=Math.min(W-L-R,Math.max(...lines.map(t=>x.measureText(t).width))+16),boxH=lines.length*16+12,bx=Math.min(W-R-boxW,Math.max(L,left+6)),by=T+5;x.fillStyle='rgba(6,13,21,.96)';x.fillRect(bx,by,boxW,boxH);x.strokeStyle='#62c8ff';x.strokeRect(bx,by,boxW,boxH);lines.forEach((line,k)=>{x.fillStyle=k===0?'#8cb8d6':'#edf6ff';x.fillText(line,bx+8,by+16+k*16)});x.restore();}
  }
}

let currentModalIndex = 0;
let currentModalSlot = 0;

window.openChartModal = function(i){
  if(!activeTab || !openTabs[activeTab]) return;
  const list = openTabs[activeTab].EV || [];
  if(!list.length || i < 0 || i >= list.length) return;
  currentModalSlot = i;
  const card=openTabs[activeTab].container.querySelector(`[data-ev-slot="${i}"]`);
  currentModalIndex = card && Number.isInteger(card._sessionPos) ? card._sessionPos : i;

  const mainSig = openTabs[activeTab].container.querySelector('#sig');
  const mainBz = openTabs[activeTab].container.querySelector('#bz');

  const modelNames = (openTabs[activeTab].result && openTabs[activeTab].result.model_names) || [];
  let sigHtml = '<option value="none">No signals</option><option value="all">All entry models</option>' +
    modelNames.map(n => `<option value="${esc(n)}">${esc(n)}</option>`).join('');
  $('#cm_sig').innerHTML = sigHtml;

  if(mainSig) $('#cm_sig').value = mainSig.value;
  if(mainBz) $('#cm_bz').checked = mainBz.checked;
  $('#cm_tf').value = card && card._chartTf ? card._chartTf : '2m';
  $('#cm_days').value=String(card?card._chartDays||1:1);

  $('#chart_modal').style.display = 'flex';
  setTimeout(renderModalChart, 30);
};

window.renderModalChart = function(){
  if(!activeTab || !openTabs[activeTab]) return;
  const tab=openTabs[activeTab], list=tab.EV||[];
  const card=tab.container.querySelector(`[data-ev-slot="${currentModalSlot}"]`);
  const item=card&&card._chartItem;
  if(!item) return;

  $('#cm_title').textContent = item.title;
  const eventDate=card?._eventDate||item.date;const eventParts=String(eventDate||'').slice(0,10).split('-');$('#cm_event_date').textContent=eventParts.length===3?`Event ${eventParts[1]}/${eventParts[2]}/${eventParts[0]}`:'';
  $('#cm_badge').innerHTML = item.sim 
    ? '<span style="color:#3ddc84;background:#141414;border:1px solid #333;padding:2px 8px;border-radius:2px;font-size:11px">SIMILAR SETUP</span>' 
    : '<span style="color:#777;font-size:11px">(not in similar set)</span>';
  const sessionCount=(tab.sessionDates||[]).length, sessionPos=card?card._sessionPos:-1;
  const cv = $('#cm_canvas');
  const body = $('#cm_body');
  const targetH = Math.max(380, (body.clientHeight || 600) - 10);
  const sel = $('#cm_sig').value;
  const bzChecked = $('#cm_bz').checked;

  const chart=card?._contextChart?{...item.chart,bars:card._contextChart}:item.chart;
  cv._anchorDate=card?._sessionDate||item.date;cv._panOffset=card?._panOffset||0;cv._yZoom=card?._yZoom||1;cv._onPanChange=value=>{if(card)card._panOffset=value;};cv._onYZoomChange=value=>{if(card)card._yZoom=value;};cv._onPanEdge=card?._onPanEdge?side=>{card._panOffset=cv._panOffset;card._onPanEdge(side);}:null;
  drawEv(cv, chart, sel, bzChecked, targetH, $('#cm_tf').value || (card?card._chartTf:'2m'),card?._zoom||1);
};

window.nextModalChart = function(){
  if(!activeTab || !openTabs[activeTab]) return;
  const tab=openTabs[activeTab],card=tab.container.querySelector(`[data-ev-slot="${currentModalSlot}"]`);
  currentModalIndex=card?card._sessionPos+1:currentModalIndex;
  navigateEvCard(card,1,tab);
};

window.prevModalChart = function(){
  if(!activeTab||!openTabs[activeTab])return;
  const tab=openTabs[activeTab],card=tab.container.querySelector(`[data-ev-slot="${currentModalSlot}"]`);
  currentModalIndex=card?card._sessionPos-1:currentModalIndex;
  navigateEvCard(card,-1,tab);
};

window.closeChartModal = function(){
  $('#chart_modal').style.display = 'none';
};

$('#cm_sig').onchange = () => {
  renderModalChart();
  if(activeTab && openTabs[activeTab]){
    const mainSig = openTabs[activeTab].container.querySelector('#sig');
    if(mainSig){
      mainSig.value = $('#cm_sig').value;
      drawEvAll(openTabs[activeTab].EV, openTabs[activeTab].container);
    }
  }
};

$('#cm_tf').onchange = () => {
  if(!activeTab || !openTabs[activeTab]) return;
  const card=openTabs[activeTab].container.querySelector(`[data-ev-slot="${currentModalSlot}"]`);
  if(card){
    card._chartTf=$('#cm_tf').value;
    card._panOffset=0;
    card._yZoom=1;
    card._chartDays=card._chartTf==='1d'?60:1;
    $('#cm_days').value=String(card._chartDays);
    const cardSelect=card.querySelector('.evtf');
    if(cardSelect) cardSelect.value=card._chartTf;
    const item=card._chartItem;
    if(item) drawEv(card.querySelector(`#ev${currentModalSlot}`),item.chart,
      openTabs[activeTab].container.querySelector('#sig')?.value||'none',
      openTabs[activeTab].container.querySelector('#bz')?.checked||false,undefined,card._chartTf);
    if(card._chartDays===1&&card._chartTf!=='1d')loadChartContext(card,openTabs[activeTab]);
    else if((card._chartTf==='1d')!==!!card._contextDaily||!card._contextChart)loadChartContext(card,openTabs[activeTab]);
    else drawEvAll(openTabs[activeTab].EV,openTabs[activeTab].container);
  }
  renderModalChart();
};

$('#cm_days').onchange=()=>{if(!activeTab||!openTabs[activeTab])return;const card=openTabs[activeTab].container.querySelector(`[data-ev-slot="${currentModalSlot}"]`);if(card){card._chartDays=Number($('#cm_days').value);loadChartContext(card,openTabs[activeTab]);}};
['cm_zoom_in','cm_zoom_out'].forEach(id=>$('#'+id).onclick=()=>{if(!activeTab||!openTabs[activeTab])return;const card=openTabs[activeTab].container.querySelector(`[data-ev-slot="${currentModalSlot}"]`);if(card){card._zoom=Math.max(.5,Math.min(8,(card._zoom||1)*(id==='cm_zoom_in'?1.25:.8)));renderModalChart();drawEvAll(openTabs[activeTab].EV,openTabs[activeTab].container);}});

$('#cm_bz').onchange = () => {
  renderModalChart();
  if(activeTab && openTabs[activeTab]){
    const mainBz = openTabs[activeTab].container.querySelector('#bz');
    if(mainBz){
      mainBz.checked = $('#cm_bz').checked;
      drawEvAll(openTabs[activeTab].EV, openTabs[activeTab].container);
    }
  }
};

function drawChart(ch, cvElement, hoverState){
  const cv = cvElement || (activeTab && openTabs[activeTab] ? openTabs[activeTab].container.querySelector('#cv') : $('#cv'));
  if(!cv || !ch || !ch.labels || !ch.labels.length) return;
  cv._chartData = ch;
  cv.style.cursor = 'crosshair';

  if(!cv._hoverEventsBound){
    cv._hoverEventsBound = true;
    cv.addEventListener('mousemove', function(e){
      const rect = cv.getBoundingClientRect();
      const mx = e.clientX - rect.left;
      const my = e.clientY - rect.top;
      drawChart(cv._chartData, cv, {mx, my});
    });
    cv.addEventListener('mouseleave', function(){
      drawChart(cv._chartData, cv, null);
    });
    cv.addEventListener('touchmove', function(e){
      if(e.touches && e.touches[0]){
        const rect = cv.getBoundingClientRect();
        const mx = e.touches[0].clientX - rect.left;
        const my = e.touches[0].clientY - rect.top;
        drawChart(cv._chartData, cv, {mx, my});
      }
    }, {passive: true});
    cv.addEventListener('touchend', function(){
      drawChart(cv._chartData, cv, null);
    });
  }

  const parentW = (cv.parentElement && cv.parentElement.clientWidth) ? cv.parentElement.clientWidth : 0;
  const dpr=window.devicePixelRatio||1, W=Math.max(300, parentW-28), H=400;
  const reqW = Math.round(W * dpr), reqH = Math.round(H * dpr);
  if(cv.width !== reqW || cv.height !== reqH){
    cv.style.width=W+'px'; cv.style.height=H+'px';
    cv.width=reqW; cv.height=reqH;
  }
  const x=cv.getContext('2d');
  x.setTransform(dpr,0,0,dpr,0,0);
  x.clearRect(0,0,W,H);
  x.font='12px Consolas,monospace';

  const gapMode = ch.normalization === 'gap';
  const plot = gapMode ? {...ch, labels:ch.gap_labels||ch.labels, open_k:ch.gap_open_k,
    now_k:ch.gap_now_k, paths:ch.gap_paths||[], med:ch.gap_med||[], p25:ch.gap_p25||[],
    p75:ch.gap_p75||[], today:ch.gap_today||[], proj:ch.gap_proj||[]} : ch;
  const L=52,R=14,T=12,B=26, pw=W-L-R, ph=H-T-B, n=plot.labels.length;
  const vs=[0]; [plot.med,plot.p25,plot.p75,plot.today,plot.proj,...plot.paths].forEach(a=>a.forEach(v=>{if(v!=null)vs.push(v)}));
  let lo=Math.max(Math.min(...vs),-0.95), hi=Math.min(Math.max(...vs),10);
  if(hi-lo<0.1){hi+=0.05;lo-=0.05}
  const X=i=>L+pw*i/(n-1), Y=v=>T+ph*(1-(Math.max(lo,Math.min(hi,v))-lo)/(hi-lo));

  // Premarket light grey background on path chart
  if(plot.open_k != null && plot.open_k > 0){
    x.fillStyle = 'rgba(255, 255, 255, 0.085)';
    x.fillRect(L, T, X(plot.open_k) - L, ph);
    x.fillStyle = '#8e8e9e';
    x.fillText('PRE-MARKET', L + 6, T + 13);
  }
  x.strokeStyle='#222'; x.fillStyle='#8a8a8a'; x.lineWidth=1;
  for(let k=0;k<=5;k++){const v=lo+(hi-lo)*k/5, y=Y(v); x.beginPath();x.moveTo(L,y);x.lineTo(W-R,y);x.stroke(); x.fillText((v*100).toFixed(0)+'%',4,y+4);}
  const xTickStep = gapMode ? 24 : 6;
  for(let i=0;i<n;i+=xTickStep){const xx=X(i); x.beginPath();x.moveTo(xx,T);x.lineTo(xx,T+ph);x.stroke(); x.fillText(plot.labels[i],xx-14,H-8);}
  x.strokeStyle='#666'; x.beginPath(); x.moveTo(L,Y(0)); x.lineTo(W-R,Y(0)); x.stroke();
  if(gapMode && lo <= 1 && hi >= 1){
    x.strokeStyle='#697386'; x.setLineDash([3,4]); x.beginPath(); x.moveTo(L,Y(1)); x.lineTo(W-R,Y(1)); x.stroke(); x.setLineDash([]);
    x.fillStyle='#9aa4b2'; x.fillText('PM HIGH 100%',L+6,Math.max(T+26,Y(1)-4));
  }
  if(plot.open_k != null && plot.open_k >= 0 && plot.open_k < n){
    x.strokeStyle='#777'; x.setLineDash([2,3]); x.beginPath(); x.moveTo(X(plot.open_k),T); x.lineTo(X(plot.open_k),T+ph); x.stroke(); x.setLineDash([]);
    x.fillStyle='#777'; x.fillText('US open',X(plot.open_k)+4,T+12);
  }
  const line=(a,col,w,dash)=>{x.strokeStyle=col;x.lineWidth=w;x.setLineDash(dash||[]);x.beginPath();let on=false;a.forEach((v,i)=>{if(v==null){on=false;return;} if(!on){x.moveTo(X(i),Y(v));on=true}else x.lineTo(X(i),Y(v));});x.stroke();x.setLineDash([]);};
  plot.paths.forEach(p=>line(p,'rgba(120,160,200,0.20)',1));
  x.fillStyle='rgba(108,196,255,0.14)'; x.beginPath(); let started=false;
  for(let i=0;i<n;i++){ if(plot.p75[i]==null) continue; if(!started){x.moveTo(X(i),Y(plot.p75[i]));started=true}else x.lineTo(X(i),Y(plot.p75[i])); }
  for(let i=n-1;i>=0;i--){ if(plot.p25[i]==null) continue; x.lineTo(X(i),Y(plot.p25[i])); }
  x.closePath(); x.fill();
  line(plot.med,'#6cc4ff',2.5);
  line(plot.proj,'#ffd23f',2,[6,5]);
  line(plot.today,'#ffd23f',2.5);
  if(plot.now_k!=null&&plot.today[plot.now_k]!=null){x.fillStyle='#ffd23f';x.beginPath();x.arc(X(plot.now_k),Y(plot.today[plot.now_k]),4,0,7);x.fill();}

  // Interactive Hover: Crosshair, Axis Badges, and HUD Tooltip
  if(hoverState && hoverState.mx != null && hoverState.my != null){
    const mx = hoverState.mx;
    const my = hoverState.my;
    if(mx >= L && mx <= W - R && my >= T && my <= T + ph){
      const ratio = Math.max(0, Math.min(1, (mx - L) / pw));
      const idx = Math.min(n - 1, Math.max(0, Math.round(ratio * (n - 1))));
      const snapX = X(idx);

      // Value calculation
      const cursorVal = lo + (1 - (my - T) / ph) * (hi - lo);
      const cursorPctStr = (cursorVal >= 0 ? '+' : '') + (cursorVal * 100).toFixed(1) + '%';
      const timeStr = plot.labels[idx] || '';

      x.save();

      // Crosshair lines
      x.strokeStyle = 'rgba(255, 255, 255, 0.4)';
      x.lineWidth = 1;
      x.setLineDash([3, 3]);

      // Vertical line
      x.beginPath();
      x.moveTo(snapX, T);
      x.lineTo(snapX, T + ph);
      x.stroke();

      // Horizontal line
      x.beginPath();
      x.moveTo(L, my);
      x.lineTo(W - R, my);
      x.stroke();
      x.setLineDash([]);

      // Highlight dots on data curves
      if(plot.today && plot.today[idx] != null){
        const ty = Y(plot.today[idx]);
        x.fillStyle = '#ffd23f';
        x.strokeStyle = '#fff';
        x.lineWidth = 2;
        x.beginPath();
        x.arc(snapX, ty, 5, 0, Math.PI * 2);
        x.fill();
        x.stroke();
      }
      if(plot.med && plot.med[idx] != null){
        const my_pt = Y(plot.med[idx]);
        x.fillStyle = '#6cc4ff';
        x.strokeStyle = '#fff';
        x.lineWidth = 1.5;
        x.beginPath();
        x.arc(snapX, my_pt, 4.5, 0, Math.PI * 2);
        x.fill();
        x.stroke();
      }
      if(plot.proj && plot.proj[idx] != null){
        const py = Y(plot.proj[idx]);
        x.fillStyle = '#ffd23f';
        x.beginPath();
        x.arc(snapX, py, 3.5, 0, Math.PI * 2);
        x.fill();
      }

      // X-Axis Badge (Bottom - Time)
      x.font = 'bold 11px Consolas,monospace';
      const xBadgeText = timeStr ? (gapMode ? timeStr : timeStr + ' IST') : '';
      const xBadgeW = x.measureText(xBadgeText).width + 12;
      const xBadgeX = Math.max(L, Math.min(W - R - xBadgeW, snapX - xBadgeW / 2));
      const xBadgeY = H - 24;
      x.fillStyle = '#0a101d';
      x.fillRect(xBadgeX, xBadgeY, xBadgeW, 18);
      x.strokeStyle = '#29a8ff';
      x.lineWidth = 1;
      x.strokeRect(xBadgeX, xBadgeY, xBadgeW, 18);
      x.fillStyle = '#ffffff';
      x.fillText(xBadgeText, xBadgeX + 6, xBadgeY + 13);

      // Y-Axis Badge (Left - % from prev close)
      const yColor = cursorVal >= 0 ? '#3ddc84' : '#ff5a5a';
      const yBadgeText = cursorPctStr;
      const yBadgeW = L - 4;
      const yBadgeH = 16;
      const yBadgeX = 2;
      const yBadgeY = Math.max(T, Math.min(T + ph - yBadgeH, my - yBadgeH / 2));
      x.fillStyle = '#0a101d';
      x.fillRect(yBadgeX, yBadgeY, yBadgeW, yBadgeH);
      x.strokeStyle = yColor;
      x.lineWidth = 1;
      x.strokeRect(yBadgeX, yBadgeY, yBadgeW, yBadgeH);
      x.fillStyle = yColor;
      x.fillText(yBadgeText, yBadgeX + 3, yBadgeY + 12);

      // Tooltip HUD Card
      const rows = [];
      rows.push({ label: gapMode ? 'From PM high' : 'Time (X)', val: gapMode ? timeStr : timeStr + ' IST', col: '#ffffff' });
      rows.push({ label: gapMode ? 'Gap retained (Y)' : 'Value (Y)', val: cursorPctStr, col: yColor });
      if(plot.today && plot.today[idx] != null){
        const v = plot.today[idx];
        rows.push({ label: 'Today', val: (v >= 0 ? '+' : '') + (v * 100).toFixed(1) + '%', col: '#ffd23f' });
      }
      if(plot.proj && plot.proj[idx] != null){
        const v = plot.proj[idx];
        rows.push({ label: 'Projected', val: (v >= 0 ? '+' : '') + (v * 100).toFixed(1) + '%', col: '#ffd23f' });
      }
      if(plot.med && plot.med[idx] != null){
        const v = plot.med[idx];
        rows.push({ label: 'History Median', val: (v >= 0 ? '+' : '') + (v * 100).toFixed(1) + '%', col: '#6cc4ff' });
      }
      if(plot.p25 && plot.p75 && plot.p25[idx] != null && plot.p75[idx] != null){
        const v25 = (plot.p25[idx] * 100).toFixed(1);
        const v75 = (plot.p75[idx] * 100).toFixed(1);
        rows.push({ label: 'p25–p75 Range', val: `${v25}% to ${v75}%`, col: '#78a0c8' });
      }

      x.font = '11px Consolas,monospace';
      let maxW = 150;
      rows.forEach(r => {
        const tw = x.measureText(`${r.label}: ${r.val}`).width;
        if(tw + 20 > maxW) maxW = tw + 20;
      });

      const pad = 8, lineH = 17;
      const boxW = maxW;
      const boxH = rows.length * lineH + pad * 2;
      let boxX = snapX + 16;
      if(boxX + boxW > W - R) boxX = snapX - boxW - 16;
      if(boxX < L + 2) boxX = L + 2;

      let boxY = my - boxH / 2;
      if(boxY < T + 4) boxY = T + 4;
      if(boxY + boxH > T + ph) boxY = T + ph - boxH;

      // Draw Tooltip Container
      x.fillStyle = 'rgba(10, 15, 26, 0.95)';
      x.fillRect(boxX, boxY, boxW, boxH);
      x.strokeStyle = '#29a8ff';
      x.lineWidth = 1;
      x.strokeRect(boxX, boxY, boxW, boxH);

      // Tooltip Rows
      rows.forEach((r, ri) => {
        const ry = boxY + pad + (ri + 1) * lineH - 4;
        x.fillStyle = '#8e9aa8';
        x.fillText(r.label + ':', boxX + 8, ry);
        const lw = x.measureText(r.label + ': ').width;
        x.fillStyle = r.col;
        x.fillText(r.val, boxX + 8 + lw, ry);
      });

      x.restore();
    }
  }
}
window.addEventListener('resize',()=>{
  if($('#chart_modal').style.display === 'flex'){
    renderModalChart();
  }
  if(activeTab && openTabs[activeTab]){
    const tab = openTabs[activeTab];
    if(tab.lastChart){
      const cv = tab.container.querySelector('#cv');
      if(cv) drawChart(tab.lastChart, cv);
    }
    if(tab.EV && tab.EV.length){
      drawEvAll(tab.EV, tab.container);
    }
  }
});

async function copyAll(){
  if(!activeTab || !openTabs[activeTab] || !openTabs[activeTab].lastText) return;
  const txt = openTabs[activeTab].lastText;
  try{await navigator.clipboard.writeText(txt)}
  catch(e){const a=document.createElement('textarea');a.value=txt;document.body.appendChild(a);a.select();document.execCommand('copy');a.remove();}
  const b=$('#cp'); b.textContent='Copied ✓'; setTimeout(()=>b.textContent='Copy all',1200);
}
$('#go').onclick=()=>run(false); $('#rf').onclick=()=>run(true); $('#cp').onclick=copyAll;
$('#t').addEventListener('keydown',e=>{if(e.key==='Enter')run(false)});
$('#gate_btn').onclick=submitGate;
$('#gate_name').addEventListener('keydown',e=>{if(e.key==='Enter')submitGate();});
if($('#gate_create_btn')) $('#gate_create_btn').onclick=submitCreate;
if($('#gate_new_pwd2')) $('#gate_new_pwd2').addEventListener('keydown',e=>{if(e.key==='Enter')submitCreate();});
if($('#gate_new_pwd')) $('#gate_new_pwd').addEventListener('keydown',e=>{if(e.key==='Enter')$('#gate_new_pwd2').focus();});
if($('#gate_login_btn')) $('#gate_login_btn').onclick=submitLogin;
if($('#gate_pwd')) $('#gate_pwd').addEventListener('keydown',e=>{if(e.key==='Enter')submitLogin();});
$('#logout_btn').onclick=()=>{
  if(confirm('Log out from this browser?')) doLogout();
};

let adminPwd = '';
let appUiVisibility = {scanner:false, runner_stats:true};
let appUiVisibilityTimer = null;

function applyAppUiVisibility(value){
  appUiVisibility = {
    scanner: value && value.scanner !== false,
    runner_stats: value && value.runner_stats !== false
  };
  const scannerButton = $('#scanner_open_btn');
  const runnerButton = $('#nav_multiday_btn');
  const runnerNewTabButton = $('#open_multiday_newtab');
  const runnerAdminButton = $('#multiday_stats_btn');
  if(scannerButton) scannerButton.style.display = appUiVisibility.scanner ? '' : 'none';
  if(runnerButton) runnerButton.style.display = appUiVisibility.runner_stats ? '' : 'none';
  if(runnerNewTabButton) runnerNewTabButton.style.display = appUiVisibility.runner_stats ? '' : 'none';
  if(runnerAdminButton) runnerAdminButton.style.display = appUiVisibility.runner_stats ? '' : 'none';
  if(!appUiVisibility.scanner && typeof closeScanner === 'function') closeScanner();
}

async function loadAppUiVisibility(){
  try{
    const response = await fetch('/api/ui-visibility?user='+encodeURIComponent(currentUser)+'&token='+encodeURIComponent(sessionToken));
    if(response.ok) applyAppUiVisibility(await response.json());
  } catch(e){}
}

function openPwdModal(){
  const cur = (currentUser||'').toLowerCase();
  if(!PWD_USERS.includes(cur) && !_canRename){
    return;
  }
  if(adminPwd === '500'){
    $('#admin_clear_btn').style.display='inline-block';
    $('#app_main').style.display='none';
    $('#admin_view').style.display='block';
    loadAdminSearches();
    return;
  }
  $('#pwd_modal').style.display='flex';
  $('#pwd_prompt').textContent='Enter password:';
  $('#pwd_input').value='';
  $('#pwd_err').style.display='none';
  $('#pwd_input').focus();
}

let adminRows = [];
let activeViewMode = 'user';
let tickerFilterQuery = '';

window.openUserPopup = function(u){
  const byUser = {};
  adminRows.forEach(s => {
    const usr = (s.user || 'UNKNOWN').toUpperCase();
    if(!byUser[usr]) byUser[usr] = { count: 0, tickers: {}, history: [] };
    byUser[usr].count++;
    byUser[usr].tickers[s.ticker] = (byUser[usr].tickers[s.ticker] || 0) + 1;
    byUser[usr].history.push(s);
  });

  const uname = (u || '').toUpperCase();
  const obj = byUser[uname] || { count: 0, tickers: {}, history: [] };
  const uniqueTickers = Object.entries(obj.tickers).sort((a,b)=>b[1]-a[1]);

  $('#up_name').textContent = uname;
  $('#up_summary').textContent = obj.count ? `${obj.count} total search${obj.count===1?'':'es'} • ${uniqueTickers.length} unique symbol${uniqueTickers.length===1?'':'s'}` : 'No search activity recorded';

  let contentHtml = '';
  if(!obj.count){
    contentHtml = '<div style="color:#777;text-align:center;padding:36px 10px;font-size:14px">No searches recorded yet for this user.</div>';
  } else {
    contentHtml = '<div style="font-size:12px;color:#8a8a8a;margin-bottom:8px">Symbols Searched (' + uniqueTickers.length + '):</div>' +
      '<div style="display:flex;flex-wrap:wrap;gap:8px;margin-bottom:20px">' +
      uniqueTickers.map(([tkr, cnt]) =>
        `<span style="background:#161616;border:1px solid #333;padding:5px 12px;font-size:13px;border-radius:3px;color:#fff"><b style="color:#29a8ff">${esc(tkr)}</b> <span style="color:#777;font-size:11px">(${cnt}x)</span></span>`
      ).join('') +
      '</div>' +
      '<div style="font-size:12px;color:#8a8a8a;margin-bottom:8px">Query History (Recent first):</div>' +
      '<div class="scroll" style="max-height:260px;background:#050505;border:1px solid #222;border-radius:3px;padding:0 6px">' +
      '<table style="margin:0"><thead><tr><th style="width:50px;padding-left:10px">#</th><th style="width:180px;padding-left:8px">Time (IST)</th><th style="padding-left:8px">Search</th></tr></thead><tbody>' +
      obj.history.map((h, i) =>
        `<tr><td class="d" style="padding-left:10px">${i+1}</td><td class="w" style="padding-left:8px">${esc(h.time)}</td><td class="big ${h.type==='keyword'?'':'c'}" style="font-weight:bold;padding-left:8px">${h.type==='keyword'?'⌕ ':''}${esc(h.keyword||h.ticker)}</td></tr>`
      ).join('') +
      '</tbody></table></div>';
  }

  $('#up_content').innerHTML = contentHtml;
  $('#user_popup').style.display = 'flex';
};

window.closeUserPopup = function(){
  $('#user_popup').style.display = 'none';
};

function renderAdminView(){
  const container = $('#admin_list');
  if(!container) return;

  if(!adminRows.length){
    container.innerHTML = '<div class="panel" style="padding:24px;text-align:center"><span class="d">No searches recorded yet. Search any ticker in the terminal to see it logged here!</span></div>';
    return;
  }

  if(activeViewMode === 'user'){
    // Collect all users from data (not just the fixed 3)
    const allUsersSet = new Set(['KARTHIK', 'LOCKE', 'GUNSHOW']);
    const counts = {};
    const uniqueSymbols = {};
    const lastSeen = {};
    adminRows.forEach(s => {
      const usr = (s.user || 'UNKNOWN').toUpperCase();
      allUsersSet.add(usr);
      counts[usr] = (counts[usr] || 0) + 1;
      if(!uniqueSymbols[usr]) uniqueSymbols[usr] = new Set();
      if(s.ticker) uniqueSymbols[usr].add(s.ticker.toUpperCase());
      if(!lastSeen[usr] || s.time > lastSeen[usr]) lastSeen[usr] = s.time;
    });
    const allUsers = Array.from(allUsersSet).sort((a,b) => (counts[b]||0) - (counts[a]||0));

    // Build header with search
    let html = `<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;flex-wrap:wrap;gap:10px">` +
      `<div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap">` +
      `<div style="font-size:13px;color:#8a8a8a">Click a user to view their search history:</div>` +
      `<input id="user_search_q" placeholder="Search user..." autocomplete="off" value="" style="height:32px;width:160px;font-size:13px;padding:0 10px;background:#000;border:1px solid #444;color:#fff;border-radius:3px">` +
      `</div>` +
      `<div style="font-size:12px;background:#141414;border:1px solid #333;padding:4px 10px;border-radius:3px;color:#29a8ff">Total searches logged: <b>${adminRows.length}</b></div>` +
      `</div>`;

    // Vertical user card list
    html += `<div id="user_card_list" style="display:flex;flex-direction:column;gap:8px;margin-bottom:26px">`;

    for(const u of allUsers){
      const c = counts[u] || 0;
      const uniq = uniqueSymbols[u] ? uniqueSymbols[u].size : 0;
      const countLabel = c ? `${c} search${c===1?'':'es'} &nbsp;·&nbsp; ${uniq} unique search term${uniq===1?'':'s'}` : '0 searches';
      const lastLabel = lastSeen[u] ? `<span style="font-size:11px;color:#555;margin-left:10px">last: ${esc(lastSeen[u])}</span>` : '';
      html += `<div class="user_card_row" data-uname="${esc(u)}" onclick="openUserPopup('${esc(u)}')" style="background:#0e0e0e;border:1px solid #222;padding:14px 18px;border-radius:4px;cursor:pointer;display:flex;justify-content:space-between;align-items:center;transition:background 0.12s,border-color 0.12s;user-select:none" onmouseover="this.style.background='#161616';this.style.borderColor='#29a8ff'" onmouseout="this.style.background='#0e0e0e';this.style.borderColor='#222'">` +
        `<div style="display:flex;align-items:center;gap:14px">` +
        `<div style="width:36px;height:36px;border-radius:50%;background:#181818;border:1px solid #333;display:flex;align-items:center;justify-content:center;font-size:14px;font-weight:bold;color:#29a8ff">${esc(u[0])}</div>` +
        `<div>` +
        `<div style="font-size:15px;font-weight:bold;color:#f0f0f0">${esc(u)}</div>` +
        `<div style="font-size:12px;color:${c ? '#3ddc84' : '#555'};margin-top:2px">${countLabel}${lastLabel}</div>` +
        `</div></div>` +
        `<span style="font-size:12px;color:#555">View →</span>` +
        `</div>`;
    }
    html += `</div>`;

    // Full chronological log below
    html += `<div class="panel"><div class="ttl" style="margin-bottom:10px">All Searches Chronological Log</div>`+
      `<div class="scroll"><table><thead><tr><th style="width:50px">#</th><th style="width:190px">Time (IST)</th><th style="width:140px">User</th><th>Search</th><th>Details</th></tr></thead><tbody>`+
      adminRows.map((s, idx)=>{
        const isKeyword=s.type==='keyword';
        const details=isKeyword?`${esc(s.start_date||'')} → ${esc(s.end_date||'')}${s.forms?` · ${esc(s.forms)}`:''}`:'';
        return `<tr><td class="d">${idx+1}</td><td class="w">${esc(s.time)}</td><td style="color:#ddd;font-weight:bold">${esc(s.user)}</td><td class="big ${isKeyword?'':'c'}">${isKeyword?'⌕ ':''}${esc(s.keyword||s.ticker)}</td><td class="d">${details}</td></tr>`;
      }).join('')+
      `</tbody></table></div></div>`;

    container.innerHTML = html;

    // Wire up the search filter
    const searchInput = document.getElementById('user_search_q');
    if(searchInput){
      searchInput.oninput = () => {
        const q = (searchInput.value || '').trim().toUpperCase();
        document.querySelectorAll('.user_card_row').forEach(row => {
          const name = (row.getAttribute('data-uname') || '').toUpperCase();
          row.style.display = (!q || name.includes(q)) ? 'flex' : 'none';
        });
      };
      searchInput.focus();
    }


  } else {
    // Ticker-wise view: tickers sorted by recent-wise searched
    const byTicker = {};
    adminRows.forEach(s => {
      if(s.type==='keyword') return;
      const t = (s.ticker || '').toUpperCase();
      if(!t) return;
      if(!byTicker[t]){
        byTicker[t] = {
          ticker: t,
          latestTime: s.time,
          count: 0,
          users: {},
          history: []
        };
      }
      byTicker[t].count++;
      const u = (s.user || 'UNKNOWN').toUpperCase();
      byTicker[t].users[u] = (byTicker[t].users[u] || 0) + 1;
      byTicker[t].history.push(s);
    });

    let tickerList = Object.values(byTicker).sort((a,b)=>(b.latestTime||'').localeCompare(a.latestTime||''));

    if(tickerFilterQuery){
      const q = tickerFilterQuery.toUpperCase();
      tickerList = tickerList.filter(t => t.ticker.includes(q));
    }

    let html = `<div class="panel" style="margin-top:0;padding:18px 22px">`+
      `<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;flex-wrap:wrap;gap:12px">`+
        `<div>`+
          `<div class="ttl" style="margin-bottom:4px">Tickers Searched (Recent-Wise)</div>`+
          `<div style="font-size:12px;color:#8a8a8a">Sorted from newest search to oldest (${tickerList.length} unique symbols)</div>`+
        `</div>`+
        `<div style="display:flex;align-items:center;gap:8px">`+
          `<input id="ticker_filter" placeholder="Filter ticker..." value="${esc(tickerFilterQuery)}" style="height:34px;width:180px;font-size:13px;padding:0 10px;background:#000;border:1px solid #444;color:#fff" autocomplete="off">`+
        `</div>`+
      `</div>`;

    if(!tickerList.length){
      html += `<div style="text-align:center;padding:24px;color:#666">No tickers match "${esc(tickerFilterQuery)}".</div>`;
    } else {
      html += `<div class="scroll"><table><thead><tr>`+
        `<th style="width:50px">#</th>`+
        `<th style="width:120px">Ticker</th>`+
        `<th style="width:190px">Most Recent Search (IST)</th>`+
        `<th style="width:130px">Total Searches</th>`+
        `<th>Searched By</th>`+
        `</tr></thead><tbody>`+
        tickerList.map((item, idx)=>{
          const userBadges = Object.entries(item.users).map(([u, c])=>{
            return `<span style="background:#151515;border:1px solid #333;padding:3px 8px;border-radius:3px;margin-right:6px;font-size:12px;color:#ddd"><b>${esc(u)}</b> <span style="color:#777">(${c}x)</span></span>`;
          }).join('');

          return `<tr>`+
            `<td class="d">${idx+1}</td>`+
            `<td class="big c" style="font-weight:bold;font-size:16px">${esc(item.ticker)}</td>`+
            `<td class="w">${esc(item.latestTime)}</td>`+
            `<td><span style="background:#181818;border:1px solid #333;padding:3px 9px;border-radius:3px;color:#fff">${item.count} search${item.count===1?'':'es'}</span></td>`+
            `<td>${userBadges}</td>`+
          `</tr>`;
        }).join('')+
        `</tbody></table></div>`;
    }
    html += `</div>`;

    container.innerHTML = html;

    const filterInput = $('#ticker_filter');
    if(filterInput){
      filterInput.oninput = (e)=>{
        tickerFilterQuery = (e.target.value || '').trim();
        renderAdminView();
        const f = $('#ticker_filter');
        if(f){ f.focus(); f.selectionStart = f.selectionEnd = f.value.length; }
      };
    }
  }
}

async function loadAdminSearches(){
  $('#admin_list').innerHTML = '<span class="d">Loading search logs…</span>';
  try {
    const res = await fetch('/api/admin/searches?user=' + encodeURIComponent(currentUser) + '&pwd=' + encodeURIComponent(adminPwd));
    const data = await res.json();
    if(data.error){
      $('#admin_list').innerHTML = `<span class="r">${esc(data.error)}</span>`;
      return;
    }
    adminRows = data.searches || [];
    if(data._warn && !adminRows.length){
      $('#admin_list').innerHTML = `<div class="panel" style="padding:16px;color:#ffd23f;font-size:13px">⚠ Server warning: ${esc(data._warn)}</div>`;
      return;
    }
    renderAdminView();
    if(data._warn) $('#admin_list').insertAdjacentHTML('afterbegin', `<div class="panel" style="padding:12px 16px;color:#ffd23f;font-size:13px">⚠ ${esc(data._warn)}</div>`);
  } catch(e) {
    $('#admin_list').innerHTML = `<span class="r">Failed to load search logs: ${esc(e)}</span>`;
  }
}

async function clearOldSearches(){
  const ok = confirm('Delete ALL search records older than 2 weeks (14 days) from the database?\n\nThis cannot be undone.');
  if(!ok) return;
  const btn = $('#admin_clear_btn');
  const origText = btn.textContent;
  btn.textContent = 'Deleting…';
  btn.disabled = true;
  try {
    const res = await fetch('/api/admin/clear-old-searches?user=' + encodeURIComponent(currentUser) + '&pwd=' + encodeURIComponent(adminPwd), { method: 'POST' });
    const data = await res.json();
    if(data.error){
      alert('Error: ' + data.error);
    } else {
      alert('\u2713 Done! Deleted ' + (data.deleted || 0) + ' record(s) older than ' + (data.cutoff || '14 days ago') + '.');
      await loadAdminSearches();
    }
  } catch(e) {
    alert('Request failed: ' + e);
  } finally {
    btn.textContent = origText;
    btn.disabled = false;
  }
}

async function verifyPwd(){
  const cur = (currentUser||'').toLowerCase();
  const pwd = ($('#pwd_input').value||'').trim();
  if(!PWD_USERS.includes(cur) && !_canRename){
    return;
  }
  if(pwd === '500'){
    adminPwd = '500';
    $('#pwd_err').style.display='none';
    $('#pwd_modal').style.display='none';
    $('#u_badge').innerHTML=' <span style="color:#3ddc84;font-size:11px;border:1px solid #3ddc84;padding:1px 6px;margin-left:4px;border-radius:2px;cursor:pointer" title="Click to view User Searches">✓ VERIFIED</span>';
    $('#admin_clear_btn').style.display='inline-block';
    $('#app_main').style.display='none';
    $('#admin_view').style.display='block';
    await loadAdminSearches();
  } else {
    $('#pwd_err').textContent='Incorrect password! Access revoked.';
    $('#pwd_err').style.display='block';
    setTimeout(()=>{
      localStorage.removeItem('gap_user');
      currentUser='';
      adminPwd='';
      $('#pwd_modal').style.display='none';
      checkAuth();
    }, 1400);
  }
}

$('#u_name').onclick=openPwdModal;
$('#admin_rename_btn').onclick=openRenameModal;
if($('#rename_input')) $('#rename_input').addEventListener('keydown',e=>{if(e.key==='Enter')submitRename();});
$('#admin_clear_btn').onclick=clearOldSearches;
$('#u_badge').onclick=openPwdModal;
$('#pwd_submit').onclick=verifyPwd;
$('#pwd_cancel').onclick=()=>{ $('#pwd_modal').style.display='none'; };
$('#pwd_input').addEventListener('keydown',e=>{if(e.key==='Enter')verifyPwd();});
$('#admin_back_btn').onclick=()=>{
  $('#admin_view').style.display='none';
  $('#app_main').style.display='block';
  $('#t').focus();
};
$('#admin_rf_btn').onclick=loadAdminSearches;

// --- User Management Modal ---
let liveAllowed = [];

function openUsersModal(){
  const cur = (currentUser||'').toLowerCase();
  if((!PWD_USERS.includes(cur) && !_canRename) || adminPwd !== '500') return;
  loadUsersModal();
  $('#users_modal').style.display='flex';
}

function closeUsersModal(){
  $('#users_modal').style.display='none';
  $('#users_modal_err').textContent='';
}

async function loadUsersModal(){
  const cur = (currentUser||'').toLowerCase();
  try {
    const res = await fetch('/api/admin/users?user='+encodeURIComponent(currentUser)+'&pwd='+encodeURIComponent(adminPwd));
    const data = await res.json();
    if(data.error){ $('#users_list').innerHTML='<span class="r">'+esc(data.error)+'</span>'; return; }
    liveAllowed = data.users || [];
    const admins = data.admins || [];
    $('#user_details').style.display = 'none';
    let html = '<div style="display:flex;flex-wrap:wrap;gap:8px">';
    liveAllowed.forEach(u=>{
      const isAdmin = admins.includes(u);
      html += `<div style="display:flex;align-items:center;gap:6px;background:#151515;border:1px solid ${isAdmin?'#ffd23f':'#333'};border-radius:4px;padding:6px 10px 6px 12px">`;
      html += `<button onclick="viewUserDetails('${esc(u)}')" style="background:transparent;border:0;color:${isAdmin?'#ffd23f':'#ddd'};font-size:14px;font-weight:${isAdmin?'bold':'normal'};cursor:pointer;text-decoration:underline;padding:0" title="View ${esc(u)} account details">${esc(u.toUpperCase())}</button>`;
      if(isAdmin) html += `<span style="color:#ffd23f;font-size:11px;margin-left:2px">★ ADMIN</span>`;
      if(isAdmin && (u === cur || ['karthik', 'locke'].includes(u))) html += `<button onclick="openRenameModal()" style="background:#240038;border:1px solid #a855f7;color:#d8b4fe;font-size:11px;padding:2px 8px;border-radius:2px;cursor:pointer;margin-left:4px" title="Rename this username">✏️ Rename</button>`;
      if(!isAdmin) html += `<button onclick="removeUser('${esc(u)}')" style="background:transparent;border:none;color:#555;cursor:pointer;font-size:16px;line-height:1;padding:0 2px" title="Remove ${esc(u)}" onmouseover="this.style.color='#ff5a5a'" onmouseout="this.style.color='#555'">×</button>`;
      html += '</div>';
    });
    html += '</div>';
    $('#users_list').innerHTML = html;
    $('#users_modal_err').textContent = '';
  } catch(e){
    $('#users_list').innerHTML = '<span class="r">Failed: '+esc(e)+'</span>';
  }
}

window.viewUserDetails = async function(name){
  const panel = $('#user_details');
  panel.style.display = 'block';
  panel.innerHTML = '<span style="color:#888">Loading account details…</span>';
  try {
    const url = '/api/admin/users/'+encodeURIComponent(name)+'/details?user='+encodeURIComponent(currentUser)+'&pwd='+encodeURIComponent(adminPwd);
    const response = await fetch(url);
    const data = await response.json();
    if(!response.ok) throw new Error(data.error || 'Could not load user details');
    const p = data.profile || {};
    const yesNo = v => v ? 'Yes' : 'No';
    const row = (label, value) => `<div style="display:flex;gap:10px;padding:5px 0;border-bottom:1px solid #1c1c1c"><span style="color:#888;min-width:190px">${esc(label)}</span><strong style="color:#ddd;overflow-wrap:anywhere">${esc(value === undefined || value === null || value === '' ? '—' : String(value))}</strong></div>`;
    const searches = (data.recent_searches || []).map(item=>`<div style="padding:5px 0;border-bottom:1px solid #1c1c1c"><strong style="color:#6cc4ff">${esc(item.ticker || '—')}</strong><span style="color:#888;margin-left:12px">${esc([item.date_ist,item.time_ist].filter(Boolean).join(' '))}</span></div>`).join('') || '<div style="color:#777;padding:6px 0">No recent searches found.</div>';
    const visibility = data.ui_visibility || {scanner:false,runner_stats:true};
    panel.innerHTML = `<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px"><strong style="color:#6cc4ff;font-size:15px">${esc((p.username || name).toUpperCase())} · Account details</strong></div>`+
      `<div style="font-weight:bold;color:#ffd23f;margin:10px 0 4px">Tool access for this user</div>`+
      `<div style="display:flex;gap:18px;align-items:center;flex-wrap:wrap;margin:7px 0 10px"><label style="color:#ddd"><input id="user_access_runner" type="checkbox" ${visibility.runner_stats?'checked':''}> Runner Stats</label><button onclick="saveUserUiVisibility('${esc(name)}')" style="background:#071a28;border:1px solid #29a8ff;color:#6cc4ff;padding:5px 10px;cursor:pointer">Save access</button><span id="user_access_status" style="color:#888;font-size:12px"></span></div>`+
      `<div style="font-weight:bold;color:#ffd23f;margin:10px 0 4px">Account</div>`+
      row('Role', p.is_admin ? 'Admin' : p.can_rename ? 'Account manager' : 'User')+
      row('Password configured', yesNo(p.has_password))+
      row('Active session', yesNo(p.active_session))+
      row('Created', p.created_at)+row('Updated', p.updated_at)+row('Last session update', p.session_updated_at)+
      `<div style="font-weight:bold;color:#ffd23f;margin:14px 0 4px">Recent searches</div>${searches}`+
      (data.search_error ? `<div style="color:#888;font-size:11px;margin-top:7px">${esc(data.search_error)}</div>` : '');
  } catch(error){ panel.innerHTML = `<span style="color:#ff7373">${esc(error.message)}</span>`; }
};

window.saveUserUiVisibility = async function(name){
  const status = $('#user_access_status');
  status.textContent = 'Saving…';
  try{
    const response = await fetch('/api/admin/users/'+encodeURIComponent(name)+'/ui-visibility?user='+encodeURIComponent(currentUser)+'&token='+encodeURIComponent(sessionToken)+'&pwd='+encodeURIComponent(adminPwd), {
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({visibility:{scanner:false,runner_stats:$('#user_access_runner').checked}})
    });
    const data = await response.json();
    if(!response.ok) throw new Error(data.error || 'Could not save access');
    status.textContent = 'Saved for '+name;
    if(name.toLowerCase() === currentUser.toLowerCase()) applyAppUiVisibility(data.visibility);
  }catch(error){status.textContent = error.message;}
};

async function addUser(){
  const name = ($('#new_user_input').value||'').trim().toLowerCase();
  if(!name){ $('#users_modal_err').textContent='Enter a username.'; return; }
  try {
    const res = await fetch('/api/admin/users/add?user='+encodeURIComponent(currentUser)+'&pwd='+encodeURIComponent(adminPwd)+'&name='+encodeURIComponent(name), {method:'POST'});
    const data = await res.json();
    if(data.error){ $('#users_modal_err').textContent = data.error; return; }
    $('#new_user_input').value='';
    $('#users_modal_err').textContent='';
    // Update ALLOWED live in JS too
    if(!ALLOWED.includes(name)) ALLOWED.push(name);
    loadUsersModal();
  } catch(e){
    $('#users_modal_err').textContent = 'Request failed: '+e;
  }
}

window.removeUser = async function(name){
  if(!confirm('Remove "'+name.toUpperCase()+'" from authorized users?')) return;
  try {
    const res = await fetch('/api/admin/users/remove?user='+encodeURIComponent(currentUser)+'&pwd='+encodeURIComponent(adminPwd)+'&name='+encodeURIComponent(name), {method:'POST'});
    const data = await res.json();
    if(data.error){ alert('Error: '+data.error); return; }
    // Update ALLOWED live in JS
    const idx = ALLOWED.indexOf(name);
    if(idx > -1) ALLOWED.splice(idx, 1);
    loadUsersModal();
  } catch(e){
    alert('Request failed: '+e);
  }
};

$('#manage_users_btn').onclick = openUsersModal;
$('#add_user_btn').onclick = addUser;
$('#new_user_input').addEventListener('keydown', e=>{ if(e.key==='Enter') addUser(); });
function switchMainView(mode){
  const intraBtn = $('#nav_intraday_btn');
  const multiBtn = $('#nav_multiday_btn');
  const tabsView = $('#tabs_view');
  const tabBar = $('#tab_bar');
  const multiView = $('#multiday_view');
  const iframe = $('#runner_iframe');
  
  if(mode === 'multiday'){
    if(intraBtn){ intraBtn.style.background = 'transparent'; intraBtn.style.color = '#29a8ff'; }
    if(multiBtn){ multiBtn.style.background = '#00c9a7'; multiBtn.style.color = '#000'; }
    if(tabsView) tabsView.style.display = 'none';
    if(tabBar) tabBar.style.display = 'none';
    if(multiView) multiView.style.display = 'block';
    const curTicker = ($('#t').value || '').trim().toUpperCase();
    if(curTicker && iframe && (!iframe.src.includes('ticker=' + curTicker))){
      try {
        const doc = iframe.contentDocument || iframe.contentWindow.document;
        if(doc && doc.getElementById('t')){
          doc.getElementById('t').value = curTicker;
        }
      } catch(e){}
    }
  } else {
    if(intraBtn){ intraBtn.style.background = '#29a8ff'; intraBtn.style.color = '#000'; }
    if(multiBtn){ multiBtn.style.background = 'transparent'; multiBtn.style.color = '#00c9a7'; }
    if(tabsView) tabsView.style.display = 'block';
    if(tabBar && typeof openTabsOrder !== 'undefined' && openTabsOrder.length > 1) tabBar.style.display = 'flex';
    if(multiView) multiView.style.display = 'none';
  }
}
if($('#nav_intraday_btn')) $('#nav_intraday_btn').onclick = () => switchMainView('intraday');
function openRunnerStats(){
  const url = '/runner?user='+encodeURIComponent(currentUser)+'&token='+encodeURIComponent(sessionToken);
  window.open(url, '_blank');
}
if($('#nav_multiday_btn')) $('#nav_multiday_btn').onclick = openRunnerStats;
if($('#open_multiday_newtab')) $('#open_multiday_newtab').onclick = openRunnerStats;
if($('#nav_keyword_btn')) $('#nav_keyword_btn').onclick = () => window.open('/keyword-search', '_blank', 'noopener');
let scannerTimer = null;
let scannerBusy = false;
let scannerAlertSession = '';
let scannerSeenApproaches = new Set();
const scannerSettingsDefaults = {version:4,voice_enabled:false,voice_uri:'',approach_pct:2,vwap_enabled:true};
let scannerSettings = {...scannerSettingsDefaults};
let scannerSettingsUser = '';
let scannerSettingsLoadPromise = null;
let scannerSettingsSaveTimer = null;
let scannerAlarmEnabled = false;
let scannerVoices = [];
let scannerSpeechQueue = [];
let scannerSpeechActive = false;
let scannerSpeechTimer = null;

function startScannerPolling(){
  if(scannerTimer) return;
  scannerTimer = setInterval(()=>{
    const panelOpen = $('#scanner_modal').style.display !== 'none';
    if(panelOpen || scannerAlarmEnabled) refreshScanner();
  }, 30000);
  if(scannerAlarmEnabled) refreshScanner();
}

function migrateScannerSettings(settings){
  const source = settings && typeof settings === 'object' ? settings : {};
  const migrated = {...scannerSettingsDefaults, ...source};
  if(Number(source.version || 0) < 4) migrated.voice_enabled = false;
  delete migrated.bone_2m_enabled;
  delete migrated.bone_5m_enabled;
  delete migrated.voice_volume;
  delete migrated.news_enabled;
  migrated.version = 4;
  return migrated;
}

function applyScannerSettings(settings){
  scannerSettings = migrateScannerSettings(settings);
  scannerAlarmEnabled = !!scannerSettings.voice_enabled;
  $('#scanner_alarm').checked = scannerAlarmEnabled;
  $('#scanner_vwap_alert').checked = !!scannerSettings.vwap_enabled;
  const voice = scannerVoices[0];
  if(voice) scannerSettings.voice_uri = voice.voiceURI;
}

function readScannerSettingsControls(){
  return {
    version: 4,
    voice_enabled: $('#scanner_alarm').checked,
    voice_uri: scannerVoices[0]?.voiceURI || scannerSettings.voice_uri || '',
    approach_pct: scannerSettings.approach_pct || 2,
    vwap_enabled: $('#scanner_vwap_alert').checked
  };
}

async function loadScannerSettings(){
  const username = currentUser;
  if(!username || scannerSettingsUser === username) return;
  if(scannerSettingsLoadPromise) await scannerSettingsLoadPromise;
  if(scannerSettingsUser === username) return;
  const previousUser = scannerSettingsUser;
  if(previousUser && previousUser !== username){
    scannerAlertSession = ''; scannerSeenApproaches.clear();
  }
  scannerSettingsLoadPromise = (async()=>{
    let settings = {...scannerSettingsDefaults};
    let configured = false;
    try {
      const url = '/api/scanner/settings?user='+encodeURIComponent(username)+'&token='+encodeURIComponent(sessionToken);
      const response = await fetch(url);
      const data = await response.json();
      if(!response.ok) throw new Error(data.error || 'Settings could not be loaded');
      settings = {...scannerSettingsDefaults, ...(data.settings || {})};
      configured = !!data.configured;
      if(!configured){
        try {
          const saved = JSON.parse(localStorage.getItem('gap_scanner_user_settings_'+username) || 'null');
          if(saved && typeof saved === 'object') settings = {...settings, ...saved};
        } catch(e){}
      }
    } catch(e){
      try {
        const saved = JSON.parse(localStorage.getItem('gap_scanner_user_settings_'+username) || 'null');
        if(saved && typeof saved === 'object') settings = {...settings, ...saved};
      } catch(err){}
    }
    if(currentUser !== username) return;
    scannerSettingsUser = username;
    applyScannerSettings(settings);
    if(!configured) await persistScannerSettings(settings, false);
  })();
  try { await scannerSettingsLoadPromise; }
  finally { scannerSettingsLoadPromise = null; }
}

function saveScannerSettings(){
  scannerSettings = readScannerSettingsControls();
  scannerAlarmEnabled = scannerSettings.voice_enabled;
  try { localStorage.setItem('gap_scanner_user_settings_'+currentUser, JSON.stringify(scannerSettings)); } catch(e){}
  $('#scanner_settings_status').textContent = 'Saving…';
  clearTimeout(scannerSettingsSaveTimer);
  scannerSettingsSaveTimer = setTimeout(()=>persistScannerSettings(scannerSettings, true), 350);
}

async function persistScannerSettings(settings, showStatus){
  if(!currentUser || !sessionToken) return;
  try {
    const response = await fetch('/api/scanner/settings', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({user:currentUser,token:sessionToken,settings})
    });
    const data = await response.json();
    if(!response.ok) throw new Error(data.error || 'Could not save settings');
    scannerSettings = migrateScannerSettings(data.settings || settings);
    try { localStorage.setItem('gap_scanner_user_settings_'+currentUser, JSON.stringify(scannerSettings)); } catch(e){}
    if(showStatus) $('#scanner_settings_status').textContent = data.storage === 'Supabase' ? 'Saved to account' : 'Saved locally · DB migration needed for cloud sync';
  } catch(e){
    if(showStatus) $('#scanner_settings_status').textContent = 'Saved on this device · sync unavailable';
  }
}

function openScanner(){
  if(!checkAuth()) return;
  $('#scanner_modal').style.display = 'block';
  $('#scanner_alarm').checked = scannerAlarmEnabled;
  refreshScanner();
}

function closeScanner(){ $('#scanner_modal').style.display = 'none'; }
function toggleScanner(){
  if($('#scanner_modal').style.display === 'none') openScanner();
  else closeScanner();
}

document.addEventListener('mousedown', e=>{
  const panel = $('#scanner_modal');
  const button = $('#scanner_open_btn');
  if(panel.style.display !== 'none' && !panel.contains(e.target) && !button.contains(e.target)) closeScanner();
});

function announceScannerTicker(ticker, extra=''){
  if(!('speechSynthesis' in window) || typeof SpeechSynthesisUtterance === 'undefined'){
    $('#scanner_status').textContent = 'Voice alarm is unavailable in this browser.';
    return;
  }
  scannerSpeechQueue.push({ticker: ticker.toUpperCase().split('').join(' '), extra: String(extra || '').replace(/\s+/g, ' ').trim()});
  playNextScannerAnnouncement();
}

function playNextScannerAnnouncement(){
  if(scannerSpeechActive || !scannerSpeechQueue.length || !scannerAlarmEnabled) return;
  const item = scannerSpeechQueue.shift();
  scannerSpeechActive = true;
  let voiceDone = false;
  const finish = ()=>{
    if(voiceDone) return;
    voiceDone = true;
    scannerSpeechTimer = setTimeout(()=>{
      scannerSpeechTimer = null;
      scannerSpeechActive = false;
      playNextScannerAnnouncement();
    }, 2000);
  };
  const speak = (text, onDone)=>{
    const utterance = new SpeechSynthesisUtterance(text);
    let completed = false;
    const complete = ()=>{ if(completed) return; completed = true; onDone(); };
    utterance.rate = 1.0;
    utterance.pitch = 1.08;
    utterance.volume = 1;
    const feminineVoice = scannerVoices[0];
    const voice = feminineVoice;
    if(!voice){
      scannerSpeechActive = false;
      $('#scanner_status').textContent = 'No supported female voice is available in this browser.';
      onDone();
      return;
    }
    utterance.voice = voice;
    utterance.lang = voice.lang;
    utterance.onend = complete;
    utterance.onerror = complete;
    window.speechSynthesis.speak(utterance);
  };
  speak(item.ticker + '.', ()=>{
    if(!item.extra || !scannerAlarmEnabled){ finish(); return; }
    scannerSpeechTimer = setTimeout(()=>{
      scannerSpeechTimer = null;
      if(!scannerAlarmEnabled){ finish(); return; }
      speak(item.extra, finish);
    }, 1000);
  });
}

function loadScannerApproachHistory(sessionDate){
  if(scannerAlertSession === sessionDate) return;
  scannerAlertSession = sessionDate;
  try {
    const saved = JSON.parse(localStorage.getItem('gap_scanner_approaches_' + currentUser + '_' + sessionDate) || '[]');
    scannerSeenApproaches = new Set(Array.isArray(saved) ? saved : []);
  } catch(e){ scannerSeenApproaches = new Set(); }
}

function saveScannerApproach(id){
  scannerSeenApproaches.add(id);
  try {
    localStorage.setItem('gap_scanner_approaches_' + currentUser + '_' + scannerAlertSession,
                         JSON.stringify([...scannerSeenApproaches].slice(-1000)));
  } catch(e){}
}

function loadScannerVoices(){
  if(!('speechSynthesis' in window)) return;
  const savedVoice = scannerSettings.voice_uri || '';
  const availableVoices = window.speechSynthesis.getVoices();
  const femaleVoiceNames = /\b(aria|ava|amy|emma|jenny|june|karen|kimberly|moira|natasha|olivia|salli|samantha|sara|serena|sophie|susan|tessa|victoria|zira)\b|female/i;
  const femaleVoices = availableVoices.filter(voice => /^en([_-]|$)/i.test(voice.lang) && femaleVoiceNames.test(voice.name));
  const preferredVoice = femaleVoices.find(voice=>voice.voiceURI===savedVoice) ||
    femaleVoices.sort((a,b)=>a.name.localeCompare(b.name))[0];
  scannerVoices = preferredVoice ? [preferredVoice] : [];
  if(preferredVoice) scannerSettings.voice_uri = preferredVoice.voiceURI;
}

async function refreshScanner(){
  if(scannerBusy || !currentUser || !sessionToken) return;
  scannerBusy = true;
  const status = $('#scanner_status');
  try {
    const url = '/api/scanner?user='+encodeURIComponent(currentUser)+'&token='+encodeURIComponent(sessionToken);
    const res = await fetch(url);
    const data = await res.json();
    if(!res.ok) throw new Error(data.error || 'Scanner request failed');
    const tickers = data.tickers || [];
    loadScannerApproachHistory(data.session_date || new Date().toISOString().slice(0,10));
    const body = $('#scanner_rows');
    if(!tickers.length){
      body.innerHTML = '<tr><td colspan="5" style="padding:18px;color:#777;text-align:center">'+
        (data.scanning ? 'Scanning Yahoo US tickers for today’s premarket gaps…' :
         data.scan_error ? 'Yahoo premarket scan failed: '+esc(data.scan_error) : 'No tickers currently meet all scanner conditions.')+
        '</td></tr>';
    } else {
      body.innerHTML = tickers.map(row=>`<tr class="scanner-result-row" data-ticker="${esc(row.ticker)}" style="border-bottom:1px solid #20242a;cursor:pointer" title="Load notes for ${esc(row.ticker)}" onmouseover="this.style.background='#101820'" onmouseout="this.style.background='transparent'">`+
        `<td style="padding:9px;color:#6cc4ff;font-weight:bold">${esc(row.ticker)}</td>`+
        `<td style="padding:9px;text-align:right">$${Number(row.price).toFixed(2)}</td>`+
        `<td style="padding:9px;text-align:right;color:#3ddc84">+${Number(row.gap_pct).toFixed(1)}%</td>`+
        `<td style="padding:9px;text-align:right">${Number(row.volume).toLocaleString()}</td>`+
        `<td style="padding:9px;text-align:right">$${Number(row.dollar_volume).toLocaleString()}</td></tr>`).join('');
      body.querySelectorAll('[data-ticker]').forEach(row=>row.onclick=()=>selectScannerTicker(row.dataset.ticker));
    }
    const stamp = new Date((data.updated || Date.now()/1000)*1000).toLocaleTimeString();
    status.textContent = data.scanning
      ? (data.scan_phase === 'alpaca'
        ? `Checking Alpaca premarket volume · ${Number(data.scan_candidates||0).toLocaleString()} Yahoo gap candidates`
        : `Checking Yahoo premarket gaps · ${Number(data.scan_symbols_done||0).toLocaleString()} / ${Number(data.scan_universe_total||0).toLocaleString()} TC2000 tickers`)
      : `${tickers.length} matching ticker${tickers.length===1?'':'s'} · ${data.scan_error ? 'Coverage incomplete: '+esc(data.scan_error)+' · ' : ''}Updated ${stamp} · Full scan refreshes every 60 seconds`;
    if(scannerAlarmEnabled){
      tickers.forEach(row=>{
        const newEvents = (row.approaches || []).filter(event=>{
          const enabled = event.code === 'VWAP' && scannerSettings.vwap_enabled;
          const approachPct = Number(scannerSettings.approach_pct);
          const previousDistancePct = Number(event.previous_distance_pct);
          if(!enabled || Number(event.distance_pct) > approachPct ||
             !Number.isFinite(previousDistancePct) || previousDistancePct <= approachPct) return false;
          const id = `${row.ticker}:${event.code}`;
          if(scannerSeenApproaches.has(id)) return false;
          saveScannerApproach(id);
          return true;
        });
        if(newEvents.length) announceScannerTicker(row.ticker, newEvents.map(event=>event.label).join('. '));
      });
    }
  } catch(e){
    status.textContent = 'Scanner error: ' + e.message;
  } finally {
    scannerBusy = false;
  }
}

function selectScannerTicker(ticker){
  if(!/^[A-Z0-9.\\-]{1,10}$/.test(ticker || '')) return;
  $('#t').value = ticker;
  closeScanner();
  run(false);
}

if($('#scanner_open_btn')) $('#scanner_open_btn').onclick = toggleScanner;
if($('#scanner_close_btn')) $('#scanner_close_btn').onclick = closeScanner;
if($('#scanner_refresh_btn')) $('#scanner_refresh_btn').onclick = refreshScanner;
if($('#scanner_alarm')) $('#scanner_alarm').onchange = e=>{
  scannerAlarmEnabled = e.target.checked;
  saveScannerSettings();
  if(scannerAlarmEnabled){
    if('speechSynthesis' in window) window.speechSynthesis.resume();
    refreshScanner();
  } else if('speechSynthesis' in window){
    scannerSpeechQueue = [];
    scannerSpeechActive = false;
    if(scannerSpeechTimer){ clearTimeout(scannerSpeechTimer); scannerSpeechTimer = null; }
    window.speechSynthesis.cancel();
  }
};
if($('#scanner_vwap_alert')) $('#scanner_vwap_alert').onchange = saveScannerSettings;
loadScannerVoices();
if('speechSynthesis' in window) window.speechSynthesis.onvoiceschanged = loadScannerVoices;
if($('#multiday_stats_btn')) $('#multiday_stats_btn').onclick = () => {
  $('#admin_view').style.display = 'none';
  $('#app_main').style.display = 'block';
  switchMainView('multiday');
};
$('#admin_view_mode').onchange=(e)=>{
  activeViewMode = e.target.value;
  renderAdminView();
};

window.addEventListener('keydown', e => {
  if(e.key === 'Escape'){
    closeChartModal();
    closeUserPopup();
    closeScanner();
    if($('#pwd_modal').style.display === 'flex'){
      $('#pwd_modal').style.display = 'none';
    }
  }});

initAuth();
</script></body></html>"""


KEYWORD_HTML = r"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Search Keyword · EDGAR Financing Research</title><link rel="icon" type="image/svg+xml" href="/favicon.svg?app=keyword">
<style>
:root{color-scheme:dark;--bg:#050607;--panel:#0c0e11;--line:#252a30;--muted:#8e98a3;--blue:#60baff;--green:#47d890;--red:#ff626c;--yellow:#ffd66b}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:#e6e9ed;font:13px/1.45 Consolas,"SF Mono",monospace}
header{position:sticky;top:0;z-index:5;background:#080a0d;border-bottom:1px solid var(--line);padding:10px 18px;display:flex;align-items:center;gap:14px}
header a{color:var(--blue);text-decoration:none}header strong{color:#b7a8ff}main{padding:16px 18px 60px;max-width:1800px;margin:auto}
.toolbar{background:#0b0d10;border:1px solid var(--line);padding:12px;display:flex;align-items:end;gap:9px;flex-wrap:wrap}.gapFilter{display:flex;align-items:center;gap:6px;height:32px;flex-direction:row}.gapFilter span{display:flex;align-items:center;gap:5px;white-space:nowrap}.gapFilter input[type=checkbox]{width:14px;height:14px;padding:0;margin:0}.gapFilter input[type=number]{width:62px;height:26px;padding:3px 5px}.gapFilter input:disabled{opacity:.5}
label{display:flex;flex-direction:column;gap:4px;color:#aeb6c0;font-size:11px}input,select,button{background:#080a0d;color:#eee;border:1px solid #343a42;border-radius:2px;font:inherit;height:32px;padding:4px 8px}
input[type=text]{width:min(420px,70vw)}input[type=date]{width:140px}select{min-width:125px}select[multiple]{height:74px}button{cursor:pointer;font-weight:bold}button:hover{border-color:var(--blue);color:var(--blue)}button.primary{background:#102033;border-color:#3285c4;color:#b5ddff}.small{height:26px;font-size:11px;padding:2px 7px}
.filters{margin-top:8px;display:flex;gap:9px;align-items:end;flex-wrap:wrap}.status{padding:9px 2px;color:var(--muted);min-height:36px}.error{color:var(--red)}
.tablewrap{max-height:52vh;overflow:auto;border:1px solid var(--line);background:#08090b}.tablewrap table{border-collapse:collapse;width:100%;min-width:850px;font-size:12px}th{position:sticky;top:0;background:#15181d;color:#d6dbe1;text-align:left;padding:7px 8px;border-bottom:1px solid #3a414a;cursor:pointer;white-space:nowrap}td{padding:6px 8px;border-bottom:1px solid #20242a;vertical-align:top}tbody tr:nth-child(even){background:#111317}tbody tr:hover{background:#171d25}.ticker{font-weight:bold;color:var(--blue);text-decoration:none}.deal a{color:#7cc9ff}.sub{display:block;color:#828b95;font-size:10px;margin-top:2px}.excerpt{max-width:420px;color:#adb6c0;font-size:11px}details summary{color:#8fcfff;cursor:pointer}.count{color:var(--green);font-weight:bold}
.stock{border-top:1px solid #39414b;margin-top:18px;padding-top:10px}.stockhead{position:sticky;top:51px;z-index:2;background:#0a0c0f;border-left:3px solid #3285c4;padding:9px 10px;margin-bottom:8px}.stockhead h2{font-size:16px;margin:0;color:#d8e8f5}.evidence{font-size:11px;color:#adb5be;margin-top:5px}.evidence a{color:#69baff}.divider{border:0;border-top:1px solid #292f36;margin:8px 0}
.tickerIndex{display:flex;gap:6px;flex-wrap:wrap;padding:6px 0 12px}.tickerIndex a{border:1px solid #303943;background:#0d1116;color:#7dc7ff;padding:5px 9px;text-decoration:none}.tickerIndex a:hover{border-color:#29a8ff}.evgrid{display:grid;grid-template-columns:1fr 1fr;gap:14px}.evc{border:1px solid #222;padding:8px 10px;min-width:0;position:relative;cursor:pointer;transition:border-color .15s,background .15s}.evc:hover{border-color:#29a8ff;background:#0d0d0d}.evhead{display:flex;align-items:center;justify-content:space-between;gap:6px;min-height:24px;margin-bottom:2px}.evhead .evt{font-size:13px;margin:0;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.evcontrols{display:flex;align-items:center;gap:3px;flex:none}.evnav{width:22px;height:21px;padding:0;font-size:15px;line-height:18px;border:1px solid #333;color:#aaa;background:#101010}.evnav:hover{color:#6cc4ff;border-color:#29a8ff}.evtf{height:21px;margin:0 0 0 2px;padding:0 3px;font-size:11px;border-color:#333;color:#aaa;background:#101010}.evzoom{width:22px;height:21px;padding:0;font-size:14px;color:#aaa;background:#101010;border:1px solid #333}.evmeasure,.evtoday{height:21px;padding:0 6px;font-size:10px;color:#9fcfff;background:#10151b;border:1px solid #30485b;cursor:pointer}.evmeasure.active{color:#071018;background:#62c8ff;border-color:#62c8ff}.evc canvas{display:block;width:100%;background:#050607;cursor:grab}.kwChartTools{display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap;border:1px solid #272d34;padding:7px 10px;margin:8px 0}.kwChartTools .kwToolGroup{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.kwChartTools select{height:25px;min-width:90px;font-size:11px}.kwBoneLabel{display:flex;align-items:center;gap:5px;color:#aaa;font-size:11px}.kwBoneLabel input{width:14px;height:14px;margin:0}.kwLegend{padding:8px 10px;color:#aaa;font-size:11px;border-top:1px solid #222;margin-top:8px}.kwModalTitle{color:#29a8ff;font-size:16px;font-weight:bold;margin-right:auto}.kwModalControls{display:flex;align-items:center;gap:6px;flex-wrap:wrap}.kwModalControls .kwBoneLabel{color:#ddd}.kwClose{height:30px;width:32px;padding:0;font-size:18px}.kwModalBody{flex:1;padding:8px 10px;position:relative;display:flex;align-items:center;justify-content:center;overflow:hidden;background:#050505}.kwModalLegend .chartHoverTip{display:inline-block;margin-left:12px;color:#e7eef5;font:11px Consolas,monospace}.kwModalBody canvas{display:block;width:100%;height:100%}@media(max-width:1100px){.evgrid{grid-template-columns:1fr}}.eventGrid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:9px}.event{border:1px solid #272d34;background:#090b0e;padding:5px;min-width:0}.eventhead{display:flex;align-items:center;gap:4px;padding:1px 0 5px}.eventtitle{font-weight:bold;color:#d7e7f2;margin-right:auto;white-space:nowrap}.event select{height:25px;min-width:58px;font-size:11px;padding:2px 5px}.event button{height:25px;font-size:11px;padding:2px 7px}.event canvas{display:block;background:#050607;cursor:grab}.chartModal{position:fixed;inset:0;z-index:20;background:rgba(0,0,0,.94);display:none;align-items:center;justify-content:center;padding:14px}.chartModal.open{display:flex}.chartModalBox{width:98vw;height:95vh;background:#080a0d;border:1px solid #343a42;display:flex;flex-direction:column}.chartModalHead{display:flex;align-items:center;gap:8px;padding:9px 12px;background:#11151a;border-bottom:1px solid #252a30}.chartModalTitle{color:#60baff;font-weight:bold;margin-right:auto}.chartModalCanvas{width:100%;height:calc(95vh - 54px);background:#050607}#comparePanel{margin-top:28px;border-top:1px solid #39414b;padding-top:14px;scroll-margin-top:58px}#comparePanel h2{margin:0 0 8px;color:#8fd0ff}.compareToolbar{display:flex;gap:9px;align-items:end;flex-wrap:wrap;padding:12px;background:#0b0d10;border:1px solid var(--line)}.compareToolbar input[type=text]{width:125px}.compareToolbar input[type=date]{width:145px}.compareToolbar select{min-width:85px}.compareMatch{border:1px solid #303841;background:#0a0c0f;margin:13px 0;padding:10px}.compareSummary{color:#b4c0ca;margin:8px 0}.scoreline{display:flex;gap:12px;flex-wrap:wrap;margin:8px 0;color:#aab4be}.scorebar{width:105px;height:7px;background:#242a31;display:inline-block;vertical-align:middle;margin-left:4px}.scorebar i{display:block;height:100%;background:#53b9ff}.compareCharts{display:grid;grid-template-columns:1fr 1fr;gap:10px}.compareOverlay{width:100%;height:220px;background:#050607;display:block}.compareStats{border-collapse:collapse;width:100%;margin:8px 0}.compareStats td,.compareStats th{border:1px solid #242a31;padding:5px;text-align:left}.compareStats th{color:#9fcfff}.compareNote{color:#909aa4;font-size:11px;margin:7px 0}.compareFiling{color:#75c6ff}@media(max-width:760px){.eventGrid,.compareCharts{grid-template-columns:1fr}}
#comparePanel .unifiedProjection{display:grid;grid-template-columns:minmax(0,2.2fr) minmax(280px,1fr);gap:12px;margin:16px 0;padding:10px;border:1px solid #34404b;background:#090b0e}#comparePanel .unifiedChartWrap{min-width:0;position:relative}#comparePanel .unifiedControls{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin:5px 0}#comparePanel .unifiedControls select,#comparePanel .unifiedControls button{height:26px;font-size:11px}#comparePanel .unifiedCanvas{width:100%;height:440px;display:block;background:#050607}#comparePanel .unifiedLegend{display:flex;flex-wrap:wrap;gap:6px;margin:6px 0}#comparePanel .unifiedLegend label{display:inline-flex;flex-direction:row;align-items:center;border:1px solid #252c34;padding:3px 6px}#comparePanel .projectionPanel{border-left:1px solid #292f36;padding:4px 10px}#comparePanel .projectionPanel h3{margin:2px 0 8px;color:#9fd3ff}#comparePanel .projectionGrid{display:grid;grid-template-columns:1fr auto;gap:5px 10px}#comparePanel .projectionGrid span{border-bottom:1px solid #20262d;padding:4px 0}#comparePanel .lowConfidence{display:inline-block;color:#ffd66b;border:1px solid #8c7026;padding:3px 6px;margin:4px 0;font-weight:bold}#comparePanel .confidenceGood{color:#47d890}#comparePanel .chartTip{position:absolute;display:none;pointer-events:none;background:#111820;border:1px solid #4c5a67;padding:6px;white-space:pre;z-index:2;font-size:10px}@media(max-width:900px){#comparePanel .unifiedProjection{grid-template-columns:1fr}#comparePanel .projectionPanel{border-left:0;border-top:1px solid #292f36;padding-top:10px}}
.compareMatch .compareCharts{display:none}.compareEventGrid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin:12px 0}.compareEventCard{cursor:pointer}.compareEventCard .evhead{align-items:flex-start}.compareEventCard .evtags{gap:4px}.compareEventCard canvas{height:390px}.compareEventCard .evtag{font-size:10px}.qualifiedMatchList{display:grid;gap:5px;margin-top:10px}.qualifiedMatch{display:grid;grid-template-columns:minmax(170px,1fr) repeat(3,minmax(110px,auto));gap:8px;align-items:center;background:#0a0c0f;border:1px solid #252c34;padding:8px 10px;color:#adb6c0;font-size:11px}.qualifiedMatch b{font-size:12px}@media(max-width:760px){.qualifiedMatch{grid-template-columns:1fr 1fr}.compareEventGrid{grid-template-columns:1fr}}
@media(max-width:720px){main{padding:10px}.stockhead{top:45px}.toolbar{align-items:stretch}input[type=text]{width:100%}}
.kwEventHeading{display:flex;align-items:center;gap:7px;flex-wrap:wrap;min-width:0;flex:1}.evhead .evt{flex:0 1 auto}.evtags{display:flex;align-items:center;gap:4px;flex-wrap:wrap}.evtag{display:inline-flex;align-items:center;border:1px solid #303b46;background:#10151b;color:#b9c7d5;border-radius:3px;padding:1px 5px;font-size:10px;line-height:16px;white-space:nowrap}.evtag.gap{color:#8fd0ff;border-color:#245474}.evtag.dvol{color:#75dfa9;border-color:#286447}.evtag.rule{color:#ffd66b;border-color:#786128}.kwModalTags{display:flex;gap:5px;align-items:center;margin-right:auto}.kwModalTags .evtag{font-size:11px;padding:3px 7px}.kwModalTitle{margin-right:0}
</style></head><body>
<header><a href="/">← Gap-Up Fade</a><strong>⌕ Search Keyword</strong><a href="#comparePanel">Jump to Compare</a><span style="margin-left:auto;color:#78838e">SEC EDGAR · financing-player research</span></header>
<main>
<form id="searchForm" class="toolbar">
  <label>Keyword<input id="keyword" type="text" maxlength="120" placeholder="Company, investor, or terms (comma = all required)" required></label>
  <label>Start date<input id="startDate" type="date"></label><label>End date<input id="endDate" type="date"></label>
  <label class="gapFilter"><span><input id="gapFilterEnabled" type="checkbox"> Require open gap-up</span><span>&gt; <input id="gapMinPct" type="number" min="0" max="500" step="1" value="10" disabled>%</span></label>
  <button class="primary" type="submit">Search SEC</button><button id="clearBtn" type="button">Clear</button>
  <button id="moreBtn" type="button" style="display:none">Load more</button>
</form>
 <div id="status" class="status">Search SEC filings from the last year. Use commas to require every term in the same filing, such as MAXIM, BOUTIQUE. Events require session dollar volume above $50M and an event-day open of $10 or less; the gap-up filter is optional.</div>
<div id="resultSummary"></div><div id="tableRoot"></div><div id="stocksRoot"></div>
 <section id="comparePanel"><h2>Compare historical event patterns</h2><div id="compareCriteria" class="compareNote">Compare your ticker with matching-keyword events whose event-day open was $10 or less and session dollar volume was above $50M.</div><form id="compareForm" class="compareToolbar"><label>Target ticker<input id="compareTicker" type="text" maxlength="10" placeholder="TICKER" required></label><label>Target date<input id="compareDate" type="date" required></label><button class="primary" type="submit">Compare</button></form><div id="compareStatus" class="status">Search for a keyword to populate historical candidate events.</div><div id="unifiedProjectionRoot"></div><div id="forecast2mRoot"></div><div id="compareResults"></div></section>
</main>
<div id="chartModal" class="chartModal" onclick="if(event.target===this)closeKWModal()"><div class="chartModalBox"><div class="chartModalHead"><div class="kwModalTitle" id="kwModalTitle"></div><div class="kwModalTags" id="kwModalTags"></div><div class="kwModalControls"><select id="kwModalTf" class="evtf"><option>1m</option><option selected>2m</option><option>5m</option><option>15m</option><option>30m</option><option value="1d">Day</option></select><select id="kwModalDays" class="evtf"><option value="1" selected>1 day</option><option value="3">3 days</option><option value="5">5 days</option><option value="10">10 days</option><option value="20">20 days</option><option value="60">60 days</option></select><button id="kwZoomOut" class="evnav" title="Zoom out">−</button><button id="kwZoomIn" class="evnav" title="Zoom in">+</button><select id="kwModalSig" class="evtf"><option value="none">No signals</option><option value="all">All entry models</option></select><label class="kwBoneLabel"><input id="kwModalBone" type="checkbox"> Bone zone</label><button onclick="closeKWModal()" class="kwClose" title="Close">✕</button></div></div><div class="kwModalBody"><canvas id="kwModalCanvas" height="720"></canvas></div><div class="kwModalLegend kwLegend"><span class="c">━ VWAP</span> &nbsp; <span class="y">┅ PM high</span> &nbsp; <span class="d">┅ prev close</span> &nbsp; <span style="color:#d7ff6e">━ EMA9</span> &nbsp; <span style="color:#ffa500">━ EMA20</span> &nbsp; <span class="g">▒</span><span class="r">▒</span> cloud &nbsp; <span class="d">━ BB(20,2)</span> &nbsp; Shaded = premarket · Drag to pan · drag or scroll price axis to scale · Esc to close</div></div></div>
<script>
const $=s=>document.querySelector(s), esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const user=localStorage.getItem('gap_user')||'', token=localStorage.getItem('gap_token')||'';
const RG_START=9*60+30;
let rows=[],offset=0,total=0,observer=null,chartObserver=null,searchGeneration=0,eventScreenGeneration=0,kwModalSource=null,kwModalSeries=[],kwModalIndex=0;
const now=new Date(), isoDate=d=>`${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
$('#endDate').value=isoDate(now);const oneYear=new Date(now);oneYear.setFullYear(oneYear.getFullYear()-1);$('#startDate').value=isoDate(oneYear);updateCriteriaLabels();$('#gapFilterEnabled').onchange=()=>{$('#gapMinPct').disabled=!$('#gapFilterEnabled').checked;updateCriteriaLabels();if(rows.length)search(true)};$('#gapMinPct').onchange=()=>{updateCriteriaLabels();if($('#gapFilterEnabled').checked&&rows.length)search(true)};
async function verify(){if(!user||!token){$('#status').innerHTML='<span class="error">Please sign in to the main app first, then reopen this page.</span>';return false}try{const r=await fetch('/api/auth/verify-session?user='+encodeURIComponent(user)+'&token='+encodeURIComponent(token));if(!r.ok)throw 0;return true}catch{$('#status').innerHTML='<span class="error">Your session is not valid. Return to the main app and sign in again.</span>';return false}}
function payload(){return {user,token,keyword:$('#keyword').value.trim(),start_date:$('#startDate').value,end_date:$('#endDate').value,offset}}
function activeEventCriteria(){const enabled=$('#gapFilterEnabled').checked,threshold=Number($('#gapMinPct').value),criteria='session dollar volume >$50M and event-day open ≤$10';return enabled?`open gap-up >${threshold}% and ${criteria}`:criteria}
function updateCriteriaLabels(){const label=$('#compareCriteria');if(label)label.textContent=`Compare your ticker with matching-keyword events meeting ${activeEventCriteria()}.`}
function fmtDate(d){const [y,m,day]=String(d||'').slice(0,10).split('-');return y&&m&&day?`${m}/${day}/${y}`:d}
function setStatus(t,err=false){$('#status').innerHTML=err?`<span class="error">${esc(t)}</span>`:esc(t)}
function buildTable(){
 const companies=groupCompanies().filter(c=>c.ticker);
 $('#resultSummary').innerHTML=rows.length?`<div class="status">Screening <span class="count">${companies.length}</span> filing-linked tickers for qualifying events…</div>`:'';
 if(!rows.length){$('#tableRoot').innerHTML='';$('#stocksRoot').innerHTML='';return}
 $('#tableRoot').innerHTML='';renderStocks(companies);
}
function groupCompanies(){const map=new Map();for(const r of rows){const key=r.ticker||r.cik;if(!map.has(key))map.set(key,{key,ticker:r.ticker,company:r.company,filings:[]});map.get(key).filings.push(r)}return [...map.values()]}
async function renderStocks(companies){
 const scanId=++eventScreenGeneration;if(observer)observer.disconnect();qualifiedTickers=new Set();kwEventCache.clear();$('#stocksRoot').innerHTML='';$('#tableRoot').innerHTML='<div class="tickerIndex"></div>';let scanned=0,shown=new Set();
 observer=new IntersectionObserver(entries=>entries.forEach(entry=>{if(entry.isIntersecting){observer.unobserve(entry.target);loadStockCharts(entry.target)}}),{rootMargin:'350px'});
 const appendQualified=()=>{for(const c of companies){if(!qualifiedTickers.has(c.ticker)||shown.has(c.ticker))continue;shown.add(c.ticker);const i=shown.size-1;$('#tableRoot .tickerIndex').insertAdjacentHTML('beforeend',`<a href="#stock-${i}">${esc(c.ticker)}</a>`);const holder=document.createElement('div');holder.innerHTML=`<section class="stock" id="stock-${i}" data-stock="${esc(c.key)}" data-ticker="${esc(c.ticker)}"><div class="stockhead"><h2><a class="ticker" href="https://finance.yahoo.com/quote/${encodeURIComponent(c.ticker)}" target="_blank" rel="noopener">${esc(c.ticker)}</a></h2><div class="evidence">${c.filings.length} keyword-matched filing${c.filings.length===1?'':'s'} · charts load as you scroll</div></div><div class="stockCharts"></div></section>`;const section=holder.firstElementChild;section._filings=c.filings;section._kwEvents=kwEventCache.get(c.ticker+'|meta')||[];$('#stocksRoot').appendChild(section);observer.observe(section)}};
 for(let i=0;i<companies.length;i+=3){const batch=companies.slice(i,i+3);const results=await Promise.allSettled(batch.map(c=>fetchKeywordEvents(c.ticker,true)));
  if(scanId!==eventScreenGeneration)return;
  results.forEach((result,j)=>{scanned++;if(result.status==='fulfilled'&&result.value.length)qualifiedTickers.add(batch[j].ticker)});appendQualified();
  $('#resultSummary').innerHTML=`<div class="status">Screened ${scanned}/${companies.length} tickers · <span class="count">${qualifiedTickers.size} qualify</span> · ${esc(activeEventCriteria())}.</div>`;
 }
 if(!shown.size)$('#stocksRoot').innerHTML='<div class="status">No filing-linked tickers had a qualifying event in this date range.</div>';
}
function fmtPct(v){return v==null?'—':`${v>=0?'+':''}${(v*100).toFixed(1)}%`}function fmtNum(v){if(v==null)return'—';if(v>=1e9)return (v/1e9).toFixed(2)+'B';if(v>=1e6)return (v/1e6).toFixed(2)+'M';if(v>=1e3)return (v/1e3).toFixed(1)+'K';return Math.round(v).toLocaleString()}
async function loadStockCharts(section){
 const ticker=section.dataset.ticker,root=section.querySelector('.stockCharts');if(!ticker){root.innerHTML='<div class="noevents">No ticker mapping is available for this SEC filer, so historical candles cannot be requested.</div>';return}
 root.innerHTML='<div class="status">Loading qualifying chart days…</div>';
 try{const events=await fetchKeywordEvents(ticker,false);renderEvents(root,{ticker,events},section._filings||[])}catch(error){root.innerHTML=`<div class="noevents">${esc(error.message)}</div>`}
}
function renderEvents(root,data,filings=[]){
 const events=data.events||[],signalNames=[...new Set(events.flatMap(e=>(e.signal_models||[]).map(s=>s.name)))];
 root.innerHTML=`<div class="status">${events.length} qualifying event days · ${esc(activeEventCriteria())}</div>`+
 `<div class="kwChartTools"><div class="kwToolGroup"><span>INTRADAY CHARTS — premarket → close, volume + VWAP</span><select class="kwSig" title="Entry signals"><option value="none">No signals</option><option value="all">All entry models</option>${signalNames.map(n=>`<option value="${esc(n)}">${esc(n)}</option>`).join('')}</select><label class="kwBoneLabel"><input class="kwBone" type="checkbox"> Bone zone (9/20 EMA cloud + BB)</label></div></div>`+
 `<div class="evgrid">`+events.map((e,i)=>`<article class="evc" data-event="${i}"><div class="evhead"><div class="kwEventHeading"><div class="evt">${esc(data.ticker)} · ${esc(fmtDate(e.date))}</div><div class="evtags"><span class="evtag eventDate" title="Original qualifying event day">Event ${esc(fmtDate(e.date))}</span><span class="evtag gap" data-gap-tag>Gap ${fmtPct(e.gap_pct)}</span><span class="evtag dvol" data-dvol-tag>Vol $${fmtNum(e.dollar_volume)}</span><span class="evtag rule" title="Why this day qualifies">${esc(e.qualifier||'Qualifying event')}</span></div></div><div class="evcontrols"><select class="evtf" title="Timeframe"><option>1m</option><option selected>2m</option><option>5m</option><option>15m</option><option>30m</option><option value="1d">Day</option></select><select class="evdays" title="Sessions shown"><option value="1" selected>1 day</option><option value="3">3 days</option><option value="5">5 days</option><option value="10">10 days</option><option value="20">20 days</option><option value="60">60 days</option></select><button class="evzoom" data-zoom="out" title="Zoom out">−</button><button class="evzoom" data-zoom="in" title="Zoom in">+</button></div></div>${e.error?`<div class="noevents">${esc(e.error)}</div>`:`<canvas class="priceChart" id="kw_ev${i}" aria-label="${esc(data.ticker)} ${esc(fmtDate(e.date))} chart"></canvas>`}</article>`).join('')+`</div>`+
 `<div class="kwLegend"><span class="c">━ VWAP</span> &nbsp; <span class="y">┅ PM high</span> &nbsp; <span class="d">┅ prev close</span> &nbsp; <span style="color:#d7ff6e">━ EMA9</span> &nbsp; <span style="color:#ffa500">━ EMA20</span> &nbsp; <span class="g">▒</span><span class="r">▒</span> cloud &nbsp; <span class="d">━ BB(20,2)</span> · Shaded = premarket. Times in IST.</div>`;
 if(!chartObserver)chartObserver=new IntersectionObserver(entries=>entries.forEach(entry=>{if(entry.isIntersecting){const cv=entry.target;chartObserver.unobserve(cv);bindKWChart(cv);loadKWContext(cv,cv._event.date,cv._chartDays)}}),{rootMargin:'450px 0px'});
 if(filings.length)root.insertAdjacentHTML('beforeend',`<details class="qualFilings"><summary>Related SEC filings (${filings.length})</summary>${filings.map(f=>`<div class="evidence"><a href="${esc(f.filing_url)}" target="_blank" rel="noopener">${esc(f.headline)}</a> · ${esc(f.form)} · ${esc(fmtDate(f.filing_date))}</div>`).join('')}</details>`);
 const redrawAll=()=>root.querySelectorAll('.priceChart').forEach(drawKWChart);
 root.querySelector('.kwSig').onchange=()=>{redrawAll();if(kwModalSource&&root.contains(kwModalSource))renderKWModal()};root.querySelector('.kwBone').onchange=()=>{redrawAll();if(kwModalSource&&root.contains(kwModalSource))renderKWModal()};
 events.forEach((e,i)=>{const card=root.querySelector(`[data-event="${i}"]`);if(e.error)return;const cv=card.querySelector('.priceChart');
   cv._chart=e.chart||{bars:{t:[],d:[],o:[],h:[],l:[],c:[],v:[],vw:[]},date:e.date,pc:e.prev_close,pmh:e.premarket_high,off:e.chart_off||0,stop:e.stop,cover:e.cover,sig:e.signal_models||[]};
   cv._event={...e,ticker:data.ticker};cv._timeframe='2m';cv._chartDays=1;cv._activeDate=e.date;cv._zoom=1;cv._panOffset=0;cv._onPanChange=value=>card._panOffset=value;cv._onPanEdge=side=>{if(cv._contextLoading)return;const dates=cv._contextDates||[];const edge=dates[side>0?dates.length-1:0];if(!edge)return;cv._chartDays=[1,3,5,10,20,60].find(value=>value>cv._chartDays)||60;cv._panOffset=card._panOffset=0;const daysSelect=card.querySelector('.evdays');if(daysSelect)daysSelect.value=String(cv._chartDays);loadKWContext(cv,edge,cv._chartDays)};chartObserver.observe(cv);
   card.onclick=ev=>{if(ev.target.closest('.evcontrols'))return;openKWModal(cv)};
   card.querySelectorAll('.evcontrols').forEach(el=>el.onclick=ev=>ev.stopPropagation());
   const tf=card.querySelector('.evtf');tf.onchange=ev=>{ev.stopPropagation();cv._timeframe=tf.value;cv._panOffset=card._panOffset=0;cv._yZoom=1;cv._chartDays=tf.value==='1d'?60:1;card.querySelector('.evdays').value=String(cv._chartDays);loadKWContext(cv,cv._activeDate||e.date,cv._chartDays)};
   const days=card.querySelector('.evdays');days.onchange=ev=>{ev.stopPropagation();cv._chartDays=Number(days.value);cv._panOffset=card._panOffset=0;loadKWContext(cv,cv._activeDate||e.date,cv._chartDays)};
   card.querySelectorAll('.evzoom').forEach(btn=>btn.onclick=ev=>{ev.stopPropagation();cv._zoom=Math.max(.5,Math.min(8,(cv._zoom||1)*(btn.dataset.zoom==='in'?1.25:.8)));drawKWChart(cv)});
 });
}
async function loadKWContext(cv,anchor,days){if(!cv?._chart)return;cv._activeDate=anchor;cv._contextLoading=true;const requestId=(cv._contextRequestId||0)+1;cv._contextRequestId=requestId;const q=new URLSearchParams({ticker:cv._event.ticker,date:anchor,days:String(days||1),tf:(cv._timeframe||'2m').toLowerCase()==='1d'?'1d':(cv._timeframe||'2m').toLowerCase(),user,token});try{const r=await fetch('/api/chart-context?'+q),data=await r.json();if(requestId!==cv._contextRequestId)return;if(!r.ok)throw Error(data.error||'Chart data unavailable');cv._contextBars=data.bars;cv._contextDates=data.dates;cv._contextDaily=!!data.daily;cv._contextMetrics=data.metrics||{};if(cv._chart)cv._chart.bars=data.bars;cv._contextError='';}catch(e){if(requestId!==cv._contextRequestId)return;cv._contextBars=null;cv._contextError=e.message;}cv._contextLoading=false;const card=cv.closest('.evc,.event');if(card){const title=card.querySelector('.evt,.eventtitle');if(title)title.textContent=card.classList.contains('compareEventCard')?`${cv._titlePrefix} · ${cv._event.ticker} · ${fmtDate(anchor)}`:`${cv._event.ticker} · ${fmtDate(anchor)}`;const metrics=cv._contextMetrics?.[anchor];if(metrics){const gap=card.querySelector('[data-gap-tag]'),dvol=card.querySelector('[data-dvol-tag]');if(gap)gap.textContent=`Gap ${fmtPct(metrics.gap_pct)}`;if(dvol)dvol.textContent=`Vol $${fmtNum(metrics.dollar_volume)}`}if(cv._contextError){let msg=card.querySelector('.chartError');if(!msg){msg=document.createElement('div');msg.className='noevents chartError';card.appendChild(msg)}msg.textContent=cv._contextError}else card.querySelector('.chartError')?.remove()}drawKWChart(cv);if(kwModalSource===cv)renderKWModal()}
function openKWModal(cv){kwModalSource=cv;const group=cv.closest('.stock,.compareMatch,.compareEventCard')||document;kwModalSeries=[...group.querySelectorAll('.priceChart')].filter(c=>c._chart);kwModalIndex=Math.max(0,kwModalSeries.indexOf(cv));$('#chartModal').classList.add('open');requestAnimationFrame(renderKWModal)}
function renderKWModal(){const src=kwModalSeries[kwModalIndex];if(!src)return;const cv=$('#kwModalCanvas');cv._chart=src._chart;cv._event=src._event;cv._timeframe=src._timeframe||'2m';cv._contextBars=src._contextBars;cv._contextDates=src._contextDates;cv._contextMetrics=src._contextMetrics;cv._panOffset=src._panOffset||0;cv._yZoom=src._yZoom||1;cv._onPanChange=value=>{src._panOffset=value;src._onPanChange?.(value)};cv._onYZoomChange=value=>{src._yZoom=value};cv._onPanEdge=side=>src._onPanEdge?.(side);cv._activeDate=src._activeDate;cv._chartDays=src._chartDays;cv._zoom=src._zoom||1;$('#kwModalTitle').textContent=src._event.ticker+' · '+fmtDate(src._activeDate||src._event.date);const activeMetrics=src._contextMetrics?.[src._activeDate||src._event.date],gapPct=activeMetrics?.gap_pct??src._event.gap_pct,dvol=activeMetrics?.dollar_volume??src._event.dollar_volume;$('#kwModalTags').innerHTML=`<span class="evtag eventDate">Event ${fmtDate(src._event.date)}</span><span class="evtag gap">Gap ${fmtPct(gapPct)}</span><span class="evtag dvol">Vol $${fmtNum(dvol)}</span>`;$('#kwModalTf').value=cv._timeframe.toLowerCase()==='1d'?'1d':cv._timeframe;$('#kwModalDays').value=String(cv._chartDays||1);const card=src.closest('.evc,.event'),sig=card?.closest('.stock')?.querySelector('.kwSig'),boneCheck=card?.closest('.stock')?.querySelector('.kwBone');$('#kwModalSig').innerHTML=sig?.innerHTML||'<option value="none">No signals</option><option value="all">All entry models</option>';$('#kwModalSig').value=sig?.value||'none';$('#kwModalBone').checked=!!boneCheck?.checked;cv.setAttribute('height',String(Math.max(350,window.innerHeight-110)));drawKWChart(cv)}
function closeKWModal(){$('#chartModal').classList.remove('open');kwModalSource=null}
$('#kwModalTf').onchange=()=>{const src=kwModalSeries[kwModalIndex];if(!src)return;src._timeframe=$('#kwModalTf').value;src._chartDays=src._timeframe==='1d'?60:1;const card=src.closest('.evc,.event'),tf=card?.querySelector('.evtf,.eventTf'),days=card?.querySelector('.evdays,.eventDays');if(tf)tf.value=src._timeframe;if(days)days.value=String(src._chartDays);src._panOffset=0;src._yZoom=1;if((src._timeframe==='1d')!==!!src._contextDaily||!src._contextBars)loadKWContext(src,src._activeDate||src._event.date,src._chartDays||5);else{drawKWChart(src);renderKWModal()}};
$('#kwModalDays').onchange=()=>{const src=kwModalSeries[kwModalIndex];if(!src)return;src._chartDays=Number($('#kwModalDays').value);const card=src.closest('.evc,.event'),days=card?.querySelector('.evdays,.eventDays');if(days)days.value=String(src._chartDays);loadKWContext(src,src._activeDate||src._event.date,src._chartDays)};
$('#kwZoomOut').onclick=()=>{const src=kwModalSeries[kwModalIndex];if(src){src._zoom=Math.max(.5,(src._zoom||1)*.8);renderKWModal()}};$('#kwZoomIn').onclick=()=>{const src=kwModalSeries[kwModalIndex];if(src){src._zoom=Math.min(8,(src._zoom||1)*1.25);renderKWModal()}};
$('#kwModalSig').onchange=()=>{const src=kwModalSeries[kwModalIndex];if(!src)return;const group=src.closest('.stock');if(group){const sig=group.querySelector('.kwSig');if(sig)sig.value=$('#kwModalSig').value;group.querySelectorAll('.priceChart').forEach(drawKWChart)}else drawKWChart($('#kwModalCanvas'))};
$('#kwModalBone').onchange=()=>{const src=kwModalSeries[kwModalIndex];if(!src)return;const group=src.closest('.stock');if(group){const boneCheck=group.querySelector('.kwBone');if(boneCheck)boneCheck.checked=$('#kwModalBone').checked;group.querySelectorAll('.priceChart').forEach(drawKWChart)}else drawKWChart($('#kwModalCanvas'))};
window.addEventListener('keydown',e=>{if($('#chartModal').classList.contains('open')&&e.key==='Escape')closeKWModal()});
window.addEventListener('resize',()=>{if($('#chartModal').classList.contains('open'))renderKWModal();document.querySelectorAll('#stocksRoot .priceChart').forEach(drawKWChart)});
function drawKWChart(cv){
 const chart=cv?._chart;if(!chart)return;
 cv._anchorDate=cv._activeDate||cv._event?.date||chart.date;
 const adapted={...chart,bars:cv._contextBars||chart.bars,sig:chart.sig||[]};
 const modal=cv.id==='kwModalCanvas',group=cv.closest('.stock'),tf=(cv._timeframe||'2m').toLowerCase();
 const signal=modal?$('#kwModalSig').value:(group?.querySelector('.kwSig')?.value||'none');
 const boneZone=modal?$('#kwModalBone').checked:!!group?.querySelector('.kwBone')?.checked;
 const targetH=modal?Math.max(380,(cv.parentElement?.clientHeight||window.innerHeight)-52):undefined;
 window.drawKeywordGapEv(cv,adapted,signal,boneZone,targetH,tf,cv._zoom||1);
}
function hm(m,off){m=((m+(off||0))%1440+1440)%1440;return String(Math.floor(m/60)).padStart(2,'0')+':'+String(m%60).padStart(2,'0')}
function bindKWChart(cv){if(cv._bound||cv.closest('.evc'))return;cv._bound=true;cv.addEventListener('click',()=>openKWModal(cv))}
function drawNormalized(root,events){const chosen=[...root.querySelectorAll('.compareCheck:checked')].map(c=>events[Number(c.dataset.index)]).filter(e=>e.chart);const cv=root.querySelector('.normalized');if(!cv)return;const dpr=devicePixelRatio||1,W=Math.max(300,cv.parentElement.clientWidth-18),H=170;cv.width=W*dpr;cv.height=H*dpr;cv.style.width=W+'px';cv.style.height=H+'px';const x=cv.getContext('2d');x.setTransform(dpr,0,0,dpr,0,0);x.fillStyle='#050607';x.fillRect(0,0,W,H);if(chosen.length<2){x.fillStyle='#8e98a3';x.fillText('Select at least two historical events below to compare normalized session paths.',12,25);return}const L=42,R=8,T=10,B=20,pw=W-L-R,ph=H-T-B;let vals=[];for(const e of chosen){const b=e.chart.bars;vals.push(b.c.map(v=>(v/e.prev_close-1)*100))}const all=vals.flat(),lo=Math.min(-5,...all),hi=Math.max(5,...all),Y=v=>T+ph*(1-(v-lo)/(hi-lo));x.strokeStyle='#2b3239';x.fillStyle='#9aa4ae';for(let k=0;k<=4;k++){const v=lo+(hi-lo)*k/4,yy=Y(v);x.beginPath();x.moveTo(L,yy);x.lineTo(W-R,yy);x.stroke();x.fillText(v.toFixed(0)+'%',2,yy+3)}const colors=['#55bfff','#ffc857','#57d996','#ee7db4','#b39cff','#ff8e66'];chosen.forEach((e,j)=>{const bars=e.chart.bars,series=vals[j];x.strokeStyle=colors[j%colors.length];x.beginPath();series.forEach((v,i)=>{const xx=L+pw*(bars.t[i]-240)/720,yy=Y(v);i?x.lineTo(xx,yy):x.moveTo(xx,yy)});x.stroke();x.fillStyle=colors[j%colors.length];x.fillText(e.date,L+5+j*100,12)});}
function drawCrossStockNormalized(){const checks=[...document.querySelectorAll('#stocksRoot .compareCheck:checked')],chosen=checks.map(c=>({...c._kwEvent,compareTicker:c._kwTicker})).filter(e=>e.chart);const cv=$('.crossNormalized');if(!cv)return;const dpr=devicePixelRatio||1,W=Math.max(320,cv.parentElement.clientWidth-18),H=200;cv.width=W*dpr;cv.height=H*dpr;cv.style.width=W+'px';cv.style.height=H+'px';const x=cv.getContext('2d');x.setTransform(dpr,0,0,dpr,0,0);x.fillStyle='#050607';x.fillRect(0,0,W,H);$('#crossStockCompareStatus').textContent=chosen.length<2?'Select events from multiple ticker sections to compare their price paths on the same previous-close percentage scale.':`${chosen.length} selected events · normalized from previous regular-session close.`;if(chosen.length<2){x.fillStyle='#8e98a3';x.fillText('Select at least two charts above.',12,25);return}const L=42,R=8,T=25,B=22,pw=W-L-R,ph=H-T-B,series=chosen.map(e=>e.chart.bars.c.map(v=>(v/e.prev_close-1)*100)),all=series.flat(),lo=Math.min(-5,...all),hi=Math.max(5,...all),Y=v=>T+ph*(1-(v-lo)/(hi-lo));x.strokeStyle='#2b3239';x.fillStyle='#9aa4ae';for(let k=0;k<=4;k++){const v=lo+(hi-lo)*k/4,yy=Y(v);x.beginPath();x.moveTo(L,yy);x.lineTo(W-R,yy);x.stroke();x.fillText(v.toFixed(0)+'%',2,yy+3)}const colors=['#55bfff','#ffc857','#57d996','#ee7db4','#b39cff','#ff8e66','#ff7373','#9ae66e'];chosen.forEach((e,j)=>{const bars=e.chart.bars;x.strokeStyle=colors[j%colors.length];x.beginPath();series[j].forEach((v,i)=>{const xx=L+pw*(bars.t[i]-240)/720,yy=Y(v);i?x.lineTo(xx,yy):x.moveTo(xx,yy)});x.stroke();x.fillStyle=colors[j%colors.length];x.fillText(`${e.compareTicker} ${e.date}`,L+(j%4)*(pw/4),12+Math.floor(j/4)*11)});}
const kwEventCache=new Map();let lastComparePayload=null,compareChartObserver=null,topCompareObserver=null,currentCompareData=null,qualifiedTickers=new Set();
async function fetchKeywordEvents(ticker,metadataOnly=false){const gapOn=$('#gapFilterEnabled').checked,gapPct=gapOn?$('#gapMinPct').value:'',cacheKey=ticker+'|'+(metadataOnly?'meta':'full')+'|gap:'+(gapOn?gapPct:'off');if(kwEventCache.has(cacheKey))return kwEventCache.get(cacheKey);const q=new URLSearchParams({ticker,user,token,start:$('#startDate').value,end:$('#endDate').value,metadata_only:metadataOnly?'1':'0',require_gap:gapOn?'1':'0',gap_min_pct:gapPct});const r=await fetch('/api/keyword-events?'+q),d=await r.json();if(!r.ok)throw Error(d.error||'Could not load qualifying chart events for '+ticker);kwEventCache.set(cacheKey,d.events||[]);return d.events||[]}
function nearestFiling(ticker,date){const filings=rows.filter(x=>x.ticker===ticker&&x.filing_date&&x.filing_date<=date);if(!filings.length)return null;return filings.reduce((best,row)=>{const distance=Math.abs(Date.parse(row.filing_date)-Date.parse(date));return !best||distance<best.distance?{row,distance}:best},null)?.row||null}
async function collectCompareCandidates(){const symbols=[...qualifiedTickers],out=[];let done=0;for(let i=0;i<symbols.length;i+=3){const batch=symbols.slice(i,i+3);const result=await Promise.allSettled(batch.map(t=>fetchKeywordEvents(t,true)));for(let j=0;j<result.length;j++){done++;$('#compareStatus').textContent=`Collecting qualifying events… ${done}/${symbols.length} tickers`;if(result[j].status!=='fulfilled')continue;for(const e of result[j].value){const f=nearestFiling(batch[j],e.date);out.push({ticker:batch[j],date:e.date,dollar_volume:e.dollar_volume||0,fast_move_multiple:e.fast_move_multiple||0,company:f?.company||batch[j],keyword:$('#keyword').value.trim(),role:f?.role||'',headline:f?.headline||'',filing_url:f?.filing_url||'',filing_date:f?.filing_date||''})}}}return out}
function comparePane(parent,label,feature){const pane=document.createElement('article');pane.className='event';pane.innerHTML=`<div class="eventhead"><span class="eventtitle">${esc(label)}</span><select class="eventTf"><option>1m</option><option selected>2m</option><option>5m</option><option>15m</option><option>30m</option><option value="1d">Day</option></select><select class="eventDays"><option value="1">1 day</option><option value="3">3 days</option><option value="5" selected>5 days</option><option value="10">10 days</option><option value="20">20 days</option></select></div><canvas class="priceChart" height="390"></canvas>`;parent.appendChild(pane);const cv=pane.querySelector('canvas');cv._chart=feature.chart;cv._event={ticker:feature.ticker,date:feature.date,high:feature.hod,low:feature.lod};cv._timeframe='2m';cv._chartDays=5;cv._activeDate=feature.date;bindKWChart(cv);drawKWChart(cv);if(!compareChartObserver)compareChartObserver=new IntersectionObserver(entries=>entries.forEach(entry=>{if(entry.isIntersecting){compareChartObserver.unobserve(entry.target);loadKWContext(entry.target,entry.target._activeDate,entry.target._chartDays)}}),{rootMargin:'250px'});compareChartObserver.observe(cv);pane.querySelector('.eventTf').onchange=e=>{cv._timeframe=e.target.value;loadKWContext(cv,cv._activeDate,cv._chartDays)};pane.querySelector('.eventDays').onchange=e=>{cv._chartDays=Number(e.target.value);loadKWContext(cv,cv._activeDate,cv._chartDays)};return cv}
function drawCompareOverlay(canvas,target,match,type,windowDays){const dpr=devicePixelRatio||1,W=Math.max(360,canvas.parentElement.clientWidth-12),H=220;canvas.width=W*dpr;canvas.height=H*dpr;canvas.style.width=W+'px';canvas.style.height=H+'px';const x=canvas.getContext('2d');x.setTransform(dpr,0,0,dpr,0,0);x.fillStyle='#050607';x.fillRect(0,0,W,H);const L=45,R=12,T=18,B=25,pw=W-L-R,ph=H-T-B,colors=['#53bfff','#ffc857'];let series=[];if(type==='intraday'){series=[target.intraday.map(p=>p.prev),match.intraday.map(p=>p.prev)];}else{const toPath=f=>{const pre=f.pre_path||[],post=f.post_path||[];return [...Array(Math.max(0,windowDays-pre.length)).fill(null),...pre,0,...post,...Array(Math.max(0,windowDays-post.length)).fill(null)]};series=[toPath(target),toPath(match)]}const vals=series.flat().filter(v=>v!=null&&Number.isFinite(v));if(!vals.length){x.fillStyle='#9ba5af';x.fillText('Not enough real bars for this overlay.',12,30);return}const lo=Math.min(0,...vals),hi=Math.max(0,...vals),Y=v=>T+ph*(1-(v-lo)/(hi-lo||1)),count=Math.max(...series.map(a=>a.length)),X=i=>L+pw*(count<2?0:i/(count-1));x.strokeStyle='#28303a';x.fillStyle='#9ba5af';for(let k=0;k<=4;k++){const v=lo+(hi-lo)*k/4,yy=Y(v);x.beginPath();x.moveTo(L,yy);x.lineTo(W-R,yy);x.stroke();x.fillText(v.toFixed(0)+'%',3,yy+3)}if(type==='intraday'){const openX=L+pw*330/720;x.fillStyle='rgba(255,255,255,.06)';x.fillRect(L,T,openX-L,ph);x.strokeStyle='#8b929a';x.setLineDash([3,3]);x.beginPath();x.moveTo(openX,T);x.lineTo(openX,T+ph);x.stroke();x.setLineDash([]);x.fillStyle='#8e98a3';x.fillText('PREMARKET',L+4,T+12);x.fillText('09:30 ET',openX+4,H-7)}else{x.fillStyle='#8e98a3';x.fillText(`-${windowDays} sessions`,L,H-7);x.fillText('D',L+pw*windowDays/(2*windowDays),H-7);x.fillText(`+${windowDays}`,W-50,H-7)}series.forEach((arr,j)=>{x.strokeStyle=colors[j];x.lineWidth=1.8;x.beginPath();let started=false;arr.forEach((v,i)=>{if(v==null){started=false;return}const xx=X(i),yy=Y(v);started?x.lineTo(xx,yy):x.moveTo(xx,yy);started=true});x.stroke()});x.fillStyle=colors[0];x.fillText(`${target.ticker} target`,L,12);x.fillStyle=colors[1];x.fillText(`${match.ticker} ${fmtDate(match.date)}`,L+130,12)}
function renderCompare(data){const root=$('#compareResults'),t=data.target;root.innerHTML=`<div class="compareSummary"><b>${esc(t.ticker)} · ${esc(fmtDate(t.date))}</b> compared against ${data.sample_matches?.length||data.matches.length} closest qualifying event days. Outcomes use all ${data.sample_matches?.length||data.matches.length} matches; only the top three matched charts are shown.</div>`;if(!data.matches.length){root.insertAdjacentHTML('beforeend','<div class="status">No matching qualified event days found.</div>');return}const colors=['#71c7ff','#ffc857','#66d99a','#f487b5','#b9a1ff','#ff9270','#f46a6a','#a6df67'];root.insertAdjacentHTML('beforeend',`<div class="qualifiedMatchList">${data.matches.map((m,i)=>{const f=m.features||{};return `<article class="qualifiedMatch"><b style="color:${colors[i%colors.length]}">${esc(m.ticker)} · ${esc(fmtDate(m.date))}</b><span>Match ${m.score.toFixed(0)}/100</span><span>PM high ${f.premarket_high_pct==null?'—':f.premarket_high_pct.toFixed(0)+'%'}</span><span>Dollar volume ${f.session_dollar_volume==null?'—':'$'+fmtNum(f.session_dollar_volume)}</span></article>`}).join('')}</div>`)}function renderTopCompareCharts(data){
 const root=$('#compareResults'),target=data.target,matches=(data.matches||[]).slice(0,3);
 if(!target?.chart)return;
 const heading=document.createElement('h3');heading.textContent=`Target date + top ${matches.length} similar charts`;root.appendChild(heading);
 const tools=document.createElement('div');tools.className='kwChartTools compareGlobalChartTools';
 tools.innerHTML=`<div class="kwToolGroup"><label>All chart timeframes <select id="compareAllTf" class="evtf"><option>1m</option><option selected>2m</option><option>5m</option><option>15m</option><option>30m</option><option value="1d">Day</option></select></label><label>Days shown <select id="compareAllDays" class="evtf"><option value="1" selected>1 day</option><option value="3">3 days</option><option value="5">5 days</option><option value="10">10 days</option><option value="20">20 days</option><option value="60">60 days</option></select></label></div>`;
 root.appendChild(tools);
 const grid=document.createElement('div');grid.className='compareEventGrid';root.appendChild(grid);
 const cards=[{ticker:target.ticker,date:target.date,feature:target,chart:target.chart,target:true},...matches.map((m,i)=>({ticker:m.ticker,date:m.date,feature:m.features||{},chart:m.features?.chart,match:m,index:i}))];
 const chartCanvases=[];
 cards.forEach(item=>{
  const f=item.feature||{},scores=item.match?.scores||{},card=document.createElement('article');card.className='evc compareEventCard';
  const gap=f.gap==null?null:Number(f.gap),dvol=f.session_dollar_volume==null?null:Number(f.session_dollar_volume);
  const gapTag=gap==null?'Gap —':`Gap ${gap>=0?'+':''}${gap.toFixed(1)}%`;
  const matchFamily=scores.A!=null&&(scores.B!=null||scores.C!=null)?'Intraday + daily':scores.A!=null?'Intraday':'Daily';
  const basis=item.target?'<span class="evtag rule">Reference day</span>':`<span class="evtag rule">Intraday match ${scores.A==null?'—':scores.A.toFixed(0)+'%'}</span><span class="evtag rule">Pre-event daily match ${scores.B==null?'—':scores.B.toFixed(0)+'%'}</span><span class="evtag rule">Post-event daily match ${scores.C==null?'—':scores.C.toFixed(0)+'%'}</span>`;
  card.innerHTML=`<div class="evhead"><div class="kwEventHeading"><div class="evt">${item.target?'TARGET':`#${item.index+1} SIMILAR`} · ${esc(item.ticker)} · ${esc(fmtDate(item.date))}</div><div class="evtags"><span class="evtag eventDate" title="Original qualifying event day">Event ${esc(fmtDate(item.date))}</span><span class="evtag gap">${gapTag}</span><span class="evtag dvol">Vol $${Number.isFinite(dvol)?fmtNum(dvol):'—'}</span>${basis}<span class="evtag rule">${item.target?'Entered ticker':`Basis: ${matchFamily} · overall ${item.match.score.toFixed(0)}%`}</span></div></div><div class="evcontrols"><select class="evtf" title="Timeframe"><option>1m</option><option selected>2m</option><option>5m</option><option>15m</option><option>30m</option><option value="1d">Day</option></select><select class="evdays" title="Sessions shown"><option value="1" selected>1 day</option><option value="3">3 days</option><option value="5">5 days</option><option value="10">10 days</option><option value="20">20 days</option><option value="60">60 days</option></select><button class="evzoom" data-zoom="out" title="Zoom out">−</button><button class="evzoom" data-zoom="in" title="Zoom in">+</button></div></div><canvas class="priceChart" height="390" aria-label="${esc(item.ticker)} ${esc(fmtDate(item.date))} comparison chart"></canvas>`;
  grid.appendChild(card);
  const cv=card.querySelector('canvas');chartCanvases.push(cv);cv._chart=item.chart;
  cv._event={...f,ticker:item.ticker,date:item.date,gap_pct:gap==null?null:gap/100,dollar_volume:dvol};
  cv._titlePrefix=item.target?'TARGET':`#${item.index+1} SIMILAR`;cv._timeframe='2m';cv._chartDays=1;
  cv._activeDate=item.date;cv._anchorDate=item.date;cv._zoom=1;cv._panOffset=0;cv._yZoom=1;
  cv._contextBars=null;cv._contextDates=[];cv._contextMetrics={};cv._contextDaily=false;
  cv._onPanChange=value=>cv._panOffset=value;
  cv._onPanEdge=side=>{if(cv._contextLoading)return;const dates=cv._contextDates||[],edge=dates[side>0?dates.length-1:0];if(!edge)return;cv._chartDays=[1,3,5,10,20,60].find(value=>value>cv._chartDays)||60;cv._panOffset=0;card.querySelector('.evdays').value=String(cv._chartDays);loadKWContext(cv,edge,cv._chartDays)};
  card.onclick=e=>{if(e.target.closest('.evcontrols'))return;openKWModal(cv)};
  card.querySelector('.evcontrols').onclick=e=>e.stopPropagation();
  const tf=card.querySelector('.evtf'),days=card.querySelector('.evdays');
  tf.onchange=e=>{e.stopPropagation();cv._timeframe=tf.value;cv._chartDays=tf.value==='1d'?60:1;days.value=String(cv._chartDays);cv._panOffset=0;cv._yZoom=1;loadKWContext(cv,cv._activeDate||item.date,cv._chartDays)};
  days.onchange=e=>{e.stopPropagation();cv._chartDays=Number(days.value);cv._panOffset=0;loadKWContext(cv,cv._activeDate||item.date,cv._chartDays)};
  card.querySelectorAll('.evzoom').forEach(btn=>btn.onclick=e=>{e.stopPropagation();cv._zoom=Math.max(.5,Math.min(8,(cv._zoom||1)*(btn.dataset.zoom==='in'?1.25:.8)));drawKWChart(cv)});
  drawKWChart(cv);
  if(!topCompareObserver)topCompareObserver=new IntersectionObserver(entries=>entries.forEach(entry=>{if(entry.isIntersecting){topCompareObserver.unobserve(entry.target);const canvas=entry.target;loadKWContext(canvas,canvas._activeDate,canvas._chartDays)}}),{rootMargin:'450px 0px'});
  topCompareObserver.observe(cv);
 });
 const setAll=(timeframe,days)=>{for(const cv of chartCanvases){const card=cv.closest('.compareEventCard');cv._timeframe=timeframe;cv._chartDays=days;cv._panOffset=0;cv._yZoom=1;card.querySelector('.evtf').value=timeframe;card.querySelector('.evdays').value=String(days);loadKWContext(cv,cv._activeDate||cv._event.date,days)}};
 $('#compareAllTf').onchange=()=>{const tf=$('#compareAllTf').value,days=tf==='1d'?60:1;$('#compareAllDays').value=String(days);setAll(tf,days)};
 $('#compareAllDays').onchange=()=>setAll($('#compareAllTf').value,Number($('#compareAllDays').value)||1);
}
function drawUnifiedProjection(data){const root=$('#unifiedProjectionRoot'),target=data.target,matches=data.matches;root.innerHTML=`<section class="unifiedProjection"><div class="unifiedChartWrap"><b>Unified normalized path · target vs closest matches</b><div class="compareNote">Weight method: ${esc(data.weight_report?.method||'documented default')} · training n=${data.weight_report?.sample_size??0}; this is a leave-one-event-out open-to-close RMSE weight fit when at least 30 events are available.</div><div class="unifiedControls"><label>Y axis<select id="unifiedMode"><option value="prev">% from previous close</option><option value="open">% from open</option><option value="retention">Gap retention</option></select></label><button id="unifiedDaily">Show D-10…D+10</button><span class="compareNote">The chart shows the top three matches; observed outcomes use all ${data.sample_matches?.length||data.matches.length} matches.</span></div><div class="unifiedLegend" id="unifiedLegend"></div><canvas class="unifiedCanvas" id="unifiedCanvas"></canvas><div class="chartTip" id="unifiedTip"></div><div class="compareNote">Historical similarity is not causation or a guarantee. Volume below is cumulative session-volume fraction; float rotation is unavailable unless verified float data exists.</div></div><aside class="projectionPanel"><h3>Observed outcomes · not a prediction</h3><div id="projectionConfidence"></div><div class="projectionGrid" id="projectionGrid"></div><div class="compareNote" id="projectionFoot"></div></aside></section>`;const legend=$('#unifiedLegend');matches.forEach((m,i)=>{const label=document.createElement('label');label.innerHTML=`<input type="checkbox" checked data-match="${i}"><span style="color:${['#71c7ff','#ffc857','#66d99a','#f487b5','#b9a1ff','#ff9270','#f46a6a','#a6df67'][i%8]}">${esc(m.ticker)} ${esc(fmtDate(m.date))}</span>`;legend.appendChild(label);label.querySelector('input').onchange=()=>drawUnifiedCanvas(data)});$('#unifiedMode').onchange=()=>drawUnifiedCanvas(data);$('#unifiedDaily').onclick=()=>{const b=$('#unifiedDaily');b.dataset.daily=b.dataset.daily==='1'?'0':'1';b.textContent=b.dataset.daily==='1'?'Show intraday':'Show D-10…D+10';drawUnifiedCanvas(data)};drawUnifiedCanvas(data);drawProjectionPanel(data)}
window.qtile=function(values,q){const a=values.filter(Number.isFinite).sort((x,y)=>x-y);if(!a.length)return null;const p=(a.length-1)*q,l=Math.floor(p),f=p-l;return a[l]*(1-f)+a[Math.min(l+1,a.length-1)]*f}
function selectedUnifiedMatches(data){return [...$('#unifiedLegend').querySelectorAll('input:checked')].map(x=>data.matches[Number(x.dataset.match)]).filter(Boolean)}
function unifiedValue(feature,point,mode){if(mode==='open')return point?.open??null;if(mode==='retention'){const gap=feature.gap;if(gap==null||Math.abs(gap)<.01)return null;return point?.prev==null?null:point.prev/gap*100}return point?.prev??null}
function drawUnifiedCanvas(data){const cv=$('#unifiedCanvas');if(!cv)return;const target=data.target,chosen=selectedUnifiedMatches(data),daily=$('#unifiedDaily').dataset.daily==='1',mode=$('#unifiedMode').value,dpr=devicePixelRatio||1,W=Math.max(500,cv.parentElement.clientWidth-10),H=440;cv.width=W*dpr;cv.height=H*dpr;cv.style.width=W+'px';cv.style.height=H+'px';const x=cv.getContext('2d');x.setTransform(dpr,0,0,dpr,0,0);x.fillStyle='#050607';x.fillRect(0,0,W,H);const L=52,R=12,T=20,B=26,pw=W-L-R,priceH=H*.68,volTop=T+priceH+18,volH=H-volTop-B,colors=['#71c7ff','#ffc857','#66d99a','#f487b5','#b9a1ff','#ff9270','#f46a6a','#a6df67'];let series=[],lineNames=[];const nowMinute=Number(data.as_of_minute??target.as_of_minute??960),nowIdx=Math.max(0,Math.min(143,Math.floor((nowMinute-240)/5)));if(daily){series=[{name:`${target.ticker} target`,vals:target.daily_path_curve||[],color:'#fff',target:true},...chosen.map((m,j)=>({name:`${m.ticker} ${fmtDate(m.date)}`,vals:m.features.daily_path_curve||[],color:colors[j%colors.length]}))]}else{const modeFor=f=>f.intraday.map(p=>unifiedValue(f,p,mode));const targetVals=modeFor(target);series=[{name:`${target.ticker} target`,vals:targetVals,color:'#fff',target:true}];chosen.forEach((m,j)=>series.push({name:`${m.ticker} ${fmtDate(m.date)}`,vals:modeFor(m.features),color:colors[j%colors.length],feature:m.features}));}
 const nowTarget=series[0].vals[daily?10:nowIdx];let projected=[];if(nowTarget!=null){const cut=daily?10:nowIdx,end=daily?21:144;for(let j=1;j<series.length;j++){const base=series[j].vals[cut];if(base==null)continue;const arr=[];for(let i=cut;i<end;i++){const v=series[j].vals[i];arr.push(v==null?null:nowTarget+v-base)}projected.push({name:series[j].name,color:series[j].color,from:cut,vals:arr})}}
 const quantiles=[];if(projected.length){const cut=daily?10:nowIdx,end=daily?21:144;for(let i=cut;i<end;i++){const vs=projected.map(s=>s.vals[i-cut]).filter(Number.isFinite);quantiles.push({i,p10:window.qtile(vs,.1),p25:window.qtile(vs,.25),p50:window.qtile(vs,.5),p75:window.qtile(vs,.75),p90:window.qtile(vs,.9)})}}
 const values=series.flatMap(s=>s.vals.filter(Number.isFinite)).concat(quantiles.flatMap(q=>[q.p10,q.p90].filter(Number.isFinite)));let lo=Math.min(-5,...values),hi=Math.max(5,...values);const Y=v=>T+priceH*(1-(v-lo)/(hi-lo||1)),X=i=>L+pw*(i/(daily?20:143));x.strokeStyle='#242c34';x.fillStyle='#aab4be';x.font='11px Consolas,monospace';for(let k=0;k<=5;k++){const v=lo+(hi-lo)*k/5,yy=Y(v);x.beginPath();x.moveTo(L,yy);x.lineTo(W-R,yy);x.stroke();x.fillText(v.toFixed(1)+'%',3,yy+3)}if(daily){x.fillStyle='#85919b';for(let i=0;i<=20;i+=5)x.fillText(String(i-10),X(i)-5,H-7);const dx=X(10);x.strokeStyle='#888';x.setLineDash([3,3]);x.beginPath();x.moveTo(dx,T);x.lineTo(dx,T+priceH);x.stroke();x.setLineDash([])}else{const preEnd=X(330/5);x.fillStyle='rgba(255,255,255,.055)';x.fillRect(L,T,preEnd-L,priceH);x.fillStyle='#89939c';x.fillText('PREMARKET',L+5,T+13);x.strokeStyle='#888';x.setLineDash([3,3]);x.beginPath();x.moveTo(preEnd,T);x.lineTo(preEnd,T+priceH);x.stroke();x.setLineDash([]);const times=[240,360,480,570,660,750,840,930];for(const m of times){const i=(m-240)/5;x.fillStyle='#929ca5';x.fillText(m===570?'09:30':`${String(Math.floor(m/60)).padStart(2,'0')}:${String(m%60).padStart(2,'0')}`,X(i)-15,H-7)}}
 if(quantiles.length){const band=(low,high,color)=>{x.fillStyle=color;x.beginPath();let started=false;for(const q of quantiles){if(q[low]==null){started=false;continue}const xx=X(q.i),yy=Y(q[low]);started?x.lineTo(xx,yy):x.moveTo(xx,yy);started=true}for(const q of [...quantiles].reverse()){if(q[high]!=null)x.lineTo(X(q.i),Y(q[high]))}x.closePath();x.fill()};band('p10','p90','rgba(255,205,90,.10)');band('p25','p75','rgba(255,205,90,.20)');x.strokeStyle='#ffd36e';x.lineWidth=2;x.beginPath();quantiles.forEach((q,i)=>{if(q.p50==null)return;i?x.lineTo(X(q.i),Y(q.p50)):x.moveTo(X(q.i),Y(q.p50))});x.stroke()}
 series.forEach((s,j)=>{x.strokeStyle=s.color;x.globalAlpha=s.target?1:.46;x.lineWidth=s.target?2.7:1.1;x.beginPath();let started=false;s.vals.forEach((v,i)=>{if(v==null||(!daily&&s.target&&i>nowIdx)){started=false;return}const xx=X(i),yy=Y(v);started?x.lineTo(xx,yy):x.moveTo(xx,yy);started=true});x.stroke();x.globalAlpha=1});if(!daily){const marker=X(nowIdx);x.strokeStyle='#f3f4f6';x.setLineDash([4,3]);x.beginPath();x.moveTo(marker,T);x.lineTo(marker,T+priceH+volH+18);x.stroke();x.setLineDash([]);x.fillStyle='#fff';x.fillText('NOW',marker+4,T+12);const baseY=volTop+volH; x.strokeStyle='#303842';x.beginPath();x.moveTo(L,volTop);x.lineTo(W-R,volTop);x.stroke();x.fillStyle='#8f9aa4';x.fillText('Cumulative volume fraction',L+3,volTop+12);const drawVol=(arr,color,width)=>{x.strokeStyle=color;x.lineWidth=width;x.beginPath();let started=false;arr.forEach((v,i)=>{if(v==null||(!data.partial_day&&i>nowIdx&&arr===target.volume_curve)){started=false;return}const xx=X(i),yy=baseY-(Number(v)||0)*volH;started?x.lineTo(xx,yy):x.moveTo(xx,yy);started=true});x.stroke()};drawVol(target.volume_curve,'#fff',2);const medVol=[];for(let i=0;i<144;i++){const vals=chosen.map(m=>m.features.intraday?.[i]?.cumvol).filter(Number.isFinite);medVol.push(window.qtile(vals,.5))}drawVol(medVol,'#ffc857',1.5)}else{ x.fillStyle='#8c98a2';x.fillText('Cumulative daily return · D-10 … D+10',L,T+priceH+14)}
 cv.onmousemove=e=>{const rect=cv.getBoundingClientRect(),i=Math.max(0,Math.min(daily?20:143,Math.round((e.clientX-rect.left-L)/pw*(daily?20:143))));const lines=[];if(daily){lines.push(`${target.ticker}: ${series[0].vals[i]?.toFixed(2)??'—'}%`);for(let j=1;j<series.length;j++)lines.push(`${series[j].name}: ${series[j].vals[i]?.toFixed(2)??'—'}%`)}else{const stamp=240+i*5;lines.push(`${String(Math.floor(stamp/60)).padStart(2,'0')}:${String(stamp%60).padStart(2,'0')} ET`);lines.push(`${target.ticker}: ${series[0].vals[i]?.toFixed(2)??'—'}%`);for(let j=1;j<series.length;j++)lines.push(`${series[j].name}: ${series[j].vals[i]?.toFixed(2)??'—'}%`);const p=target.intraday[i];if(p?.vol!=null)lines.push(`Target OHLC ${p.low?.toFixed(3)??'—'} / ${p.high?.toFixed(3)??'—'} · vol ${fmtNum(p.vol)}`)}const tip=$('#unifiedTip');tip.textContent=lines.join('\n');tip.style.display='block';tip.style.left=Math.min(W-260,e.offsetX+12)+'px';tip.style.top=Math.max(25,e.offsetY+10)+'px'};cv.onmouseleave=()=>$('#unifiedTip').style.display='none'}
function drawProjectionPanel(data){if(data.validation?.beats_baseline===false&&!document.querySelector("#matcherWarning"))document.querySelector("#compareResults")?.insertAdjacentHTML("afterbegin",`<div id="matcherWarning" class="lowConfidence">${esc(data.validation.warning||"Validation failed against baselines.")}</div>`);const target=data.target,chosen=data.sample_matches||data.matches||[],panel=$('#projectionGrid'),badge=$('#projectionConfidence'),foot=$('#projectionFoot');if(!panel)return;const partial=data.partial_day,now=Number(data.as_of_minute??target.as_of_minute??960),idx=Math.max(0,Math.min(143,Math.floor((now-240)/5))),mode=$('#unifiedMode').value,cur=unifiedValue(target,target.intraday?.[idx],mode);const remainders=[],fades=[],closeOpen=[],nextGaps=[],newHod=[],nextHod=[];if(partial&&cur!=null)for(const m of chosen){const f=m.features,p=f.intraday||[],last=unifiedValue(f,p[143],mode),at=unifiedValue(f,p[idx],mode);if(last!=null&&at!=null)remainders.push(last-at);if(f.hod_min!=null&&f.close&&f.hod){fades.push((f.close/f.hod-1)*100);if(f.hod_min>(now-RG_START)){newHod.push(1);nextHod.push(f.hod_min-(now-RG_START))}else newHod.push(0)}if(f.open_close!=null)closeOpen.push(f.open_close);if(f.post_open_gap?.length&&f.post_open_gap[0]!=null)nextGaps.push(f.post_open_gap[0])}const avg=chosen.length?chosen.reduce((s,m)=>s+m.score,0)/chosen.length:null,disp=remainders.length?Math.sqrt(remainders.reduce((s,v)=>s+(v-window.qtile(remainders,.5))**2,0)/remainders.length):null,coverage=chosen.length?Math.round(100*chosen.reduce((s,m)=>s+Object.values(m.features.feature_mask||{}).filter(Boolean).length/Math.max(1,Object.keys(m.features.feature_mask||{}).length),0)/chosen.length):0,low=chosen.length<5||avg<40||disp>25||data.validation?.beats_baseline===false;badge.innerHTML=low?'<span class="lowConfidence">LOW CONFIDENCE</span>':'<span class="confidenceGood">Confidence checks passed</span>';const dist=(a,suffix='%')=>{const q10=window.qtile(a,.1),q25=window.qtile(a,.25),q50=window.qtile(a,.5),q75=window.qtile(a,.75),q90=window.qtile(a,.9);return q50==null?'Unavailable':`${q50.toFixed(1)}${suffix} · 25–75 ${q25.toFixed(1)} to ${q75.toFixed(1)} · 10–90 ${q10.toFixed(1)} to ${q90.toFixed(1)}`};const probability=newHod.length?`${newHod.reduce((s,v)=>s+v,0)} of ${newHod.length} (${(100*newHod.reduce((s,v)=>s+v,0)/newHod.length).toFixed(0)}%)`: 'Unavailable';panel.innerHTML=`<span>Matches used</span><span>${chosen.length} / ${(data.sample_matches||data.matches).length}</span><span>Average match score</span><span>${avg==null?'Unavailable':avg.toFixed(1)+'/100'}</span><span>Feature coverage</span><span>${coverage}% available</span><span>Remaining move to close</span><span>${partial?dist(remainders):'Historical full day; no remaining session'}</span><span>HOD-to-close fade</span><span>${partial?dist(fades):'Unavailable'}</span><span>New HOD after now</span><span>${partial?probability:'Unavailable'}</span><span>Time to next HOD</span><span>${partial?dist(nextHod,' min'):'Unavailable'}</span><span>Close vs open</span><span>${partial?dist(closeOpen):'Unavailable'}</span><span>D+1 open gap</span><span>${dist(nextGaps)}</span><span>Outcome dispersion</span><span>${disp==null?'Unavailable':disp.toFixed(1)+' percentage points SD'}</span>`;foot.textContent=partial?`Outcomes are rebased to the target's last complete 5-minute value near ${String(Math.floor(now/60)).padStart(2,'0')}:${String(now%60).padStart(2,'0')} ET. Match counts vary by available bars; D+1 uses only candidates with a real next session.`:'The selected target date is a completed historical session, so today’s remaining-session projection is not applicable.'}
function drawForecast2m(data){
 const root=$('#forecast2mRoot');if(!root)return;const fc=data.forecast;
 if(!fc){root.innerHTML='';return}
 const low=!fc.calibrated||fc.ess<4||data.validation?.beats_baseline===false;
 const pc=data.target.pc,fmtP=v=>v==null?'—':v.toFixed(v<1?4:3),pctOf=v=>((v/pc-1)*100).toFixed(1)+'%',nowP=fc.now_price,cq=fc.close_q;
 root.innerHTML=`<section class="unifiedProjection"><div class="unifiedChartWrap"><b>Intraday candle forecast - 2-minute bars</b><div class="compareNote">This chart appears below the normalized comparison graph. Each 2-minute candle shows a weighted OHLC percentile from the same clock-time slices of ${fc.n} similar days, volatility-scaled and centered on the drift-shrunk forecast. Percentiles are calculated candle by candle, so the plotted sequence summarizes the distribution and is not one exact historical path. Shaded bands show the wider projected price range. Hover each candle for its historical-analog probability of closing up.</div><div class="unifiedControls"><label>Y axis<select id="fc2mMode"><option value="price">Price</option><option value="pct">% from previous close</option></select></label><label>Predicted candles<select id="fc2mScn">${(fc.scenarios||[]).map((s,i)=>`<option value="${i}">${esc(s.name)}</option>`).join("")}<option value="-1">Off (fan only)</option></select></label><label style="flex-direction:row;align-items:center;gap:4px"><input type="checkbox" id="fc2mAnalogs"> show analog paths</label></div><canvas class="unifiedCanvas" id="fc2mCanvas" style="height:400px"></canvas><div class="chartTip" id="fc2mTip"></div></div><aside class="projectionPanel"><h3>Projection from now · ${esc(String(Math.floor(fc.as_of_minute/60)).padStart(2,'0')+':'+String(fc.as_of_minute%60).padStart(2,'0'))} ET</h3>${low?'<span class="lowConfidence">LOW CONFIDENCE</span>':'<span class="confidenceGood">Calibrated band</span>'}<div class="projectionGrid"><span>Price now</span><span>${fmtP(nowP)} (${pctOf(nowP)})</span><span>Median close</span><span>${fmtP(cq[2])} (${pctOf(cq[2])})</span><span>Close 25–75</span><span>${fmtP(cq[1])} – ${fmtP(cq[3])}</span><span>Close 10–90</span><span>${fmtP(cq[0])} – ${fmtP(cq[4])}</span><span>P(new HOD)</span><span>${(fc.p_new_hod*100).toFixed(0)}%</span><span>P(close below now)</span><span>${(fc.p_close_below_now*100).toFixed(0)}%</span><span>Analogs / effective n</span><span>${fc.n} / ${fc.ess}</span><span>2-min vol (last hour)</span><span>${fc.sigma_2m_pct}%</span><span>Drift kept</span><span>${Math.round(fc.shrink*100)}%</span></div></aside></section>`;
 $('#fc2mMode').onchange=()=>drawForecast2mCanvas(data);$('#fc2mAnalogs').onchange=()=>drawForecast2mCanvas(data);$('#fc2mScn').onchange=()=>drawForecast2mCanvas(data);drawForecast2mCanvas(data)}
function drawForecast2mCanvas(data){
 const cv=$('#fc2mCanvas');if(!cv)return;const fc=data.forecast,tg=data.target,pc=tg.pc,mode=$('#fc2mMode').value,showA=$('#fc2mAnalogs').checked;
 const cvt=v=>v==null?null:(mode==='pct'?(v/pc-1)*100:v);
 const dpr=devicePixelRatio||1,W=Math.max(500,cv.parentElement.clientWidth-10),H=400;cv.width=W*dpr;cv.height=H*dpr;cv.style.width=W+'px';cv.style.height=H+'px';
 const x=cv.getContext('2d');x.setTransform(dpr,0,0,dpr,0,0);x.fillStyle='#050607';x.fillRect(0,0,W,H);
 const L=52,R=12,T=14,B=24,VH=58,pw=W-L-R,ph=H-T-B-VH-10,now=fc.as_of_minute,X=m=>L+pw*(m-240)/720;
 const scn=(fc.scenarios||[])[Number($('#fc2mScn').value)],all=window.aggregateKeywordBars(tg.chart.bars,2),ids=[];for(let i=0;i<all.t.length;i++)if(all.t[i]<=now)ids.push(i);
 const vals=[];ids.forEach(i=>{vals.push(all.h[i],all.l[i])});['p10','p90'].forEach(k=>fc[k].forEach(v=>vals.push(v)));if(fc.actual)fc.actual.forEach(v=>vals.push(v));if(scn)scn.bars.forEach(b=>vals.push(b[2],b[3]));
 let lo=Math.min(...vals),hi=Math.max(...vals);const pad=(hi-lo)*.05||hi*.02;lo-=pad;hi+=pad;const Y=v=>T+ph*(1-(v-lo)/(hi-lo||1));
 x.font='11px Consolas,monospace';x.strokeStyle='#242c34';x.fillStyle='#aab4be';
 for(let k=0;k<=5;k++){const v=lo+(hi-lo)*k/5,yy=Y(v);x.beginPath();x.moveTo(L,yy);x.lineTo(W-R,yy);x.stroke();const t=cvt(v);x.fillText(mode==='pct'?t.toFixed(1)+'%':t.toFixed(v<1?3:2),3,yy+3)}
 const preEnd=X(570);x.fillStyle='rgba(255,255,255,.055)';x.fillRect(L,T,preEnd-L,ph);x.fillStyle='#89939c';x.fillText('PREMARKET',L+5,T+13);
 for(const m of [240,360,480,570,660,750,840,930]){x.fillStyle='#929ca5';x.fillText(m===570?'09:30':String(Math.floor(m/60)).padStart(2,'0')+':'+String(m%60).padStart(2,'0'),X(m)-15,H-7)}
 x.strokeStyle='#888';x.setLineDash([3,3]);x.beginPath();x.moveTo(preEnd,T);x.lineTo(preEnd,T+ph);x.stroke();x.setLineDash([]);
 const cw=Math.max(1,pw/360*.7);
 const maxV=Math.max(1,...ids.map(i=>all.v[i]||0),...(scn?scn.bars.map(b=>b[5]):[])),vy1=H-B;
 ids.forEach(i=>{const up=all.c[i]>=all.o[i],col=up?'#3fb97f':'#e5575e',xx=X(all.t[i]+1);x.strokeStyle=col;x.fillStyle=col;x.beginPath();x.moveTo(xx,Y(all.h[i]));x.lineTo(xx,Y(all.l[i]));x.stroke();const y1=Y(all.o[i]),y2=Y(all.c[i]);x.fillRect(xx-cw/2,Math.min(y1,y2),cw,Math.max(1,Math.abs(y2-y1)));x.globalAlpha=.55;x.fillRect(xx-cw/2,vy1-VH*(all.v[i]||0)/maxV,cw,VH*(all.v[i]||0)/maxV);x.globalAlpha=1});
 x.strokeStyle='rgba(200,210,255,.6)';x.lineWidth=1;x.beginPath();let st=false;ids.forEach(i=>{const v=all.vw[i];if(v==null){st=false;return}const xx=X(all.t[i]+1),yy=Y(v);st?x.lineTo(xx,yy):x.moveTo(xx,yy);st=true});x.stroke();
 const nx0=X(now),tt=fc.t.map(m=>X(m+1)),band=(a,b,c)=>{x.fillStyle=c;x.beginPath();fc[a].forEach((v,i)=>i?x.lineTo(tt[i],Y(v)):x.moveTo(tt[i],Y(v)));for(let i=fc[b].length-1;i>=0;i--)x.lineTo(tt[i],Y(fc[b][i]));x.closePath();x.fill()};
 band('p10','p90','rgba(255,205,90,.10)');band('p25','p75','rgba(255,205,90,.22)');
 if(showA)fc.paths.forEach(p=>{x.strokeStyle='rgba(150,190,255,.35)';x.lineWidth=1;x.beginPath();p.vals.forEach((v,i)=>i?x.lineTo(tt[i],Y(v)):x.moveTo(tt[i],Y(v)));x.stroke()});
 if(scn){scn.bars.forEach(b=>{const up=b[4]>=b[1],col=up?'#3fb97f':'#e5575e',xx=X(b[0]+1);x.strokeStyle=col;x.fillStyle=col;x.globalAlpha=.8;x.beginPath();x.moveTo(xx,Y(b[2]));x.lineTo(xx,Y(b[3]));x.stroke();const y1=Y(b[1]),y2=Y(b[4]);x.globalAlpha=up?.35:.8;x.fillRect(xx-cw/2,Math.min(y1,y2),cw,Math.max(1,Math.abs(y2-y1)));if(up){x.globalAlpha=.9;x.strokeRect(xx-cw/2,Math.min(y1,y2),cw,Math.max(1,Math.abs(y2-y1)))}x.globalAlpha=.3;x.fillRect(xx-cw/2,vy1-VH*b[5]/maxV,cw,VH*b[5]/maxV);x.globalAlpha=1});x.fillStyle='#ffd36e';x.fillText('PREDICTED '+scn.name+' weighted OHLC quantiles',nx0+8,T+26)}
 if(fc.actual){x.strokeStyle='rgba(255,255,255,.8)';x.setLineDash([2,2]);x.lineWidth=1.5;x.beginPath();fc.actual.forEach((v,i)=>i?x.lineTo(tt[i],Y(v)):x.moveTo(tt[i],Y(v)));x.stroke();x.setLineDash([])}
 x.strokeStyle='#ffd36e';x.lineWidth=1.6;x.setLineDash([5,3]);x.beginPath();fc.p50.forEach((v,i)=>i?x.lineTo(tt[i],Y(v)):x.moveTo(tt[i],Y(v)));x.stroke();x.setLineDash([]);x.lineWidth=1;
 const nx=X(now);x.strokeStyle='#f3f4f6';x.setLineDash([4,3]);x.beginPath();x.moveTo(nx,T);x.lineTo(nx,H-B);x.stroke();x.setLineDash([]);x.fillStyle='#fff';x.fillText('NOW',nx+4,T+12);
 cv.onmousemove=e=>{const rect=cv.getBoundingClientRect(),m=240+(e.clientX-rect.left-L)/pw*720,tip=$('#fc2mTip');if(m<240||m>960){tip.style.display='none';return}const lines=[String(Math.floor(m/60)).padStart(2,'0')+':'+String(Math.floor(m)%60).padStart(2,'0')+' ET'];
  if(m<=now+1){let bi=-1,bd=1e9;ids.forEach(i=>{const d=Math.abs(all.t[i]+1-m);if(d<bd){bd=d;bi=i}});if(bi>=0){const f=v=>cvt(v).toFixed(mode==='pct'?1:3);lines.push('O '+f(all.o[bi])+'  H '+f(all.h[bi]));lines.push('L '+f(all.l[bi])+'  C '+f(all.c[bi]));lines.push('vol '+fmtNum(all.v[bi]))}}
  else{let k=Math.max(0,Math.min(fc.t.length-1,Math.round((m-fc.t[0])/fc.step)));const f=v=>cvt(v).toFixed(mode==='pct'?1:3);if(scn){const pb=scn.bars.find(b=>b[0]===Math.floor((m-240)/2)*2+240);if(pb){lines.push('PRED O '+f(pb[1])+'  H '+f(pb[2]));lines.push('PRED L '+f(pb[3])+'  C '+f(pb[4]));if(pb[6]!=null)lines.push('P(up close) '+(pb[6]*100).toFixed(0)+'%')}}lines.push('median '+f(fc.p50[k]));lines.push('25–75 '+f(fc.p25[k])+' … '+f(fc.p75[k]));lines.push('10–90 '+f(fc.p10[k])+' … '+f(fc.p90[k]));if(fc.actual&&fc.actual[k]!=null)lines.push('actual '+f(fc.actual[k]))}
  tip.textContent=lines.join(String.fromCharCode(10));tip.style.display='block';tip.style.left=Math.min(W-200,e.offsetX+12)+'px';tip.style.top=Math.max(25,e.offsetY+10)+'px'};
 cv.onmouseleave=()=>$('#fc2mTip').style.display='none'}
async function runCompare(force=false){if(!qualifiedTickers.size){$('#compareStatus').textContent='Search a keyword and wait for qualifying tickers to finish screening.';return}if(!await verify())return;const button=$('#compareForm button[type=submit]');button.disabled=true;$('#compareResults').innerHTML='';$('#unifiedProjectionRoot').innerHTML='';$('#forecast2mRoot').innerHTML='';try{$('#compareStatus').textContent='Finding similar qualified event days…';const candidates=await collectCompareCandidates();if(!candidates.length)throw Error('No qualifying keyword-linked events are available.');const payload={user,token,ticker:$('#compareTicker').value.trim().toUpperCase(),date:$('#compareDate').value,window:10,top_k:10,include_own:false,manual_weights:false,force,weights:{A:50,B:25,C:25},candidates};lastComparePayload=payload;const r=await fetch('/api/keyword-compare',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)}),d=await r.json();if(!r.ok)throw Error(d.error||'Compare failed');currentCompareData=d;$('#compareStatus').textContent=`Comparison complete · ${d.matches.length} closest qualifying days from ${qualifiedTickers.size} matching-keyword tickers.`;renderCompare(d);renderTopCompareCharts(d);drawUnifiedProjection(d);drawForecast2m(d)}catch(e){$('#compareStatus').innerHTML=`<span class="error">${esc(e.message)}</span>`}finally{button.disabled=false}}async function runMatcherValidation(){if(!rows.length){$('#compareStatus').textContent='Run a keyword search first.';return}if(!await verify())return;const button=$('#validateMatcher');button.disabled=true;try{$('#compareStatus').textContent='Validating against held-out SEC-associated events and three baselines…';const candidates=await collectCompareCandidates(),weights=currentCompareData?.weights||{A:50,B:25,C:25};const response=await fetch('/api/keyword-validate',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({user,token,window:Number($('#compareWindow').value),mode:$('#validationMode').value,weights,candidates})}),result=await response.json();if(!response.ok)throw Error(result.error||'Validation failed');if(currentCompareData){currentCompareData.validation=result;drawProjectionPanel(currentCompareData)}const names={fade:'HOD-to-close fade (RMSE)',open_close:'Open-to-close sign (hit rate)',d1:'D+1 direction (hit rate)',midday_remaining:'Midday remaining move (RMSE)'},bases=[['matcher','Matcher'],['gap_price_baseline','Gap + price baseline'],['random_keyword_baseline','Random same-keyword'],['scalar_only_baseline','Scalar-only']];let html=`<section class="compareMatch"><h3>Matcher validation · ${esc(result.mode)} · n=${result.sample_size}</h3>${result.warning?`<div class="lowConfidence">${esc(result.warning)}</div>`:'<div class="confidenceGood">Beat each baseline on both continuous measures.</div>'}<div class="compareNote">${esc(result.note)}</div><table class="compareStats"><tr><th>Outcome</th>${bases.map(x=>`<th>${x[1]}</th>`).join('')}</tr>`;for(const [key,label]of Object.entries(names)){html+=`<tr><td>${label}</td>${bases.map(([base])=>{const s=result.metrics[key][base];if(!s.n)return '<td>n=0</td>';return `<td>n=${s.n} · ${s.rmse==null?'':`RMSE ${s.rmse.toFixed(2)} ${s.rmse_ci95?`(95% ${s.rmse_ci95[0].toFixed(2)}–${s.rmse_ci95[1].toFixed(2)}) · `:''}`}hit ${(100*s.hit_rate).toFixed(0)}% (95% ${(100*s.hit_ci95[0]).toFixed(0)}–${(100*s.hit_ci95[1]).toFixed(0)}%)</td>`}).join('')}</tr>`}html+='</table></section>';$('#compareResults').insertAdjacentHTML('beforeend',html);$('#compareStatus').textContent=`Validation finished on ${result.evaluated} held-out events (${result.mode}).`}catch(e){$('#compareStatus').innerHTML=`<span class="error">${esc(e.message)}</span>`}finally{button.disabled=false}}
$('#compareDate').value=isoDate(now);$('#compareTicker').addEventListener('input',e=>e.target.value=e.target.value.toUpperCase());$('#compareForm').onsubmit=e=>{e.preventDefault();runCompare()};async function search(reset=true){if($('#gapFilterEnabled').checked){const gapValue=$('#gapMinPct').value;if(gapValue===''||!Number.isFinite(Number(gapValue))||Number(gapValue)<0||Number(gapValue)>500){setStatus('Enter a gap-up threshold from 0 to 500%.',true);return}}if(!await verify())return;if(reset){eventScreenGeneration++;qualifiedTickers.clear();rows=[];offset=0;kwEventCache.clear();lastComparePayload=null;$('#compareResults').innerHTML='';$('#tableRoot').innerHTML='';$('#stocksRoot').innerHTML=''}const gen=++searchGeneration;$('#moreBtn').disabled=true;$('#moreBtn').textContent='Searching…';setStatus('Searching EDGAR full text; inspecting filing excerpts…');try{const body=payload();const res=await fetch('/api/keyword-search',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}),data=await res.json();if(gen!==searchGeneration)return;if(!res.ok)throw Error(data.error||'SEC search failed');rows=reset?data.rows:rows.concat(data.rows);offset=data.from+data.size;total=data.total;setStatus(`Search complete: ${data.rows.length} filing matches returned on this page.`);buildTable();$('#moreBtn').style.display=offset<total?'inline-block':'none';$('#moreBtn').disabled=false;$('#moreBtn').textContent='Load more';}catch(e){if(gen===searchGeneration){setStatus(e.message,true);$('#moreBtn').disabled=false;$('#moreBtn').textContent='Load more'}}}
$('#searchForm').onsubmit=e=>{e.preventDefault();search(true)};$('#moreBtn').onclick=()=>search(false);
$('#clearBtn').onclick=()=>{searchGeneration++;eventScreenGeneration++;qualifiedTickers.clear();kwEventCache.clear();lastComparePayload=null;$('#compareResults').innerHTML='';rows=[];offset=0;total=0;$('#keyword').value='';$('#tableRoot').innerHTML='';$('#stocksRoot').innerHTML='';$('#resultSummary').innerHTML='';$('#moreBtn').style.display='none';$('#status').textContent='Search SEC filings from the last year. Events require session dollar volume above $50M and an event-day open of $10 or less; the gap-up filter is optional.';$('#startDate').value=isoDate(oneYear);$('#endDate').value=isoDate(now);$('#gapFilterEnabled').checked=false;$('#gapMinPct').value='10';$('#gapMinPct').disabled=true;updateCriteriaLabels()};
$('#keyword').addEventListener('keydown',e=>{if(e.key==='Escape')$('#clearBtn').click()});
</script></body></html>"""


@app.route("/keyword-search")
def keyword_search_page():
    # Inject the exact Gap-Up event-chart renderer and its helpers into this
    # standalone page so both chart screens use the same candle layout logic.
    helper_start = HTML.index("const MC=")
    helper_end = HTML.index("function drawEvAll", helper_start)
    renderer_start = HTML.index("function aggregateEvBars", helper_end)
    renderer_end = HTML.index("\nlet currentModalIndex", renderer_start)
    shared_renderer = ("<script>(function(){\n" + HTML[helper_start:helper_end] +
                       HTML[renderer_start:renderer_end] +
                       "\nwindow.drawKeywordGapEv=drawEv;\n"
                       "window.aggregateKeywordBars=aggregateEvBars;\n})();</script>")
    page = KEYWORD_HTML.replace("<script>", shared_renderer + "<script>", 1)
    return Response(page, mimetype="text/html")

# ----------------------------------------------------------------------------- runner_stats integration
# Load runner_stats.py from the same directory so Multiday Stats runs inside
# this process — no separate server on port 5055 needed.
_rs = None
try:
    import importlib.util as _ilu
    _spec = _ilu.spec_from_file_location(
        "runner_stats",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "runner_stats.py")
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


if __name__ == "__main__":
    _load_env_file()
    POLY_KEY = os.getenv("POLYGON_API_KEY", "")
    if not POLY_KEY:
        print("WARNING: POLYGON_API_KEY not set — historical data will fail until you set it in .env or environment.")
    else:
        print("Polygon API Key loaded successfully.")
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else int(os.getenv("PORT", "8000"))
    print(f"Open http://127.0.0.1:{port}")
    app.run(host="127.0.0.1", port=port, debug=False, threaded=True)
