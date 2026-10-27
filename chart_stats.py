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
import os, re, json, time, math, threading, uuid, warnings, hashlib, secrets, datetime as dt
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
    if d is None:
        end = dt.date.today() + dt.timedelta(days=1)
        start = end - dt.timedelta(days=HISTORY_DAYS)
        j = poly_get(f"/v2/aggs/ticker/{t}/range/1/day/{start}/{end}",
                     {"adjusted": "false", "sort": "asc", "limit": 50000})
        d = [{"date": str(pd.to_datetime(r["t"], unit="ms", utc=True).tz_convert(ET).date()),
              "o": r["o"], "h": r["h"], "l": r["l"], "c": r["c"], "v": round(float(r["v"]))}
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


def minute_df(t, day):
    key = f"{t}_{day}_1m"
    rows = cget(key)
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


def chart_pack(df, m, pc, day):
    """Everything the browser needs for one 2-min chart (bars + VWAP + entry-model signals)."""
    sig = [{"name": k, "m": v["m"], "p": v["p"], "stop_m": v["stop_m"], "pnl": v["pnl"]}
           for k, v in (m.get("models") or {}).items()]
    return {"bars": bars2(df), "pc": pc, "open": m.get("open"), "pmh": m.get("pm_high"),
            "off": ist_off(day), "stop": STOP_PCT, "sig": sig,
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
        "chart": chart_pack(df, m, pc, row["date"]),
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
                     "chart": chart_pack(live["df"], lm, pc, day_obj)})
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
    return send_file(Path(__file__).with_name("favicon.svg"), mimetype="image/svg+xml", max_age=86400)


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
                             "ticker": x.get("ticker"),
                             "time": x.get("time_ist") or x.get("time"),
                             "date": x.get("date_ist")} for x in rows]
            else:
                err_info = f"Supabase HTTP {r.status_code}: {r.text[:200]}"
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
    for visibility_key in (_APP_UI_VISIBILITY_KEY, _APP_UI_VISIBILITY_USER_KEY, _APP_UI_VISIBILITY_DEFAULT_KEY):
        if isinstance(preserved.get(visibility_key), dict):
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
    # Yahoo search is already relevance/recentness ranked. Keep older releases
    # when they are the latest items available, but discard future timestamps.
    unique = [item for item in unique if not published_at(item) or published_at(item) <= now_ts]
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
<link rel="icon" type="image/svg+xml" href="/favicon.svg">
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
.evc{border:1px solid #222;padding:8px 10px;min-width:0;position:relative;cursor:pointer;transition:border-color 0.15s, background 0.15s}
.evc:hover{border-color:#29a8ff;background:#0d0d0d}
.ev_zoom_hint{position:absolute;top:6px;right:8px;background:rgba(18,18,18,0.92);border:1px solid #29a8ff;color:#29a8ff;font-size:11px;padding:2px 7px;border-radius:3px;opacity:0;transition:opacity 0.15s;pointer-events:none;z-index:2}
.evc:hover .ev_zoom_hint{opacity:1}
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
        <span id="cm_badge" style="font-size:12px"></span>
      </div>
      <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap">
        <button id="cm_prev" onclick="prevModalChart()" style="height:32px;font-size:13px;padding:0 12px;background:#1a1a1a;border:1px solid #444;color:#ddd;cursor:pointer;border-radius:3px" title="Previous chart (Left Arrow)">← Prev</button>
        <span id="cm_counter" style="font-size:12px;color:#888;min-width:70px;text-align:center"></span>
        <button id="cm_next" onclick="nextModalChart()" style="height:32px;font-size:13px;padding:0 12px;background:#1a1a1a;border:1px solid #444;color:#ddd;cursor:pointer;border-radius:3px" title="Next chart (Right Arrow)">Next →</button>

        <select id="cm_tf" title="Chart timeframe" style="height:32px;font-size:12px;padding:0 8px;background:#181818;border:1px solid #444;color:#fff;margin:0">
          <option>1m</option><option>2m</option><option>5m</option><option>15m</option>
        </select>

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
      <div style="color:#666;font-size:11px">Use <b>←</b> / <b>→</b> arrow keys to browse charts · <b>Esc</b> to close</div>
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
        publishedDate=/^\d{4}-\d{2}-\d{2}/.test(raw)?raw.slice(0,10):(!Number.isNaN(parsed.getTime())?parsed.toISOString().slice(0,10):'');
      }
      const dateTag=publishedDate?`<span style="display:inline-block;margin-left:8px;padding:1px 6px;border:1px solid #294458;border-radius:3px;background:#0b151c;color:#9fc5dc;font-size:11px;white-space:nowrap">${esc(publishedDate)}</span>`:'';
      return `<div style="padding:7px 0;border-bottom:1px solid #222"><span style="color:#ffd23f;font-size:12px">${esc(item.kind || 'News')}</span> · ${link}<span style="color:#777;font-size:12px"> — ${esc(item.publisher || 'Yahoo Finance')}</span>${dateTag}</div>`;
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
     `<div style="display:flex;align-items:center;flex-wrap:wrap">${esc(T.p6)}<select id="sig"><option value="none" selected>No signals</option><option value="all">All entry models</option>`+
     (r.model_names||[]).map(n=>`<option value="${esc(n)}">${esc(n)}</option>`).join('')+`</select><label style="margin-left:14px;font-size:13px;color:#e8e8e8"><input type="checkbox" id="bz" style="width:auto;height:auto;vertical-align:middle"> Bone zone (9/20 EMA cloud + BB)</label></div>`+
     `<span style="font-size:12px;color:#29a8ff;font-weight:normal">🔍 Click any chart to enlarge</span></div>`+
  `<div class="evgrid">`+tab.EV.map((c,i)=>`<div class="evc" data-ev-slot="${i}" onclick="openChartModal(${i})"><div class="evhead"><div class="evt" id="evtitle${i}"></div><div class="evcontrols"><button class="evnav" data-step="-1" title="Previous chart">‹</button><button class="evnav" data-step="1" title="Next chart">›</button><select class="evtf" title="Timeframe"><option>1m</option><option selected>2m</option><option>5m</option><option>15m</option></select></div></div><canvas id="ev${i}"></canvas></div>`).join('')+
     `<\/div><div class="foot"><span class="c" data-tip="VWAP — Volume Weighted Average Price. Key intraday support/resistance.">━ VWAP<\/span>  <span class="y" data-tip="Pre-market High — highest price before the US market open. Common fade target.">┅ PM high<\/span>  <span class="d" data-tip="Previous Close — yesterday’s close. Gap % is measured from here.">┅ prev close<\/span> &nbsp; <span style="color:#d7ff6e" data-tip="EMA 9 — 9-period EMA on 2-min closes. Fast trend line.">━ EMA9<\/span> <span style="color:#ffa500" data-tip="EMA 20 — 20-period EMA on 2-min closes. Slower trend line. Bearish when EMA9 drops below it.">━ EMA20<\/span> <span class="g" data-tip="Bone Zone cloud — shaded between EMA9 &amp; EMA20. Green = bullish. Red = bearish.">▒<\/span><span class="r">▒<\/span> cloud <span class="d" data-tip="Bollinger Bands (20-period, 2 std dev). Shows expected volatility range on the 2-min chart.">━ BB(20,2)<\/span> &nbsp; <span data-tip="SELL signal — short entry triggered by an entry model.">▼ SELL<\/span> &nbsp; <span data-tip="BUY / Cover signal — close the short here. P&amp;L % shown next to it.">▲ BUY<\/span>. <span data-tip="Lighter shaded background = pre-market session (before 9:30 ET / 19:00 IST).">Shaded = premarket<\/span>. X axis = IST.<\/div><\/div>`;

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
  list.forEach((c,i)=>{
    const card=cEl.querySelector(`[data-ev-slot="${i}"]`), cv=cEl.querySelector('#ev'+i);
    if(!card || !cv) return;
    if(!card._chartItem) card._chartItem=c;
    if(!card._sessionDate) card._sessionDate=card._chartItem.date||c.date;
    if(!card._chartTf) card._chartTf='2m';
    const item=card._chartItem || c;
    const dateList=tabForCharts?tabForCharts.sessionDates:[];
    card._sessionPos=dateList.indexOf(card._sessionDate);
    const title=card.querySelector(`#evtitle${i}`), tf=card.querySelector('.evtf');
    if(title) title.innerHTML=esc(item.title)+(item.sim?'':'<span class="d"> (not in similar set)</span>');
    card.querySelectorAll('.evnav').forEach(btn=>{const next=card._sessionPos+Number(btn.dataset.step);btn.disabled=card._sessionPos<0||next<0||next>=dateList.length;btn.style.opacity=btn.disabled?'.35':'1';});
    if(tf){tf.value=card._chartTf;tf.onclick=e=>e.stopPropagation();tf.onchange=e=>{e.stopPropagation();card._chartTf=tf.value;drawEvAll(list,cEl);};}
    card.querySelectorAll('.evnav').forEach(btn=>{btn.onclick=e=>{e.stopPropagation();navigateEvCard(card,Number(btn.dataset.step),tabForCharts);};});
    drawEv(cv,item.chart,sel,bzChecked,undefined,card._chartTf);
  });
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
  drawEvAll(tab.EV,tab.container);
  if($('#chart_modal').style.display==='flex'&&currentModalSlot===Number(card.dataset.evSlot))renderModalChart();
}
function aggregateEvBars(source, tf){
  if(!source || !source.t || tf===1) return source;
  const out={t:[],o:[],h:[],l:[],c:[],v:[],vw:[]}, groups=new Map();
  for(let i=0;i<source.t.length;i++){
    const start=Math.floor(source.t[i]/tf)*tf;
    if(!groups.has(start)) groups.set(start,[]);
    groups.get(start).push(i);
  }
  for(const [start,ids] of groups){
    out.t.push(start);out.o.push(source.o[ids[0]]);out.h.push(Math.max(...ids.map(i=>source.h[i])));
    out.l.push(Math.min(...ids.map(i=>source.l[i])));out.c.push(source.c[ids[ids.length-1]]);
    out.v.push(ids.reduce((sum,i)=>sum+(source.v[i]||0),0));out.vw.push(source.vw[ids[ids.length-1]]);
  }
  return out;
}
function drawEv(cv, c, sel, bzChecked, targetH, timeframe='2m'){
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
  const tf=parseInt(timeframe,10)||2, b=aggregateEvBars(c.bars,tf);
  if(!b || !b.t.length){
    x.fillStyle = '#8a8a8a';
    x.fillText('No intraday data', 10, 20);
    return;
  }
  const L = isLarge ? 64 : 50, R = 12, T = 10;
  const VH = Math.round(H * 0.18), B = isLarge ? 24 : 18;
  const ph = H - T - VH - B - 6, pw = W - L - R, vy0 = T + ph + 6, vy1 = vy0 + VH;
  const sg = sel === 'none' ? [] : (c.sig || []).filter(s => sel === 'all' || s.name === sel);
  let lo = Math.min(...b.l), hi = Math.max(...b.h);
  const pad = (hi - lo) * 0.05 || hi * 0.02;
  lo -= pad;
  hi += pad;
  const X = m => L + pw * (m - 240) / 720, Y = v => T + ph * (1 - (v - lo) / (hi - lo));
  // Premarket light grey background
  x.fillStyle = 'rgba(255, 255, 255, 0.085)';
  x.fillRect(X(240), T, X(570) - X(240), ph + 6 + VH);
  x.fillStyle = '#8e8e9e';
  x.font = 'bold 10px Consolas,monospace';
  x.fillText('PRE-MARKET', X(240) + 6, T + 13);
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
  for(let m = 240; m <= 960; m += 60){
    const xx = X(m);
    x.beginPath();
    x.moveTo(xx, T);
    x.lineTo(xx, vy1);
    x.stroke();
    x.fillText(hm(m, c.off), xx - 14, H - 5);
  }
  x.strokeStyle = '#777';
  x.setLineDash([2, 3]);
  x.beginPath();
  x.moveTo(X(570), T);
  x.lineTo(X(570), vy1);
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
  dash(c.pc, '#888');
  dash(c.pmh, '#ffd23f');
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
  const bw = Math.max(1.4, pw / (720 / tf) * 0.84), vmax = Math.max(...b.v) || 1;
  for(let i = 0; i < b.t.length; i++){
    const xx = X(b.t[i] + tf / 2), up = b.c[i] >= b.o[i], col = up ? '#3ddc84' : '#ff5a5a';
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
  let on = false;
  for(let i = 0; i < b.t.length; i++){
    const v = b.vw[i];
    if(v == null){ on = false; continue; }
    const xx = X(b.t[i] + tf / 2), yy = Y(Math.min(hi, Math.max(lo, v)));
    if(!on){ x.moveTo(xx, yy); on = true; } else x.lineTo(xx, yy);
  }
  x.stroke();
  x.lineWidth = 1;
  x.fillStyle = '#8a8a8a';
  x.fillText('vol', L + 2, vy0 + 10);
  sg.forEach((s, idx) => {
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
  $('#cm_badge').innerHTML = item.sim 
    ? '<span style="color:#3ddc84;background:#141414;border:1px solid #333;padding:2px 8px;border-radius:2px;font-size:11px">SIMILAR SETUP</span>' 
    : '<span style="color:#777;font-size:11px">(not in similar set)</span>';
  const sessionCount=(tab.sessionDates||[]).length, sessionPos=card?card._sessionPos:-1;
  $('#cm_counter').textContent = `${sessionPos+1} of ${sessionCount}`;
  $('#cm_prev').disabled = (sessionPos <= 0);
  $('#cm_next').disabled = (sessionPos < 0 || sessionPos >= sessionCount-1);
  $('#cm_prev').style.opacity = (sessionPos <= 0) ? '0.35' : '1';
  $('#cm_next').style.opacity = (sessionPos < 0 || sessionPos >= sessionCount-1) ? '0.35' : '1';

  const cv = $('#cm_canvas');
  const body = $('#cm_body');
  const targetH = Math.max(380, (body.clientHeight || 600) - 10);
  const sel = $('#cm_sig').value;
  const bzChecked = $('#cm_bz').checked;

  drawEv(cv, item.chart, sel, bzChecked, targetH, $('#cm_tf').value || (card?card._chartTf:'2m'));
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
    const cardSelect=card.querySelector('.evtf');
    if(cardSelect) cardSelect.value=card._chartTf;
    const item=card._chartItem;
    if(item) drawEv(card.querySelector(`#ev${currentModalSlot}`),item.chart,
      openTabs[activeTab].container.querySelector('#sig')?.value||'none',
      openTabs[activeTab].container.querySelector('#bz')?.checked||false,undefined,card._chartTf);
  }
  renderModalChart();
};

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
      '<table style="margin:0"><thead><tr><th style="width:50px;padding-left:10px">#</th><th style="width:180px;padding-left:8px">Time (IST)</th><th style="padding-left:8px">Ticker</th></tr></thead><tbody>' +
      obj.history.map((h, i) =>
        `<tr><td class="d" style="padding-left:10px">${i+1}</td><td class="w" style="padding-left:8px">${esc(h.time)}</td><td class="big c" style="font-weight:bold;padding-left:8px">${esc(h.ticker)}</td></tr>`
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
      const countLabel = c ? `${c} search${c===1?'':'es'} &nbsp;·&nbsp; ${uniq} unique ticker${uniq===1?'':'s'}` : '0 searches';
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
      `<div class="scroll"><table><thead><tr><th style="width:50px">#</th><th style="width:190px">Time (IST)</th><th style="width:140px">User</th><th>Ticker</th></tr></thead><tbody>`+
      adminRows.map((s, idx)=>{
        return `<tr><td class="d">${idx+1}</td><td class="w">${esc(s.time)}</td><td style="color:#ddd;font-weight:bold">${esc(s.user)}</td><td class="big c">${esc(s.ticker)}</td></tr>`;
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
  } else if($('#chart_modal').style.display === 'flex'){
    if(e.key === 'ArrowLeft'){
      prevModalChart();
    } else if(e.key === 'ArrowRight'){
      nextModalChart();
    }
  }
});

initAuth();
</script></body></html>"""

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
