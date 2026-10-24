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
import os, re, json, time, math, threading, uuid, warnings, datetime as dt
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from flask import Flask, request, jsonify, Response

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
                        if k and k not in os.environ:
                            os.environ[k] = v
        except Exception:
            pass

_load_env_file()

warnings.filterwarnings("ignore")
ET = ZoneInfo("America/New_York")
IST = ZoneInfo("Asia/Kolkata")
POLY_KEY = os.getenv("POLYGON_API_KEY", "")
POLY_RPM = float(os.getenv("POLYGON_RPM", "5"))
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


_rl_lock = threading.Lock()
_last_call = [0.0]


def poly_get(path, params=None):
    global POLY_KEY
    key = POLY_KEY or os.getenv("POLYGON_API_KEY", "")
    if not key:
        _load_env_file()
        key = os.getenv("POLYGON_API_KEY", "")
        POLY_KEY = key
    if not key:
        raise RuntimeError("POLYGON_API_KEY is not set. Set it and restart the app.")
    params = dict(params or {})
    params["apiKey"] = key
    rpm = float(os.getenv("POLYGON_RPM", str(POLY_RPM)))
    gap = 60.0 / rpm
    for _ in range(5):
        with _rl_lock:
            wait = _last_call[0] + gap - time.time()
            if wait > 0:
                time.sleep(wait)
            _last_call[0] = time.time()
        r = requests.get("https://api.polygon.io" + path, params=params, timeout=40)
        if r.status_code == 429:
            time.sleep(20)
            continue
        if r.status_code in (401, 403):
            raise RuntimeError(f"Polygon rejected the request ({r.status_code}): {r.text[:150]}")
        r.raise_for_status()
        return r.json()
    raise RuntimeError("Polygon rate limit hit repeatedly")


def load_daily(t, force=False):
    key = f"{t}_daily2"
    d = None if force else cget(key, 6 * 3600)
    if d is None:
        end = dt.date.today() + dt.timedelta(days=1)
        start = end - dt.timedelta(days=HISTORY_DAYS)
        j = poly_get(f"/v2/aggs/ticker/{t}/range/1/day/{start}/{end}",
                     {"adjusted": "false", "sort": "asc", "limit": 50000})
        d = [{"date": str(pd.to_datetime(r["t"], unit="ms", utc=True).tz_convert(ET).date()),
              "o": r["o"], "h": r["h"], "l": r["l"], "c": r["c"], "v": r["v"]}
             for r in j.get("results", [])]
        cput(key, d)
    return d


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
    return df[["o", "h", "l", "c", "v"]].astype(float)


def minute_df(t, day):
    key = f"{t}_{day}_1m"
    rows = cget(key)
    if rows is None:
        j = poly_get(f"/v2/aggs/ticker/{t}/range/1/minute/{day}/{day}",
                     {"adjusted": "false", "sort": "asc", "limit": 50000})
        rows = j.get("results") or []
        cput(key, rows)
    return poly_frame(rows) if rows else None


# ----------------------------------------------------------------------------- live (Yahoo)
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
    return {"date": day, "df": d, "pc": hist[-1]["c"], "hist": hist,
            "avg20": float(np.mean([r["v"] for r in hist[-20:]])), "cur": float(d["c"].iloc[-1]),
            "float": info.get("floatShares"), "so": info.get("sharesOutstanding"),
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
    """2-minute OHLCV + continuous VWAP, 04:00 -> 16:00 ET, for the per-event charts."""
    mm = df.index.hour * 60 + df.index.minute
    s = df[(mm >= PM_START) & (mm < RG_END)]
    if s.empty:
        return None
    tp = (s.h + s.l + s.c) / 3
    vw = ((tp * s.v).cumsum() / s.v.cumsum().replace(0, np.nan)).ffill()
    b = s.resample("2min").agg({"o": "first", "h": "max", "l": "min", "c": "last", "v": "sum"}).dropna(subset=["o"])
    vb = vw.resample("2min").last().reindex(b.index)
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
        "pm_high": m["pm_high"], "pm_high_pct": (m["pm_high"] / pc - 1) if m["pm_high"] else None,
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
def build_report(t, live, lm, lpath, lctx, events, S, notes):
    simset = {id(e) for e in S}
    st = stats(S)
    g = grade(st)
    pc, cur = live["pc"], live["cur"]
    fl_now = live["float"] or live["so"]
    open_ = lm.get("open")
    vol = lm["pm_vol"] + lm.get("rg_vol", 0)
    dvol = lm["pm_dv"] + lm.get("rg_dv", 0)
    gap_live = ((open_ or cur) / pc - 1)
    rot_live = (vol / fl_now) if fl_now else None
    hod_rot_live = (lm["cum_to_hod"] / fl_now) if (fl_now and lm.get("cum_to_hod")) else rot_live
    mcap = live["mcap"] or ((live["so"] or fl_now or 0) * cur) or None
    pm_rel_live = (lm["pm_vol"] / lctx["avgvol20"]) if lctx.get("avgvol20") else None

    day_obj = live["date"]
    day_str = str(day_obj)
    day_name = day_obj.strftime("%A") if hasattr(day_obj, "strftime") else ""
    day_display = f"{day_str} ({day_name})" if day_name else day_str

    # ---- [1] current day
    pm_fade = (open_ / lm["pm_high"] - 1) if open_ and lm.get("pm_high") else None
    left = [
        ["Current Day (session)", day_display, "c"],
        ["Gap %", pct(gap_live), sc(gap_live)],
        ["Float Rotation (so far)", f"{rot_live:.1f}x" if rot_live else "—", "b"],
        ["Dollar Volume (so far)", "$" + big(dvol), "b"],
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
        evch.append({"title": f"TODAY {day_str}  gap {pct(gap_live, 0)}  (live)", "sim": True,
                     "chart": chart_pack(live["df"], lm, pc, day_obj)})
    for i, e in enumerate(events, 1):
        evch.append({"title": f"#{i}  {fdate(e['date'])}  gap {pct(e['gap'], 0)}  O→C {pct(e['o2c'], 0)}",
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
    chart = {"labels": labels, "paths": [e["path"] for e in S], "med": band(50), "p25": band(25), "p75": band(75),
             "today": lpath, "proj": proj, "now_k": now_k, "open_k": (RG_START - PM_START) // 5}
    keyk = [(0, 240), (66, 570), (72, 600), (84, 660), (96, 720), (120, 840), (144, 960)]
    ptext = []
    for k, mn in keyk:
        m_, a_, b_ = chart["med"][k], chart["p25"][k], chart["p75"][k]
        ptext.append([f"{hhmm(mn, live['date'])} IST  history median / p25..p75 / today / proj",
                      f"{pct(m_, 0)}  /  {pct(a_, 0)}..{pct(b_, 0)}  /  {pct(lpath[k], 0)}  /  {pct(proj[k], 0)}", "w"])

    head.update(ticker=t, n=st["n"], summary=summary,
                sub=f"Chart-Based Gap Up Analysis — Current Day: {day_display} · Times in IST", notes=notes)
    T = {
        "p1": f"[1] CURRENT DAY SETUP ({day_display} — latest data)",
        "p6": "[6] 2-MIN CHARTS — premarket → close, volume + VWAP, SELL = short entry, BUY = cover (stop or close)",
        "p7": "[7] NORMALIZED PATH (% from prev close) — premarket → close, Times in IST",
        "pa": "[A] HISTORICAL STATISTICS (similar)", "pb": "[B] KEY PATTERNS (similar)", "pc": "[C] FINAL RATING",
        "pd": "[D] ENTRY MODELS (similar setups)",
    }
    text = [f"Ticker: {t}", f"Current Day: {day_display}", head["sub"], "=" * 42,
            f"Final Fade Grade:  {head['grade']}   ({head['score']})",
            f"Sample Size: {st['n']} similar setups of {len(events)} gap-ups",
            f"Verdict: {head['verdict']}", f"Summary: {summary}", "",
            T["p1"], kv_text(left), kv_text(right)]
    for tb in tables:
        text += ["", tb["title"], tbl_text(tb["cols"], tb["rows"]), tb["foot"]]
    text += ["", T["p7"], kv_text(ptext, 40), "",
             T["pa"], kv_text(p3), "", T["pb"], kv_text(p4), "", T["pc"], kv_text(p5)]
    if mrows:
        text += ["", T["pd"], tbl_text(mcols, mrows)]
    if notes:
        text += [""] + [f"Note: {n}" for n in notes]

    return {"head": head, "titles": T, "p1": {"left": left, "right": right}, "tables": tables,
            "evcharts": evch, "model_names": ["Short at open", "PM high rejection", "5m low break",
                                              "First red 5m candle", "VWAP loss (post-HOD)"],
            "pa": p3, "pb": p4, "pc": p5, "pd": {"cols": mcols, "rows": mrows},
            "chart": chart, "text": "\n".join(text)}


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


def run_job(job, t, refresh):
    try:
        job["msg"] = "Loading live data (Yahoo)…"
        live = live_snapshot(t)
        job["msg"] = "Loading daily history (Polygon)…"
        daily = [r for r in load_daily(t, refresh) if r["date"] < str(live["date"])]
        splits = load_splits(t)
        typ = ticker_type(t) if (POLY_KEY or os.getenv("POLYGON_API_KEY", "")) else None
        job["msg"] = "Loading shares outstanding history (SEC EDGAR)…"
        sec = load_shares_out(t)
        fl_now = live["float"] or live["so"]
        ratio = None
        if live["float"] and live["so"]:
            ratio = min(1.0, max(0.02, live["float"] / live["so"]))
        ctx = {"sec": sec, "splits": splits, "float": fl_now, "ratio": ratio}

        idxs = detect_events(daily, splits, live["date"])
        events, skipped = [], 0
        for k, i in enumerate(idxs, 1):
            job["msg"] = (f"Fetching 1-min data {k}/{len(idxs)} ({daily[i]['date']}) — cached after first run; "
                          f"free tier ≈ {POLY_RPM:.0f} calls/min")
            e = analyze_event(t, i, daily, ctx)
            if e:
                events.append(e)
            else:
                skipped += 1
        events.sort(key=lambda e: e["date"], reverse=True)

        job["msg"] = "Building report and charts…"
        lm = metrics(live["df"], live["pc"], False)
        lctx = ctx_feats(live["hist"], len(live["hist"]))
        lpath = path_full(live["df"], live["pc"], False)
        lg = ((lm.get("open") or live["cur"]) / live["pc"] - 1)
        lf = {"gap": max(lg, 0.01), "bucket": pbucket(lm.get("open") or live["cur"]), "float": fl_now,
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
        job["result"] = clean(build_report(t, live, lm, lpath, lctx, events, S, notes))
        job["status"] = "done"
    except Exception as ex:
        job["status"] = "error"
        job["error"] = f"{type(ex).__name__}: {ex}"


# ----------------------------------------------------------------------------- web
app = Flask(__name__)


@app.route("/")
def index():
    return Response(HTML, mimetype="text/html")


ALLOWED_USERS = {"karthik", "locke", "gunshow"}
ADMIN_USERS = {"karthik", "locke"}
SEARCH_LOG_FILE = ROOT_DIR / "searches.json"
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
_search_log_lock = threading.Lock()


def _supabase_headers():
    k = os.getenv("SUPABASE_KEY", "") or SUPABASE_KEY
    return {
        "apikey": k,
        "Authorization": f"Bearer {k}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }


def log_search(user, ticker):
    if not user or not ticker:
        return
    now_dt = dt.datetime.now(IST)
    now_ist = now_dt.strftime("%Y-%m-%d %H:%M:%S")
    now_date = now_dt.strftime("%Y-%m-%d")
    entry = {"user": user.upper(), "ticker": ticker, "time": now_ist, "date": now_date}

    # 1. Local searches.json file (reliable fallback)
    with _search_log_lock:
        try:
            data = []
            if SEARCH_LOG_FILE.exists():
                try:
                    data = json.loads(SEARCH_LOG_FILE.read_text(encoding="utf-8"))
                except Exception:
                    data = []
            data.insert(0, entry)
            data = data[:2000]
            SEARCH_LOG_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
        except Exception:
            pass

    # 2. Supabase Cloud Database (if configured)
    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    if sub_url and sub_key:
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
                # Fallback if date_ist column is not yet created in table
                requests.post(endpoint, json={"user_name": user.upper(), "ticker": ticker, "time_ist": now_ist}, headers=_supabase_headers(), timeout=5)
        except Exception:
            pass


@app.route("/api/start")
def start():
    u = (request.args.get("user") or "").strip().lower()
    if u not in ALLOWED_USERS:
        return jsonify({"error": "Access denied"}), 401
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
    if sub_url and sub_key:
        try:
            endpoint = f"{sub_url}/rest/v1/searches?select=user_name,ticker,time_ist,date_ist&order=id.desc&limit=1000"
            r = requests.get(endpoint, headers=_supabase_headers(), timeout=5)
            if r.status_code == 200:
                rows = r.json()
                if isinstance(rows, list) and len(rows):
                    data = [{"user": (x.get("user_name") or x.get("user") or "").upper(), "ticker": x.get("ticker"), "time": x.get("time_ist") or x.get("time"), "date": x.get("date_ist")} for x in rows]
                    return jsonify({"searches": data})
        except Exception:
            pass

    data = []
    with _search_log_lock:
        if SEARCH_LOG_FILE.exists():
            try:
                data = json.loads(SEARCH_LOG_FILE.read_text(encoding="utf-8"))
            except Exception:
                data = []
    return jsonify({"searches": data})


@app.route("/api/job/<jid>")
def job(jid):
    j = JOBS.get(jid)
    if not j:
        return jsonify({"status": "error", "error": "Unknown job"}), 404
    return jsonify(j)


HTML = r"""<!doctype html>
<html><head><meta charset="utf-8"><title>Gap-Up Fade Analyzer</title>
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
.evc{border:1px solid #222;padding:6px 8px;min-width:0}
.evt{font-size:13px;margin-bottom:2px}
.tag{border:1px solid var(--y);color:var(--y);padding:0 6px;font-size:11px;margin-left:8px}
@media(max-width:1100px){.three{grid-template-columns:1fr}.two{grid-template-columns:1fr}.two>div+div{border-left:0;padding-left:14px}.evgrid{grid-template-columns:1fr}}
</style></head><body>
<!-- Access Gate Modal -->
<div id="gate" style="position:fixed;inset:0;background:#000;z-index:9999;display:flex;align-items:center;justify-content:center;padding:20px">
  <div style="background:#0c0c0c;border:1px solid #333;padding:36px 32px;width:380px;max-width:92vw;text-align:center;box-shadow:0 12px 40px rgba(0,0,0,0.95)">
    <div style="font-size:22px;font-weight:bold;color:#29a8ff;margin-bottom:8px">Gap-Up Fade Terminal</div>
    <div style="color:#8a8a8a;font-size:14px;margin-bottom:24px">Authorized access only. Enter your name:</div>
    <input id="gate_name" placeholder="Enter name" style="width:100%;height:46px;font-size:17px;margin-bottom:14px;padding:0 14px;background:#000;border:1px solid #444;color:#fff;text-align:center" autofocus spellcheck="false" autocomplete="off">
    <div style="display:flex;align-items:center;justify-content:center;gap:8px;margin-bottom:22px;font-size:13px;color:#aaa">
      <input type="checkbox" id="gate_rem" checked style="width:16px;height:16px;cursor:pointer">
      <label for="gate_rem" style="cursor:pointer">Remember my device</label>
    </div>
    <button id="gate_btn" style="width:100%;height:46px;font-size:16px;background:#141414;border:1px solid #29a8ff;color:#29a8ff;cursor:pointer;font-weight:bold">Enter Terminal</button>
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
    <div style="margin-left:auto;display:flex;align-items:center;gap:12px;font-size:13px;color:#8a8a8a">
      <span>User: <span id="u_name" style="color:#29a8ff;font-weight:bold"></span><span id="u_badge"></span></span>
      <button id="logout_btn" style="height:34px;font-size:12px;padding:0 12px;border-color:#444">Exit</button>
    </div>
  </div>
  <!-- Multi-Ticker Tab Bar -->
  <div id="tab_bar" style="display:none;padding:8px 24px 0;background:#050505;border-bottom:1px solid #222;gap:6px;overflow-x:auto;align-items:center;flex-wrap:nowrap"></div>
  <div id="tabs_view">
    <div id="out" style="padding:24px 34px"><span class="d">Enter a ticker that is gapping up 30%+ and press Get notes.</span></div>
  </div>
</div>

<!-- Admin Search History View -->
<div id="admin_view" style="display:none;padding:24px 34px">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;flex-wrap:wrap;gap:16px;border-bottom:1px solid #333;padding-bottom:16px">
    <div>
      <div class="big" style="color:#29a8ff;font-size:20px">Searched Symbols Audit</div>
      <div style="color:#8a8a8a;font-size:13px;margin-top:4px">View search history grouped by user or by recent tickers (IST)</div>
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
      <button id="admin_back_btn" style="height:36px;font-size:13px;padding:0 14px;border-color:#29a8ff;color:#29a8ff">Back to Terminal</button>
    </div>
  </div>
  <div id="admin_list" style="margin-top:10px"></div>
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
</div>
<script>
const $=s=>document.querySelector(s);
const ALLOWED = ['karthik', 'locke', 'gunshow'];
const PWD_USERS = ['karthik', 'locke'];
let currentUser = localStorage.getItem('gap_user') || '';

function checkAuth(){
  const cur = (currentUser || '').toLowerCase();
  if(currentUser && ALLOWED.includes(cur)){
    $('#gate').style.display='none';
    $('#admin_view').style.display='none';
    $('#app_main').style.display='block';
    $('#u_name').textContent=currentUser.toUpperCase();
    if(PWD_USERS.includes(cur)){
      $('#u_name').style.cursor='pointer';
      $('#u_name').style.textDecoration='underline dotted';
    } else {
      $('#u_name').style.cursor='default';
      $('#u_name').style.textDecoration='none';
    }
    $('#t').focus();
    return true;
  }
  $('#gate').style.display='flex';
  $('#app_main').style.display='none';
  $('#admin_view').style.display='none';
  $('#gate_name').focus();
  return false;
}

function submitGate(){
  const val = ($('#gate_name').value||'').trim().toLowerCase();
  if(!ALLOWED.includes(val)){
    $('#gate_err').textContent='Access denied.';
    $('#gate_err').style.display='block';
    return;
  }
  $('#gate_err').style.display='none';
  currentUser = val;
  if($('#gate_rem').checked){
    localStorage.setItem('gap_user', val);
  } else {
    localStorage.removeItem('gap_user');
  }
  checkAuth();
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
  if(tab && tab.lastChart){
    const cv = tab.container.querySelector('#cv');
    if(cv) drawChart(tab.lastChart, cv);
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
    const r = await fetch('/api/start?ticker=' + encodeURIComponent(t) + '&user=' + encodeURIComponent(currentUser) + (refresh ? '&refresh=1' : ''));
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
  tab.EV = r.evcharts || [];
  tab.result = r;

  const h = r.head, T = r.titles;
  let o = '';

  const nowIstStr = new Date().toLocaleTimeString('en-US', {timeZone: 'Asia/Kolkata', hour12: false});
  const refreshBadge = isRefresh
    ? `<span style="background:#161616;border:1px solid #333;color:#3ddc84;font-size:11px;padding:3px 9px;border-radius:3px;margin-left:12px;font-weight:normal">● LIVE DAY STATS UPDATED: ${nowIstStr} IST</span>`
    : '';

  o += `<div class="refresh_banner" style="display:none;background:#141414;border:1px solid #29a8ff;padding:8px 12px;margin-bottom:14px;color:#29a8ff;font-size:13px"></div>`;
  o += `<div style="display:flex;align-items:center;flex-wrap:wrap"><span class="big">Ticker: <span class="c">${esc(h.ticker)}</span></span>${refreshBadge}</div><div class="w">${esc(h.sub)}</div><div>==========================================</div><br>`;
  o += `<div>Final Fade Grade:<span class="badge ${h.gcls}">${esc(h.grade)}</span>(${esc(h.score)})</div>`;
  o += `<div>Similar Setups: <span class="c">${h.n}</span> (${h.total ? 'of ' + h.total + ' gap-ups' : ''})</div>`;
  o += `<div>Verdict: <span class="${h.gcls}">${esc(h.verdict)}</span></div>`;
  o += `<div>Summary: ${esc(h.summary)}</div>`;
  (h.notes||[]).forEach(n=>o+=`<div class="y">Note: ${esc(n)}</div>`);
  o += `<div class="panel two"><div><div class="ttl">${esc(T.p1)}</div>${kv(r.p1.left)}</div><div>${kv(r.p1.right)}</div></div>`;
  (r.tables||[]).forEach(t=>{
    o += `<div class="panel"><div class="ttl">${esc(t.title)}</div>${table(t.cols,t.rows,t.dim,true)}<div class="foot">${esc(t.foot||'')}</div></div>`;
  });
  o += `<div class="panel"><div class="ttl">${esc(T.p6)}<select id="sig"><option value="none" selected>No signals</option><option value="all">All entry models</option>`+
     (r.model_names||[]).map(n=>`<option value="${esc(n)}">${esc(n)}</option>`).join('')+`</select><label style="margin-left:14px;font-size:13px;color:#e8e8e8"><input type="checkbox" id="bz" style="width:auto;height:auto;vertical-align:middle"> Bone zone (9/20 EMA cloud + BB)</label></div>`+
     `<div class="evgrid">`+tab.EV.map((c,i)=>`<div class="evc"><div class="evt">${esc(c.title)}${c.sim?'':'<span class="d"> (not in similar set)</span>'}</div><canvas id="ev${i}"></canvas></div>`).join('')+
     `</div><div class="foot"><span class="c">━ VWAP</span>  <span class="y">┅ PM high</span>  <span class="d">┅ prev close</span> &nbsp; <span style="color:#d7ff6e">━ EMA9</span> <span style="color:#ffa500">━ EMA20</span> <span class="g">▒</span><span class="r">▒</span> cloud (green = 9 above 20) <span class="d">━ BB(20,2)</span> &nbsp; ▼ SELL (short entry) &nbsp; ▲ BUY (cover at stop or close, with P&amp;L). Shaded = premarket. X axis = IST.</div></div>`;
  o += `<div class="panel"><div class="ttl">${esc(T.p7)}</div><canvas id="cv"></canvas>`+
     `<div class="foot"><span class="b">━ history median</span> &nbsp; <span style="color:#6c8">▒ p25–p75</span> &nbsp; <span class="y">━ today</span> &nbsp; <span class="y">┅ today projected (history re-based at now)</span> &nbsp; <span class="d">━ each similar setup</span> &nbsp; Y = % from previous close</div></div>`;
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
  list.forEach((c,i)=>{const cv=cEl.querySelector('#ev'+i); if(cv) drawEv(cv,c.chart,sel,bzChecked);});
}
function drawEv(cv,c,sel,bzChecked){
  const dpr=window.devicePixelRatio||1, W=Math.max(300,cv.parentElement.clientWidth-16), H=330;
  cv.style.width=W+'px'; cv.style.height=H+'px'; cv.width=W*dpr; cv.height=H*dpr;
  const x=cv.getContext('2d'); x.setTransform(dpr,0,0,dpr,0,0); x.clearRect(0,0,W,H); x.font='11px Consolas,monospace';
  const b=c.bars; if(!b||!b.t.length){x.fillStyle='#8a8a8a';x.fillText('No intraday data',10,20);return;}
  const L=50,R=8,T=8,VH=58,B=18, ph=H-T-VH-B-6, pw=W-L-R, vy0=T+ph+6, vy1=vy0+VH;
  const sg=sel==='none'?[]:(c.sig||[]).filter(s=>sel==='all'||s.name===sel);
  let lo=Math.min(...b.l), hi=Math.max(...b.h);
  const pad=(hi-lo)*0.05||hi*0.02; lo-=pad; hi+=pad;
  const X=m=>L+pw*(m-240)/720, Y=v=>T+ph*(1-(v-lo)/(hi-lo));
  x.fillStyle='rgba(255,255,255,0.04)'; x.fillRect(X(240),T,X(570)-X(240),ph+6+VH);
  x.lineWidth=1; x.strokeStyle='#1f1f1f'; x.fillStyle='#8a8a8a';
  for(let k=0;k<=4;k++){const v=lo+(hi-lo)*k/4,y=Y(v);x.beginPath();x.moveTo(L,y);x.lineTo(W-R,y);x.stroke();x.fillText(v>=1?v.toFixed(2):v.toFixed(3),2,y+4);}
  for(let m=240;m<=960;m+=60){const xx=X(m);x.beginPath();x.moveTo(xx,T);x.lineTo(xx,vy1);x.stroke();x.fillText(hm(m,c.off),xx-14,H-5);}
  x.strokeStyle='#777'; x.setLineDash([2,3]); x.beginPath(); x.moveTo(X(570),T); x.lineTo(X(570),vy1); x.stroke(); x.setLineDash([]);
  const dash=(v,col)=>{if(v==null||v<lo||v>hi)return;x.strokeStyle=col;x.setLineDash([4,4]);x.beginPath();x.moveTo(L,Y(v));x.lineTo(W-R,Y(v));x.stroke();x.setLineDash([]);};
  dash(c.pc,'#888'); dash(c.pmh,'#ffd23f');
  // ---- bone zone: 9/20 EMA cloud + Bollinger Bands (20, 2) on the 2-min closes, continuous from 04:00 ET
  const BZ=bzChecked?bone(b):null;
  const clipP=()=>{x.save();x.beginPath();x.rect(L,T,pw,ph);x.clip();};
  const poly=(a,col,w)=>{x.strokeStyle=col;x.lineWidth=w;x.beginPath();let on=false;for(let i=0;i<a.length;i++){if(a[i]==null){on=false;continue;}const xx=X(b.t[i]+1),yy=Y(a[i]);if(!on){x.moveTo(xx,yy);on=true}else x.lineTo(xx,yy);}x.stroke();x.lineWidth=1;};
  if(BZ){clipP();
    for(let i=1;i<b.t.length;i++){x.fillStyle=BZ.e9[i]>BZ.e20[i]?'rgba(0,255,0,0.30)':'rgba(255,0,0,0.30)';
      x.beginPath();x.moveTo(X(b.t[i-1]+1),Y(BZ.e9[i-1]));x.lineTo(X(b.t[i]+1),Y(BZ.e9[i]));x.lineTo(X(b.t[i]+1),Y(BZ.e20[i]));x.lineTo(X(b.t[i-1]+1),Y(BZ.e20[i-1]));x.closePath();x.fill();}
    poly(BZ.bu,'#7a7a7a',1);poly(BZ.bl,'#7a7a7a',1);x.restore();}
  const bw=Math.max(1,pw/360*0.72), vmax=Math.max(...b.v)||1;
  for(let i=0;i<b.t.length;i++){
    const xx=X(b.t[i]+1), up=b.c[i]>=b.o[i], col=up?'#3ddc84':'#ff5a5a';
    x.fillStyle=up?'rgba(61,220,132,0.55)':'rgba(255,90,90,0.55)';
    const vh=VH*b.v[i]/vmax; x.fillRect(xx-bw/2,vy1-vh,bw,vh);
    x.strokeStyle=col; x.beginPath(); x.moveTo(xx,Y(b.h[i])); x.lineTo(xx,Y(b.l[i])); x.stroke();
    x.fillStyle=col; const yt=Y(Math.max(b.o[i],b.c[i])); x.fillRect(xx-bw/2,yt,bw,Math.max(1,Math.abs(Y(b.o[i])-Y(b.c[i]))));
  }
  if(BZ){clipP();poly(BZ.e9,'rgb(215,255,110)',1.2);poly(BZ.e20,'rgb(255,165,0)',1.2);x.restore();}
  x.strokeStyle='#29a8ff'; x.lineWidth=1.3; x.beginPath(); let on=false;
  for(let i=0;i<b.t.length;i++){const v=b.vw[i]; if(v==null){on=false;continue;} const xx=X(b.t[i]+1),yy=Y(Math.min(hi,Math.max(lo,v))); if(!on){x.moveTo(xx,yy);on=true}else x.lineTo(xx,yy);} x.stroke(); x.lineWidth=1;
  x.fillStyle='#8a8a8a'; x.fillText('vol',L+2,vy0+10);
  sg.forEach((s,idx)=>{
    const col=MC[s.name]||'#fff', xs=X(s.m), ys=Y(s.p); let xm,ym,pl;
    if(s.stop_m!=null){xm=X(s.stop_m);ym=Y(s.p*(1+c.stop));pl=-c.stop;}
    else if(c.cover){xm=X(c.cover.m);ym=Y(c.cover.p);pl=s.pnl;} else return;
    x.strokeStyle=col; x.setLineDash([3,3]); x.beginPath(); x.moveTo(xs,ys); x.lineTo(xm,ym); x.stroke(); x.setLineDash([]);
    tri(x,xs,ys,'down',col); tri(x,xm,ym,'up','#3ddc84');
    x.fillStyle=col; x.fillText('SELL '+(MA[s.name]||''),xs+6,ys-8-idx*0);
    x.fillStyle=pl>=0?'#3ddc84':'#ff5a5a'; x.fillText('BUY '+(pl*100>=0?'+':'')+(pl*100).toFixed(0)+'%',xm+6,ym+16);
  });
}

function drawChart(ch, cvElement){
  const cv = cvElement || (activeTab && openTabs[activeTab] ? openTabs[activeTab].container.querySelector('#cv') : $('#cv'));
  if(!cv) return;
  const dpr=window.devicePixelRatio||1, W=cv.parentElement.clientWidth-28, H=400;
  cv.style.width=W+'px'; cv.style.height=H+'px'; cv.width=W*dpr; cv.height=H*dpr;
  const x=cv.getContext('2d'); x.setTransform(dpr,0,0,dpr,0,0); x.font='12px Consolas,monospace';
  const L=52,R=14,T=12,B=26, pw=W-L-R, ph=H-T-B, n=ch.labels.length;
  const vs=[0]; [ch.med,ch.p25,ch.p75,ch.today,ch.proj,...ch.paths].forEach(a=>a.forEach(v=>{if(v!=null)vs.push(v)}));
  let lo=Math.max(Math.min(...vs),-0.95), hi=Math.min(Math.max(...vs),10);
  if(hi-lo<0.1){hi+=0.05;lo-=0.05}
  const X=i=>L+pw*i/(n-1), Y=v=>T+ph*(1-(Math.max(lo,Math.min(hi,v))-lo)/(hi-lo));
  x.strokeStyle='#222'; x.fillStyle='#8a8a8a'; x.lineWidth=1;
  for(let k=0;k<=5;k++){const v=lo+(hi-lo)*k/5, y=Y(v); x.beginPath();x.moveTo(L,y);x.lineTo(W-R,y);x.stroke(); x.fillText((v*100).toFixed(0)+'%',4,y+4);}
  for(let i=0;i<n;i+=6){const xx=X(i); x.beginPath();x.moveTo(xx,T);x.lineTo(xx,T+ph);x.stroke(); x.fillText(ch.labels[i],xx-14,H-8);}
  x.strokeStyle='#666'; x.beginPath(); x.moveTo(L,Y(0)); x.lineTo(W-R,Y(0)); x.stroke();
  x.strokeStyle='#777'; x.setLineDash([2,3]); x.beginPath(); x.moveTo(X(ch.open_k),T); x.lineTo(X(ch.open_k),T+ph); x.stroke(); x.setLineDash([]);
  x.fillStyle='#777'; x.fillText('US open '+ch.labels[ch.open_k],X(ch.open_k)+4,T+12);
  const line=(a,col,w,dash)=>{x.strokeStyle=col;x.lineWidth=w;x.setLineDash(dash||[]);x.beginPath();let on=false;a.forEach((v,i)=>{if(v==null){on=false;return;} if(!on){x.moveTo(X(i),Y(v));on=true}else x.lineTo(X(i),Y(v));});x.stroke();x.setLineDash([]);};
  ch.paths.forEach(p=>line(p,'rgba(120,160,200,0.20)',1));
  x.fillStyle='rgba(108,196,255,0.14)'; x.beginPath(); let started=false;
  for(let i=0;i<n;i++){ if(ch.p75[i]==null) continue; if(!started){x.moveTo(X(i),Y(ch.p75[i]));started=true}else x.lineTo(X(i),Y(ch.p75[i])); }
  for(let i=n-1;i>=0;i--){ if(ch.p25[i]==null) continue; x.lineTo(X(i),Y(ch.p25[i])); }
  x.closePath(); x.fill();
  line(ch.med,'#6cc4ff',2.5);
  line(ch.proj,'#ffd23f',2,[6,5]);
  line(ch.today,'#ffd23f',2.5);
  if(ch.now_k!=null&&ch.today[ch.now_k]!=null){x.fillStyle='#ffd23f';x.beginPath();x.arc(X(ch.now_k),Y(ch.today[ch.now_k]),4,0,7);x.fill();}
}
window.addEventListener('resize',()=>{
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
$('#logout_btn').onclick=()=>{
  localStorage.removeItem('gap_user');
  currentUser='';
  $('#gate_name').value='';
  $('#u_badge').innerHTML='';
  checkAuth();
};

let adminPwd = '';

function openPwdModal(){
  const cur = (currentUser||'').toLowerCase();
  if(!PWD_USERS.includes(cur)){
    return;
  }
  if(adminPwd === '500'){
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
      '<div class="scroll" style="max-height:260px;background:#050505;border:1px solid #222;border-radius:3px">' +
      '<table style="margin:0"><thead><tr><th style="width:40px">#</th><th style="width:180px">Time (IST)</th><th>Ticker</th></tr></thead><tbody>' +
      obj.history.map((h, i) =>
        `<tr><td class="d">${i+1}</td><td class="w">${esc(h.time)}</td><td class="big c" style="font-weight:bold">${esc(h.ticker)}</td></tr>`
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
    const users = ['KARTHIK', 'LOCKE', 'GUNSHOW'];

    let html = '<div style="margin-bottom:12px;font-size:13px;color:#8a8a8a">Select a user to view what symbols they have searched:</div>';
    html += '<div style="display:grid;grid-template-columns:repeat(auto-fit, minmax(200px, 1fr));gap:12px;margin-bottom:26px">';

    for(const u of users){
      html += `<div onclick="openUserPopup('${esc(u)}')" style="background:#0e0e0e;border:1px solid #282828;padding:18px 20px;border-radius:4px;cursor:pointer;display:flex;justify-content:space-between;align-items:center;transition:background 0.15s, border-color 0.15s;user-select:none" onmouseover="this.style.background='#161616';this.style.borderColor='#444'" onmouseout="this.style.background='#0e0e0e';this.style.borderColor='#282828'">`+
        `<span style="font-size:16px;font-weight:bold;color:#f0f0f0">${esc(u)}</span>`+
        `<span style="font-size:12px;color:#888">View →</span>`+
      `</div>`;
    }
    html += '</div>';

    // Full chronological log below with clean neutral colors
    html += `<div class="panel"><div class="ttl" style="margin-bottom:10px">All Searches Chronological Log</div>`+
      `<div class="scroll"><table><thead><tr><th style="width:50px">#</th><th style="width:190px">Time (IST)</th><th style="width:140px">User</th><th>Ticker</th></tr></thead><tbody>`+
      adminRows.map((s, idx)=>{
        return `<tr><td class="d">${idx+1}</td><td class="w">${esc(s.time)}</td><td style="color:#ddd;font-weight:bold">${esc(s.user)}</td><td class="big c">${esc(s.ticker)}</td></tr>`;
      }).join('')+
      `</tbody></table></div></div>`;

    container.innerHTML = html;

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
    renderAdminView();
  } catch(e) {
    $('#admin_list').innerHTML = `<span class="r">Failed to load search logs: ${esc(e)}</span>`;
  }
}

async function verifyPwd(){
  const cur = (currentUser||'').toLowerCase();
  const pwd = ($('#pwd_input').value||'').trim();
  if(!PWD_USERS.includes(cur)){
    return;
  }
  if(pwd === '500'){
    adminPwd = '500';
    $('#pwd_err').style.display='none';
    $('#pwd_modal').style.display='none';
    $('#u_badge').innerHTML=' <span style="color:#3ddc84;font-size:11px;border:1px solid #3ddc84;padding:1px 6px;margin-left:4px;border-radius:2px;cursor:pointer" title="Click to view User Searches">✓ VERIFIED</span>';
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
$('#admin_view_mode').onchange=(e)=>{
  activeViewMode = e.target.value;
  renderAdminView();
};

window.addEventListener('keydown', e => {
  if(e.key === 'Escape'){
    closeUserPopup();
    if($('#pwd_modal').style.display === 'flex'){
      $('#pwd_modal').style.display = 'none';
    }
  }
});

checkAuth();
</script></body></html>"""


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