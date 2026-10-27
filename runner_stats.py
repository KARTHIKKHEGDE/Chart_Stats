#!/usr/bin/env python3
"""
runner_stats.py  -  Rally + First Red Day (FRD) stats, single file, runs like a website.

    pip install flask requests python-dotenv
    python runner_stats.py        ->  http://127.0.0.1:5055

Key is read from .env next to this file (or the environment):  POLYGON_API_KEY=xxxx
"""
import os, time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from flask import Flask, request, jsonify, Response

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
    load_dotenv()
except ImportError:
    pass

API_KEY = os.getenv("POLYGON_API_KEY") or os.getenv("POLYGON_KEY")
BASE = "https://api.polygon.io"
ET = ZoneInfo("America/New_York")   # used for market session classification (do not change)
IST = ZoneInfo("Asia/Kolkata")       # used for all displayed timestamps
SEC_UA = os.getenv("SEC_USER_AGENT", "RunnerStats admin@example.com")  # SEC EDGAR requires a contact header

# ---------------- tunables ----------------
LOOKBACK_DAYS = 365      # daily history pulled
RALLY_WIN = 90           # max trading days a rally low can sit before its high
MIN_RUN = 1.0            # 100% low->high to count as a rally candidate
RECENT_BARS = 120        # prefer rallies whose high is within the last N bars
BASE_MIN_DAYS = 3        # a base = >=3 days w/o a new high ...
BASE_MIN_DEPTH = 0.10    # ... and >=10% pullback from the running high, then a new high
RALLY_MAX_DAYS = 120     # when you give a start date: look this far for the high
PIVOT_N = 6              # 15min swing detection: bars required on each side of a pivot high
MIN_LEG_PCT = 0.60       # minimum % gain per structural leg (smaller wiggles get merged forward)

app = Flask(__name__)
S = requests.Session()
_aggs_cache = {}


# ---------------- polygon helpers ----------------
def pg(path, params=None, tries=4):
    if not API_KEY:
        raise RuntimeError("POLYGON_API_KEY not found in .env / environment")
    url = path if path.startswith("http") else BASE + path
    p = dict(params or {})
    p["apiKey"] = API_KEY
    for t in range(tries):
        r = S.get(url, params=p, timeout=30)
        if r.status_code == 429:
            time.sleep(2 + 3 * t)
            continue
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    raise RuntimeError("Polygon rate limit hit, try again in a minute")


def _alpaca_live_df(t):
    key = os.getenv("ALPACA_API_KEY")
    sec = os.getenv("ALPACA_API_SECRET")
    if not (key and sec):
        return None
    try:
        now_et = datetime.now(ET)
        today_start_et = datetime(now_et.year, now_et.month, now_et.day, 4, 0, tzinfo=ET)
        start_utc = today_start_et.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
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
                    dt_et = datetime.fromisoformat(b["t"].replace("Z", "+00:00")).astimezone(ET)
                    rows.append({"Datetime": dt_et, "o": float(b["o"]), "h": float(b["h"]), "l": float(b["l"]), "c": float(b["c"]), "v": float(b["v"])})
                import pandas as pd
                df = pd.DataFrame(rows).set_index("Datetime")
                if not df.empty and df["v"].sum() > 0:
                    # Calibrate volume to match TC2000 / Nasdaq Basic (FINRA TRF "D" + Nasdaq "Q")
                    try:
                        r1 = requests.get(f"https://data.alpaca.markets/v2/stocks/{t.upper()}/trades?start={start_utc}&limit=10000&feed={feed}", headers=headers, timeout=5).json()
                        r2 = requests.get(f"https://data.alpaca.markets/v2/stocks/{t.upper()}/trades?limit=10000&feed={feed}&sort=desc", headers=headers, timeout=5).json()
                        all_t = (r1.get("trades") or []) + (r2.get("trades") or [])
                        if all_t:
                            tc_v = sum(x.get("s", 0) for x in all_t if x.get("x") in ("D", "Q"))
                            tot_v = sum(x.get("s", 0) for x in all_t)
                            if tot_v > 0 and 0 < tc_v <= tot_v:
                                tc_ratio = tc_v / tot_v
                                df["v"] = (df["v"] * tc_ratio).round()
                    except Exception:
                        pass
                    return df
    except Exception:
        pass
    return None


def aggs(tkr, mult, span, frm, to, adjusted="false"):
    k = (tkr, mult, span, str(frm), str(to), str(adjusted))
    if k in _aggs_cache:
        return _aggs_cache[k]
    j = pg(f"/v2/aggs/ticker/{tkr}/range/{mult}/{span}/{frm}/{to}",
           {"adjusted": str(adjusted).lower(), "sort": "asc", "limit": 50000})
    res = (j or {}).get("results") or []
    for b in res:
        if "v" in b:
            b["v"] = round(float(b["v"]))
    _aggs_cache[k] = res
    return res


def details(tkr, date=None):
    try:
        j = pg(f"/v3/reference/tickers/{tkr}", {"date": date} if date else None)
        return (j or {}).get("results") or {}
    except Exception:
        return {}


def poly_float(tkr):
    """Polygon GET /stocks/vX/float  (experimental endpoint, current float only).
    Returns (free_float, effective_date, free_float_percent, error)."""
    try:
        j = pg(f"{BASE}/stocks/vX/float", {"ticker": tkr})
        res = (j or {}).get("results") or []
        if res and res[0].get("free_float"):
            r = res[0]
            return float(r["free_float"]), r.get("effective_date"), r.get("free_float_percent"), None
        return None, None, None, "no float returned for " + tkr
    except Exception as e:
        return None, None, None, str(e)


def load_shares_out(tkr):
    """
    Pull EntityCommonStockSharesOutstanding from SEC EDGAR XBRL company facts.
    Returns a list of {filed, end, val} dicts sorted by filing date (oldest first).
    This gives a point-in-time history of shares outstanding from public filings.
    """
    out = []
    try:
        h = {"User-Agent": SEC_UA}
        # Step 1: map ticker -> CIK
        raw = S.get("https://www.sec.gov/files/company_tickers.json", headers=h, timeout=25).json()
        ticker_map = {v["ticker"].upper(): v["cik_str"] for v in raw.values()}
        cik = ticker_map.get(tkr.upper())
        if not cik:
            return []
        # Step 2: fetch all XBRL facts for this company
        facts = S.get(
            f"https://data.sec.gov/api/xbrl/companyfacts/CIK{int(cik):010d}.json",
            headers=h, timeout=40
        ).json()
        rows = facts["facts"]["dei"]["EntityCommonStockSharesOutstanding"]["units"]["shares"]
        out = sorted(
            [{"filed": r["filed"], "end": r["end"], "val": r["val"]}
             for r in rows if r.get("val", 0) > 0],
            key=lambda r: r["filed"]
        )
    except Exception:
        out = []
    return out


def load_splits(tkr):
    """Polygon split history for a ticker. Returns list of {date, from, to} dicts."""
    try:
        j = pg(f"/v3/reference/splits", {"ticker": tkr, "limit": 1000})
        results = (j or {}).get("results") or []
        return [{"date": r["execution_date"], "from": r["split_from"], "to": r["split_to"]}
                for r in results]
    except Exception:
        return []


def float_at_date(day_str, sec_rows, splits, poly_ratio):
    """
    Reconstruct the free float as it existed on `day_str` (YYYY-MM-DD).

    Algorithm (mirrors chart_stats.py float_at):
      1. Find the last SEC filing whose `filed` date <= day_str  ->  shares outstanding at that time.
      2. Forward-adjust for any stock splits between the filing date and day_str.
      3. Multiply by poly_ratio (free_float / shares_out from Polygon today) to convert
         shares-outstanding -> free float.  If ratio unavailable, use shares-out as proxy.
      4. If no SEC filing predates day_str, fall back to Polygon current float back-adjusted
         for splits that happened after day_str.

    Returns (float_value, filing_date_str, stale_bool)
      stale=True when the filing is >60 days old relative to day_str.
    """
    from datetime import date as _date
    d = _date.fromisoformat(day_str)
    best = None
    for r in sec_rows:          # already sorted oldest -> newest
        if r["filed"] <= day_str:
            best = r
        else:
            break

    if best:
        so = float(best["val"])
        # forward-adjust for splits between filing and event date
        for s in splits:
            if best["filed"] < s["date"] <= day_str:
                so *= s["to"] / s["from"]
        flt = so * poly_ratio if poly_ratio else so
        age = (_date.fromisoformat(day_str) - _date.fromisoformat(best["filed"])).days
        return flt, best["filed"], age > 60

    return None, None, True


# ---------------- formatting ----------------
def big(x):
    if x is None:
        return "-"
    a = abs(x)
    for div, s in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if a >= div:
            return f"{x / div:,.2f}{s}"
    return f"{x:,.0f}"


def usd(x):
    return "-" if x is None else ("-$" + big(-x) if x < 0 else "$" + big(x))


def pct(x, d=1):
    return "-" if x is None else f"{x * 100:+,.{d}f}%"


def rot(x):
    return "-" if x is None else f"{x * 100:,.0f}%  ({x:,.2f}x)"


def mx(x):
    return "-" if x is None else f"{x:,.2f}x"


def px(x):
    return "-" if x is None else f"${x:,.4f}" if x < 1 else f"${x:,.2f}"


def dstr(d):
    return d.strftime("%m/%d/%Y") if d else "-"


def div(a, b):
    return None if (a is None or not b) else a / b


# ---------------- data shaping ----------------
def load_daily(tkr, start_date=None):
    today = datetime.now(ET).date()
    if start_date:
        frm_date = min(start_date - timedelta(days=60), today - timedelta(days=LOOKBACK_DAYS + 60))
    else:
        frm_date = today - timedelta(days=LOOKBACK_DAYS + 60)
    frm = frm_date.isoformat()
    rows = []
    for b in aggs(tkr, 1, "day", frm, today.isoformat(), adjusted="false"):
        d = datetime.fromtimestamp(b["t"] / 1000 + 6 * 3600, timezone.utc).date()
        vw = b.get("vw") or (b["h"] + b["l"] + b["c"]) / 3
        rows.append(dict(d=d, o=b["o"], h=b["h"], l=b["l"], c=b["c"], v=b["v"], dv=b["v"] * vw))
    if not any(r["d"] == today for r in rows):
        alp_df = _alpaca_live_df(tkr)
        if alp_df is not None and not alp_df.empty:
            tot_v = float(alp_df["v"].sum())
            if tot_v > 0:
                vw = float((alp_df["c"] * alp_df["v"]).sum() / tot_v)
                rows.append(dict(
                    d=today,
                    o=float(alp_df["o"].iloc[0]),
                    h=float(alp_df["h"].max()),
                    l=float(alp_df["l"].min()),
                    c=float(alp_df["c"].iloc[-1]),
                    v=round(tot_v),
                    dv=round(tot_v * vw)
                ))
    return rows


def detect(D):
    n = len(D)
    cands = []
    for i in range(n):
        lo = max(0, i - RALLY_WIN)
        if i == lo:
            continue
        w = D[max(0, i - 7): i + 8]
        if D[i]["h"] < max(r["h"] for r in w):
            continue
        j = min(range(lo, i), key=lambda k: D[k]["l"])
        if D[j]["l"] <= 0:
            continue
        run = D[i]["h"] / D[j]["l"] - 1
        if run >= MIN_RUN:
            cands.append((i, j, run))
    if not cands:
        return None, []
    recent = [c for c in cands if c[0] >= n - RECENT_BARS]
    pool = recent or cands
    best = max(pool, key=lambda c: c[2])
    alts = sorted([c for c in cands if c is not best], key=lambda c: -c[2])[:3]
    return best, alts


def bases_legs(R):
    """R = daily rows from rally low day to rally high day."""
    bases, legs = 0, []
    leg_start, leg_low = 0, R[0]["l"]
    hi, hi_i, days, depth = R[0]["h"], 0, 0, 0.0
    b_low, b_low_i = None, None
    for i in range(1, len(R)):
        r = R[i]
        if r["h"] > hi:
            if days >= BASE_MIN_DAYS and depth >= BASE_MIN_DEPTH:
                bases += 1
                legs.append((hi / leg_low - 1, hi_i - leg_start + 1))
                leg_start, leg_low = b_low_i, b_low
            hi, hi_i, days, depth, b_low, b_low_i = r["h"], i, 0, 0.0, None, None
        else:
            days += 1
            depth = max(depth, 1 - r["l"] / hi)
            if b_low is None or r["l"] < b_low:
                b_low, b_low_i = r["l"], i
    legs.append((hi / leg_low - 1, hi_i - leg_start + 1))
    return bases, legs


def legs_15min(tkr, start_date, high_date, flt, chart_end_date=None):
    """
    Detect rally legs on the 15min chart using structural swing pivot analysis.
    A swing high is a bar whose high is the highest of PIVOT_N bars on each side.
    Each major impulse leg sets a new high of the rally so far.
    Legs smaller than MIN_LEG_PCT merge into the next leg so small ripples don't create false legs.
    chart_end_date: if provided, bars are fetched through this date (e.g. FRD+1)
                    but legs are only detected up to the peak within [start, high_date].
    Returns (chart_bars, legs, pivot_high_indices, pivot_low_indices).
    """
    fetch_end = chart_end_date if chart_end_date else high_date
    raw = aggs(tkr, 15, "minute", start_date.isoformat(), fetch_end.isoformat())
    if not raw:
        return [], [], [], []

    # Keep full extended trading hours: premarket (4am-9:30am) + RTH (9:30am-4pm) + after-hours (4pm-8pm ET)
    bars = []
    for b in raw:
        t_et = datetime.fromtimestamp(b["t"] / 1000, ET)
        hm = t_et.hour * 60 + t_et.minute
        if 240 <= hm < 1200:
            b_copy = dict(b)
            b_copy["_t"] = t_et.astimezone(IST)
            b_copy["_et"] = t_et
            b_copy["_dv"] = b["v"] * (b.get("vw") or (b["h"] + b["l"] + b["c"]) / 3)
            bars.append(b_copy)

    n = len(bars)
    if n < PIVOT_N * 2 + 2:
        return [], [], [], []

    # Leg detection is ONLY performed on bars up to high_date peak (using US ET date).
    # Remaining bars (high_date+1 ...) appear on the chart for context only.
    analysis_n = n
    if chart_end_date and chart_end_date > high_date:
        analysis_n = max(
            (k + 1 for k, b in enumerate(bars) if b["_et"].date() <= high_date),
            default=n
        )
        analysis_n = max(analysis_n, PIVOT_N * 2 + 2)

    peak_bar_i = max(range(analysis_n), key=lambda k: bars[k]["h"])

    # Detect pivot highs within analysis window
    piv_h = []
    for i in range(PIVOT_N, analysis_n - PIVOT_N):
        hi = bars[i]["h"]
        if all(bars[i - k]["h"] <= hi for k in range(1, PIVOT_N + 1)) and \
           all(bars[i + k]["h"] <= hi for k in range(1, PIVOT_N + 1)):
            piv_h.append(i)

    if peak_bar_i not in piv_h:
        piv_h.append(peak_bar_i)
    piv_h = sorted(list(set(piv_h)))

    # Track progressive new-high pivots (major structural impulse wave peaks)
    major_highs = []
    running_max = 0
    for h_i in piv_h:
        px = bars[h_i]["h"]
        if px > running_max:
            major_highs.append(h_i)
            running_max = px

    # Construct legs between highs:
    raw_legs = []
    prev_h_i = 0
    for h_i in major_highs:
        abs_lo_i = min(range(prev_h_i, h_i + 1), key=lambda k: bars[k]["l"])
        abs_lo_px = bars[abs_lo_i]["l"]
        # Find the impulse launch low right before the breakout ascent into h_i
        k = h_i
        while k > abs_lo_i and bars[k]["l"] >= bars[k - 1]["l"] * 0.99:
            k -= 1
        lo_i = k if (bars[k]["l"] <= abs_lo_px * 1.05 and h_i - k >= 3) else abs_lo_i
        lo_px = bars[lo_i]["l"]
        hi_px = bars[h_i]["h"]
        pct = hi_px / lo_px - 1
        raw_legs.append((lo_i, h_i, lo_px, hi_px, pct))
        prev_h_i = h_i

    # Merge legs that gained less than MIN_LEG_PCT into next leg
    merged = []
    j = 0
    while j < len(raw_legs):
        lo_i, hi_i, lo_px, hi_px, pct = raw_legs[j]
        if pct >= MIN_LEG_PCT or j == len(raw_legs) - 1:
            merged.append((lo_i, hi_i, lo_px, hi_px, pct))
            j += 1
        else:
            if j + 1 < len(raw_legs):
                nlo_i, nhi_i, nlo_px, nhi_px, _ = raw_legs[j + 1]
                if lo_px < nlo_px:
                    nlo_i, nlo_px = lo_i, lo_px
                new_pct = nhi_px / nlo_px - 1
                raw_legs[j + 1] = (nlo_i, nhi_i, nlo_px, nhi_px, new_pct)
            else:
                merged.append((lo_i, hi_i, lo_px, hi_px, pct))
            j += 1
    raw_legs = merged

    # Build final leg dicts with base detection
    legs = []
    for li, (lo_i, hi_i, lo_px, hi_px, _) in enumerate(raw_legs):
        leg_bars = bars[lo_i: hi_i + 1]
        vol = sum(x["v"]   for x in leg_bars)
        dv  = sum(x["_dv"] for x in leg_bars)

        # Base: from this leg's high to the next leg's low
        base_info = None
        base_lo_bar_i = None
        if li + 1 < len(raw_legs):
            nlo_i, nhi_i, nlo_px, _, _ = raw_legs[li + 1]
            bl_i = min(range(hi_i, nlo_i + 1), key=lambda k: bars[k]["l"])
            bl_px = bars[bl_i]["l"]
            base_bars  = bars[hi_i: nlo_i + 1]
            base_depth = (hi_px - bl_px) / hi_px
            base_dur   = len(base_bars)
            base_lo_bar_i = bl_i
            base_info  = dict(
                low_px   = bl_px,
                depth    = base_depth,
                bars     = base_dur,
                start_t  = bars[hi_i]["_t"].strftime("%m/%d %H:%M"),
                end_t    = bars[nlo_i]["_t"].strftime("%m/%d %H:%M"),
            )

        legs.append(dict(
            leg_num       = len(legs) + 1,
            lo_bar_i      = lo_i,
            hi_bar_i      = hi_i,
            base_lo_bar_i = base_lo_bar_i,
            low_px        = lo_px,
            high_px       = hi_px,
            pct_gain      = hi_px / lo_px - 1,
            dollar_gain   = hi_px - lo_px,
            vol           = vol,
            dv            = dv,
            float_rot     = vol / flt if flt else None,
            bars          = len(leg_bars),
            start_t       = bars[lo_i]["_t"].strftime("%m/%d %H:%M"),
            end_t         = bars[hi_i]["_t"].strftime("%m/%d %H:%M"),
            base          = base_info,
        ))

    # Pivot high and low markers for chart annotations
    ph_indices = [lg["hi_bar_i"] for lg in legs]
    pl_indices = [lg["lo_bar_i"] for lg in legs]
    for lg in legs:
        if lg.get("base_lo_bar_i") is not None and lg["base_lo_bar_i"] not in pl_indices:
            pl_indices.append(lg["base_lo_bar_i"])
    ph_indices.sort()
    pl_indices.sort()

    # Serialize ALL fetched bars for the chart (includes post-peak context through FRD+1)
    chart_bars = [
        {
            "t": b["_t"].strftime("%m/%d %H:%M"),
            "o": round(b["o"], 4),
            "h": round(b["h"], 4),
            "l": round(b["l"], 4),
            "c": round(b["c"], 4),
            "v": int(b["v"]),
            "dv": round(b["_dv"], 2),
            "pm": bool(240 <= (b["_et"].hour * 60 + b["_et"].minute) < 570),
            "sess": ("pm" if 240 <= (b["_et"].hour * 60 + b["_et"].minute) < 570 else
                     ("rth" if 570 <= (b["_et"].hour * 60 + b["_et"].minute) < 960 else "ah")),
        }
        for b in bars
    ]

    return chart_bars, legs, ph_indices, pl_indices


def frd_intraday(tkr, d, prev_close):
    today = datetime.now(ET).date()
    bars = []
    if d == today:
        alp_df = _alpaca_live_df(tkr)
        if alp_df is not None and not alp_df.empty:
            for dt_et, row in alp_df.iterrows():
                bars.append({
                    "t": int(dt_et.timestamp() * 1000),
                    "o": float(row["o"]), "h": float(row["h"]),
                    "l": float(row["l"]), "c": float(row["c"]),
                    "v": round(float(row["v"])), "vw": float(row.get("c"))
                })
    if not bars:
        bars = aggs(tkr, 1, "minute", d.isoformat(), d.isoformat(), adjusted="false")
    if not bars:
        return None
    pm, rth, allb = [], [], []
    for b in bars:
        t = datetime.fromtimestamp(b["t"] / 1000, ET)
        hm = t.hour * 60 + t.minute
        b["_t"] = t
        b["_dv"] = b["v"] * (b.get("vw") or b["c"])
        if 240 <= hm < 570:
            pm.append(b)
            allb.append(b)
        elif 570 <= hm < 960:
            rth.append(b)
            allb.append(b)
    if not rth:
        return None
    o = rth[0]["o"]
    hi_i = max(range(len(rth)), key=lambda i: rth[i]["h"])
    lo_i = min(range(len(rth)), key=lambda i: rth[i]["l"])
    H, L = rth[hi_i]["h"], rth[lo_i]["l"]
    L_after = min(x["l"] for x in rth[hi_i:])
    to_h = rth[:hi_i + 1]
    out = dict(
        o=o, h=H, l=L, c=rth[-1]["c"], l_after=L_after,
        hod_t=rth[hi_i]["_t"].astimezone(IST).strftime("%H:%M"), lod_t=rth[lo_i]["_t"].astimezone(IST).strftime("%H:%M"),
        mins_to_hod=hi_i,
        gap=div(o - prev_close, prev_close) if prev_close else None,
        o2h=div(H - o, o), h2l=div(L_after - H, H), h2l_full=div(L - H, H),
        close_from_h=div(rth[-1]["c"] - H, H), close_vs_open=div(rth[-1]["c"] - o, o),
        o2h_vol=sum(x["v"] for x in to_h), o2h_dv=sum(x["_dv"] for x in to_h),
        rth_vol=sum(x["v"] for x in rth), rth_dv=sum(x["_dv"] for x in rth),
        pm_vol=sum(x["v"] for x in pm), pm_dv=sum(x["_dv"] for x in pm),
        pm_high=max((x["h"] for x in pm), default=None),
        pm_low=min((x["l"] for x in pm), default=None),
        pm_high_t=(max(pm, key=lambda x: x["h"])["_t"].astimezone(IST).strftime("%H:%M") if pm else None),
    )
    out["tot_vol"] = out["pm_vol"] + out["rth_vol"]
    out["tot_dv"] = out["pm_dv"] + out["rth_dv"]
    sv = sum(x["v"] for x in allb)
    out["vwap_close"] = (sum(x["_dv"] for x in allb) / sv) if sv else None
    return out


# ---------------- main analysis ----------------
def analyze(tkr, start_s=None, frd_s=None, float_m=None):
    if not start_s:
        raise RuntimeError("Enter the rally start date")
    sd = datetime.strptime(start_s, "%Y-%m-%d").date()
    D = load_daily(tkr, start_date=sd)
    if len(D) < 30:
        raise RuntimeError("Not enough daily data for " + tkr)
    n = len(D)
    notes, alts_txt = [], []

    # ---- rally window
    si = next((i for i, r in enumerate(D) if r["d"] >= sd), None)
    if si is None:
        raise RuntimeError("Start date is after the last bar")
    if D[si]["d"] != sd:
        notes.append(f"{dstr(sd)} is not a trading day with data - used {dstr(D[si]['d'])} as the start")
    if si + 1 >= n:
        raise RuntimeError("No bars after the start date yet")
    pi = max(range(si, min(n, si + RALLY_MAX_DAYS)), key=lambda k: D[k]["h"])
    if pi == si:
        raise RuntimeError("The high is on the start day itself - pick an earlier start date")

    # ---- first red day
    fi = None
    if frd_s:
        fd = datetime.strptime(frd_s, "%Y-%m-%d").date()
        fi = next((i for i, r in enumerate(D) if r["d"] == fd), None)
    else:
        if D[pi]["c"] < D[pi]["o"]:
            fi = pi
        else:
            fi = next((k for k in range(pi + 1, n) if D[k]["c"] < D[k]["o"]), None)
    frd = D[fi] if fi is not None else None
    prev_close = D[fi - 1]["c"] if fi else None

    # ---- shares / float / market cap  (point-in-time historical float)
    pre = D[si - 1] if si > 0 else D[si]
    det_then = details(tkr, pre["d"].isoformat())
    det_now  = details(tkr)
    shares = (det_then.get("share_class_shares_outstanding") or det_then.get("weighted_shares_outstanding")
              or det_now.get("share_class_shares_outstanding") or det_now.get("weighted_shares_outstanding"))

    flt, flt_src = None, ""
    if float_m:
        flt, flt_src = float(float_m) * 1e6, "manual override"
    else:
        # --- Step A: get Polygon current float + free-float ratio (ratio = free_float / shares_out)
        poly_f, poly_asof, poly_pct, poly_err = poly_float(tkr)
        poly_ratio = None
        if poly_f and shares:
            poly_ratio = min(1.0, max(0.02, poly_f / float(shares)))

        # --- Step B: reconstruct float as of the rally-start date via SEC EDGAR
        sec_rows = load_shares_out(tkr)
        splits   = load_splits(tkr)
        rally_start_str = D[si]["d"].isoformat()

        hist_flt, filing_date, stale = float_at_date(rally_start_str, sec_rows, splits, poly_ratio)

        if hist_flt:
            flt = hist_flt
            pct_part = f", {poly_pct:.1f}% of shares out" if poly_pct is not None else ""
            stale_mark = "*" if stale else ""
            flt_src = (f"SEC EDGAR historical{stale_mark} (filing {filing_date}, "
                       f"as of {rally_start_str}{pct_part})")
            if stale:
                notes.append(f"Historical float marked * — nearest SEC filing ({filing_date}) is "
                             f">60 days before the rally start ({rally_start_str}); may be stale.")
        elif poly_f:
            # No SEC filing predates rally start: use Polygon current float back-adjusted for splits
            flt = poly_f
            pct_part = f", {poly_pct:.1f}% of shares out" if poly_pct is not None else ""
            flt_src = f"Polygon float (as of {poly_asof}{pct_part})"
            notes.append(f"No SEC filing found before rally start ({rally_start_str}); "
                         f"using Polygon current float (as of {poly_asof}) — rotation may be overstated if dilution occurred.")
        else:
            # Last resort: shares outstanding from Polygon ticker details
            notes.append("Historical float unavailable (SEC + Polygon both failed). " + str(poly_err))
            if shares:
                flt, flt_src = float(shares), "shares outstanding (true float unavailable)"

    mcap_before = (shares * pre["c"]) if shares else det_now.get("market_cap")
    mcap_peak   = (shares * D[pi]["h"]) if shares else None

    # ---- intraday on FRD
    mb = None
    if frd:
        try:
            mb = frd_intraday(tkr, frd["d"], prev_close)
        except Exception as e:
            notes.append("FRD minute data failed: %s" % e)
        if not mb:
            notes.append("No minute bars for FRD - premarket / O->H volume unavailable")

    # ---- rally totals (start -> high)
    R = D[si:pi + 1]
    bases, legs = bases_legs(R)
    # ---- 15min leg breakdown
    legs_15, bars_15, ph_15, pl_15 = [], [], [], []
    try:
        # Chart extends to FRD+1 trading day for context; legs detected only up to rally high
        if fi is not None:
            chart_end = D[fi + 1]["d"] if (fi + 1 < len(D)) else D[fi]["d"]
        elif pi is not None and pi + 1 < len(D):
            chart_end = D[pi + 1]["d"]
        else:
            chart_end = D[pi]["d"]
        bars_15, legs_15, ph_15, pl_15 = legs_15min(tkr, D[si]["d"], D[pi]["d"], flt, chart_end_date=chart_end)
    except Exception as e:
        notes.append(f"15min leg detection failed: {e}")
    partial = bool(frd and fi == pi and mb)
    if partial:
        rally_vol = sum(r["v"] for r in D[si:pi]) + mb["pm_vol"] + mb["o2h_vol"]
        rally_dv = sum(r["dv"] for r in D[si:pi]) + mb["pm_dv"] + mb["o2h_dv"]
        notes.append("FRD is also the rally-high day: rally volume / $ volume counted through the HOD minute (premarket included)")
    else:
        rally_vol = sum(r["v"] for r in R)
        rally_dv = sum(r["dv"] for r in R)
    ndays = len(R)
    rally_run = D[pi]["h"] / D[si]["l"] - 1
    base20 = D[max(0, si - 20):si]
    base_v = (sum(r["v"] for r in base20) / len(base20)) if base20 else None
    base_dv = (sum(r["dv"] for r in base20) / len(base20)) if base20 else None
    big_v = max(R, key=lambda r: r["v"])
    big_dv = max(R, key=lambda r: r["dv"])
    big_move = max(range(si, pi + 1), key=lambda k: (D[k]["c"] / D[k - 1]["c"] - 1) if k > 0 else -9)
    big_move_pct = (D[big_move]["c"] / D[big_move - 1]["c"] - 1) if big_move > 0 else None
    green = sum(1 for r in R if r["c"] > r["o"])

    # ================= PANEL 1 =================
    p1 = [
        ["Leg", "0 legs drawn", "Draw legs on 15min chart below (click or drag)"],
        ["Float (before rally)", big(flt), flt_src],
        ["Market cap (before rally)", usd(mcap_before), f"{dstr(pre['d'])} close {px(pre['c'])}"],
        ["Float rotation (start -> high)", rot(div(rally_vol, flt)), f"{big(rally_vol)} sh over {ndays}d"],
        ["Market cap at rally high", f"{usd(mcap_peak)}  ({mx(div(mcap_peak, mcap_before))})", f"{mx(div(mcap_peak, mcap_before))} vs pre-rally {usd(mcap_before)}  (peak {px(D[pi]['h'])})"],
        ["Dollar volume (start -> high)", usd(rally_dv), f"avg {usd(rally_dv / ndays)}/day over {ndays}d"],
        ["Run O->H (FRD)", pct(mb["o2h"]) if mb else (pct(div(frd["h"] - frd["o"], frd["o"])) if frd else "-"),
         f"open {px(mb['o'] if mb else frd['o'])} -> HOD {px(mb['h'] if mb else frd['h'])}" + (f" @ {mb['hod_t']} IST" if mb else "") if frd else "no FRD yet"],
        ["H->L (FRD)", pct(mb["h2l"]) if mb else (pct(div(frd["l"] - frd["h"], frd["h"])) if frd else "-"),
         ("HOD -> lowest low after HOD" + (f"  (close {pct(mb['close_from_h'])} from HOD)" if mb else "")) if frd else ""],
        ["Gap up (FRD)", pct(mb["gap"]) if mb else (pct(div(frd["o"] - prev_close, prev_close)) if frd else "-"),
         f"open {px(mb['o'] if mb else (frd['o'] if frd else None))} vs prev close {px(prev_close)}" if frd else ""],
        ["Pre-mkt vol (FRD)", big(mb["pm_vol"]) if mb else "-", "04:00-09:30 ET / 13:30-19:00 IST"],
        ["Pre-mkt $ vol (FRD)", usd(mb["pm_dv"]) if mb else "-", f"PM high {px(mb['pm_high'])} @ {mb['pm_high_t']} IST" if mb and mb.get("pm_high") else ""],
        ["O->H vol (FRD)", big(mb["o2h_vol"]) if mb else "-", (usd(mb["o2h_dv"]) + " traded open -> HOD") if mb else ""],
        ["Date of First Red Day", dstr(frd["d"]) if frd else "none yet", ("rally high " + dstr(D[pi]["d"])) + (" (same day)" if fi == pi else "") if frd else ""],
    ]

    # ================= PANEL 2 =================
    p2 = []
    p2_prof = [
        ["Rally start (low)", f"{dstr(D[si]['d'])}  {px(D[si]['l'])}"],
        ["Rally high", f"{dstr(D[pi]['d'])}  {px(D[pi]['h'])}"],
        ["Days start -> high", f"{ndays}  ({green} green)"],
        ["Biggest day move", f"{pct(big_move_pct, 0)} on {dstr(D[big_move]['d'])}"],
    ]
    p2.append(["Rally profile", p2_prof])
    p2.append(["Volume", [
        ["Total rally volume", big(rally_vol)],
        ["Avg daily vol (rally)", big(rally_vol / ndays)],
        ["Pre-rally 20d avg vol", big(base_v)],
        ["Rally avg RVOL", mx(div(rally_vol / ndays, base_v))],
        ["Peak-day volume / RVOL", f"{big(D[pi]['v'])}  /  {mx(div(D[pi]['v'], base_v))}"],
        ["Biggest volume day", f"{big(big_v['v'])} on {dstr(big_v['d'])}"],
        ["Last 3d vs first 3d vol", mx(div(sum(r['v'] for r in R[-3:]), sum(r['v'] for r in R[:3])))],
        ["FRD full-day volume", big(mb["tot_vol"] if mb else (frd["v"] if frd else None))],
        ["FRD vol vs peak-day vol", mx(div((mb["tot_vol"] if mb else (frd["v"] if frd else None)), D[pi]["v"]))],
        ["PM vol % of FRD total", pct(div(mb["pm_vol"], mb["tot_vol"]), 0) if mb else "-"],
    ]])
    p2.append(["Dollar volume", [
        ["Total rally $ vol", usd(rally_dv)],
        ["Avg daily $ vol (rally)", usd(rally_dv / ndays)],
        ["Pre-rally 20d avg $ vol", usd(base_dv)],
        ["$ vol expansion", mx(div(rally_dv / ndays, base_dv))],
        ["Peak-day $ vol", usd(D[pi]["dv"])],
        ["Biggest $ vol day", f"{usd(big_dv['dv'])} on {dstr(big_dv['d'])}"],
        ["FRD full-day $ vol", usd(mb["tot_dv"] if mb else (frd["dv"] if frd else None))],
        ["FRD PM $ vol / O->H $ vol", f"{usd(mb['pm_dv'])}  /  {usd(mb['o2h_dv'])}" if mb else "-"],
    ]])
    p2.append(["Float & market cap", [
        ["Float", f"{big(flt)}  ({flt_src})"],
        ["Shares outstanding", big(shares)],
        ["Market cap before rally", usd(mcap_before)],
        ["Market cap at rally high", usd(mcap_peak)],
        ["Market cap multiple", mx(div(mcap_peak, mcap_before))],
        ["Avg $ vol / market cap (rally)", pct(div(rally_dv / ndays, mcap_before), 1)],
        ["Float turnover price (rally $vol / float)", px(div(rally_dv, flt)) + " avg paid/sh"],
    ]])
    fv_tot = mb["tot_vol"] if mb else (frd["v"] if frd else None)
    p2.append(["Float rotation", [
        ["Total (start -> high)", rot(div(rally_vol, flt))],
        ["Avg per day", rot(div(rally_vol / ndays, flt))],
        ["Peak day", rot(div(D[pi]["v"], flt))],
        ["Biggest rotation day", f"{rot(div(big_v['v'], flt))} on {dstr(big_v['d'])}"],
        ["FRD premarket", rot(div(mb["pm_vol"], flt)) if mb else "-"],
        ["FRD open -> HOD", rot(div(mb["o2h_vol"], flt)) if mb else "-"],
        ["FRD full day", rot(div(fv_tot, flt))],
    ]])
# (legs are manually drawn by user on the interactive chart)
    if frd:
        p2.append(["First red day detail", [
            ["Date / gap", f"{dstr(frd['d'])}  /  {pct(mb['gap']) if mb else '-'}"],
            ["Open / High / Low / Close",
             (f"{px(mb['o'])} / {px(mb['h'])} / {px(mb['l'])} / {px(mb['c'])}" if mb else
              f"{px(frd['o'])} / {px(frd['h'])} / {px(frd['l'])} / {px(frd['c'])}")],
            ["Close vs open", pct(mb["close_vs_open"]) if mb else pct(div(frd['c'] - frd['o'], frd['o']))],
            ["Close vs prev close", pct(div(frd["c"] - prev_close, prev_close))],
            ["HOD time / min after open", f"{mb['hod_t']} IST / {mb['mins_to_hod']}m" if mb else "-"],
            ["LOD time", f"{mb['lod_t']} IST" if mb else "-"],
            ["HOD -> close", pct(mb["close_from_h"]) if mb else "-"],
            ["HOD -> full-day low", pct(mb["h2l_full"]) if mb else "-"],
            ["Close vs VWAP (from 04:00)", pct(div(mb["c"] - mb["vwap_close"], mb["vwap_close"])) if mb and mb["vwap_close"] else "-"],
            ["PM high / low", f"{px(mb['pm_high'])} / {px(mb['pm_low'])}" if mb and mb["pm_high"] else "-"],
            ["PM high vs prev close", pct(div(mb["pm_high"] - prev_close, prev_close)) if mb and mb["pm_high"] else "-"],
            ["PM range", pct(div(mb["pm_high"] - mb["pm_low"], mb["pm_low"])) if mb and mb["pm_high"] else "-"],
        ]])
        after = []
        for k in (1, 2, 3, 5):
            if fi + k < n:
                after.append([f"D+{k} close vs FRD close", pct(D[fi + k]["c"] / frd["c"] - 1)])
        if fi + 1 < n:
            after.insert(0, ["D+1 gap vs FRD close", pct(D[fi + 1]["o"] / frd["c"] - 1)])
            w = D[fi + 1: fi + 6]
            after.append(["Lowest low next 5d vs FRD close", pct(min(r["l"] for r in w) / frd["c"] - 1)])
            after.append(["Highest high next 5d vs FRD close", pct(max(r["h"] for r in w) / frd["c"] - 1)])
        if after:
            p2.append(["After the FRD", after])

    # ================= daily table =================
    end = fi if fi is not None else n - 1
    cum = 0.0
    trows = []
    for k in range(si, end + 1):
        r = D[k]
        cum += r["v"]
        chg = (r["c"] / D[k - 1]["c"] - 1) if k > 0 else None
        tag = ("LOW" if k == si else "") + (" HIGH" if k == pi else "") + (" FRD" if k == fi else "")
        trows.append([dstr(r["d"]), px(r["o"]), px(r["h"]), px(r["l"]), px(r["c"]), pct(chg, 0) if chg is not None else "-",
                      big(r["v"]), usd(r["dv"]), (rot(div(cum, flt)) if k <= pi else "-"), tag.strip()])
    trows = trows[-80:]

    if "true float not available" in flt_src:
        notes.append("True float unavailable on this key - using shares outstanding. Pass a float override (millions) to be exact.")
    notes.append("Float / shares are current-ish values; reverse splits or dilution during the rally are not reflected.")

    chart_15 = None
    if bars_15:
        chart_15 = {
            "bars": bars_15,
            "pivot_highs": ph_15,
            "pivot_lows": pl_15,
            "float_val": flt,
            "shares_out": shares,
            "mcap_before": mcap_before,
            "legs": [],
        }

    return dict(
        ticker=tkr,
        header=f"{tkr}  |  rally {dstr(D[si]['d'])} -> {dstr(D[pi]['d'])}  |  FRD {dstr(frd['d']) if frd else 'none yet'}",
        alts=alts_txt, p1=p1, p2=p2,
        chart_15=chart_15,
        table=dict(cols=["Date", "Open", "High", "Low", "Close", "Chg", "Volume", "$ Vol", "Cum float rot", ""], rows=trows),
        notes=notes,
    )


@app.route("/api/analyze")
def api():
    t = (request.args.get("ticker") or "").strip().upper()
    if not t:
        return jsonify(error="ticker required"), 400
    try:
        return jsonify(analyze(t, request.args.get("start") or None,
                               request.args.get("frd") or None,
                               request.args.get("float_m") or None))
    except Exception as e:
        return jsonify(error=str(e)), 500


@app.route("/")
def index():
    return Response(PAGE, mimetype="text/html")


PAGE = r"""<!doctype html><html><head><meta charset="utf-8"><title>Runner Stats – Multiday Rally & FRD</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
:root{--g:#26d974;--r:#ff4d4d;--y:#ffd23f;--b:#6cc4ff;--c:#29a8ff;--w:#e8e8e8;--d:#8a8a8a;--line:#262626}
*{box-sizing:border-box}
body{margin:0;background:#000;color:var(--w);font:15px/1.45 Consolas,"SF Mono",Menlo,monospace}
.bar{display:flex;gap:10px;padding:18px 34px;flex-wrap:wrap;align-items:center;border-bottom:1px solid var(--line)}
input[type=text],input[type=number]{background:#0d0d0d;border:1px solid var(--line);color:#fff;font:inherit;font-size:17px;padding:0 12px;height:46px}
input[type=date]{background:#0d0d0d;border:1px solid var(--line);color:#fff;font:inherit;font-size:14px;padding:0 10px;height:46px;color-scheme:dark}
#t{width:130px;text-transform:uppercase;font-size:19px;font-weight:bold}
button{background:#0d0d0d;border:1px solid var(--line);color:#fff;font:inherit;font-size:15px;padding:0 16px;height:46px;cursor:pointer;transition:all .15s}
button:hover{background:#1a1a1a}
label{color:var(--d);font-size:13px;display:flex;flex-direction:column;gap:3px}
#out{margin:0 34px 40px;border:1px solid #1a1a1a;padding:20px 22px;background:#000}
.hdr{font-size:18px;font-weight:bold;color:var(--w);margin:0 0 4px}
.alt{color:var(--d);font-size:13px;margin-bottom:8px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:10px;margin-top:10px}
.card{border:1px solid var(--line);padding:10px 14px;background:#0a0a0c}
.card .k{color:var(--d);font-size:12px;margin-bottom:3px}
.card .v{font-size:20px;font-weight:bold;color:var(--w);margin-bottom:2px}
.card .s{color:var(--d);font-size:12px;word-break:break-word}
.g2{display:grid;grid-template-columns:repeat(auto-fill,minmax(360px,1fr));gap:12px;margin-top:10px}
.sec{border:1px solid var(--line);padding:10px 14px;background:#0a0a0c}
.sec h3{margin:0 0 8px;font-size:13px;color:var(--c);text-transform:uppercase;letter-spacing:.08em}
.row{display:flex;justify-content:space-between;gap:16px;padding:3px 0;border-bottom:1px solid #151518}
.row:last-child{border:0}.row span:first-child{color:var(--d)}.row span:last-child{color:var(--w);text-align:right}
.scroll{overflow-x:auto;margin-top:10px}
table{border-collapse:collapse;width:100%;font-size:13px}
th{text-align:left;font-weight:normal;color:var(--w);padding:3px 14px 5px 0;border-bottom:1px solid var(--line);white-space:nowrap}
td{padding:3px 14px 3px 0;white-space:nowrap;color:var(--d)}
td:first-child{color:var(--w)}tr:hover td{background:#0a0a0c}
.note{color:#a08840;font-size:12px;margin-top:4px}.err{color:var(--r)}.status{color:var(--d);font-size:13px;margin-left:4px}
h2{font-size:12px;letter-spacing:.1em;color:var(--c);text-transform:uppercase;margin:22px 0 0;border-bottom:1px solid var(--line);padding-bottom:4px}
.back-btn{margin-left:auto;border-color:var(--c);color:var(--c);font-size:13px;height:40px;padding:0 16px}.back-btn:hover{background:#001a33}
#c15wrap{overflow-x:auto;border:1px solid var(--line);background:#070709;position:relative;margin-top:8px;user-select:none;scrollbar-color:#333 #070709}
.btn-tool{font-size:12px;padding:2px 10px;height:28px;line-height:1;border-radius:2px;display:inline-flex;align-items:center;gap:4px}
.btn-tool:hover{filter:brightness(1.2)}
.leg-pill{border:1px solid #444;padding:4px 10px;font-size:12px;background:#0a0a0c;display:inline-flex;align-items:center;gap:8px;border-radius:2px}

/* Enlarge / Fullscreen mode */
#c15section.enlarged{
  position:fixed;inset:0;z-index:99999;background:#050507;padding:16px 24px;display:flex;flex-direction:column;overflow:hidden;
}
#c15section.enlarged #c15wrap{
  flex:1;height:calc(100vh - 120px)!important;max-height:calc(100vh - 120px)!important;margin-top:8px;
}
.ticker_tab:hover{background:#00221c!important}
</style></head><body>
<div class="bar">
  <input id="t" type="text" placeholder="TICKER" autofocus autocomplete="off" spellcheck="false">
  <label>Rally start date<input id="s" type="date"></label>
  <label>FRD (optional)<input id="f" type="date"></label>
  <label>Float M (optional)<input id="fl" type="number" step="any" style="width:110px"></label>
  <button id="go">Get stats</button>
  <button id="rf">Refresh</button>
  <button id="cp">Copy all</button>
  <button id="cpimg" title="Copy the full report as a high-resolution PNG">Copy Details Image</button>
  <span id="st" class="status"></span>
  <button class="back-btn" id="backbtn">← Back to Chart Stats</button>
</div>
<!-- Multi-Ticker Tab Bar -->
<div id="tab_bar" style="display:none;padding:8px 34px 0;background:#050505;border-bottom:1px solid var(--line);gap:6px;overflow-x:auto;align-items:center;flex-wrap:nowrap"></div>
<div id="out"><span class="d">Enter a ticker and rally start date, then press Get stats.</span></div>
<script>
let LAST=null, API_BASE='/api';
const $=id=>document.getElementById(id);

let openTabs = {}; // t -> { ticker, start, frd, float_m, data, legs, status, error, isRunning }
let activeTab = null;

if(location.pathname.startsWith('/runner')) API_BASE='/api/runner';
document.getElementById('backbtn').onclick=()=>{
  if(window.opener && !window.opener.closed){
    window.close();
  } else if(document.referrer && document.referrer !== window.location.href){
    window.location.href = document.referrer;
  } else {
    window.location.href = '/';
  }
};

function renderTabBar(){
  const bar = $('tab_bar');
  const tickers = Object.keys(openTabs);
  if(!tickers.length){
    bar.style.display = 'none';
    return;
  }
  bar.style.display = 'flex';

  bar.innerHTML = tickers.map(t => {
    const isActive = t === activeTab;
    const tabObj = openTabs[t];
    const isRunning = tabObj && tabObj.isRunning;
    const bg = isActive ? '#002b24' : '#080808';
    const borderTop = isActive ? '2px solid #00c9a7' : '2px solid transparent';
    const color = isActive ? '#00c9a7' : '#888';
    const border = isActive ? '1px solid #005a48' : '1px solid #1f1f1f';

    return `<div class="ticker_tab" onclick="switchTab('${esc(t)}')" style="background:${bg};border:${border};border-top:${borderTop};color:${color};padding:8px 14px;border-radius:4px 4px 0 0;font-size:13px;cursor:pointer;display:flex;align-items:center;gap:8px;user-select:none;transition:background 0.15s">`+
      `<span style="font-weight:bold">${esc(t)}</span>`+
      (isRunning ? '<span style="color:#ffd23f;font-size:11px" title="Analyzing...">●</span>' : '')+
      `<span onclick="event.stopPropagation(); closeTab('${esc(t)}')" style="font-size:14px;color:#666;line-height:1;padding:0 2px" onmouseover="this.style.color='#ff5a5a'" onmouseout="this.style.color='#666'">✕</span>`+
    `</div>`;
  }).join('');
}

function switchTab(t){
  if(!openTabs[t]) return;
  if(activeTab && openTabs[activeTab] && _chartState){
    openTabs[activeTab].legs = _chartState.legs ? [..._chartState.legs] : [];
  }
  activeTab = t;
  const tab = openTabs[t];
  $('t').value = tab.ticker;
  $('s').value = tab.start || '';
  $('f').value = tab.frd || '';
  $('fl').value = tab.float_m || '';
  $('st').textContent = tab.status || '';
  renderTabBar();

  if(tab.isRunning){
    $('out').innerHTML = `<span class="d">Loading runner stats for <b>${esc(t)}</b>…</span>`;
  } else if(tab.error){
    $('out').innerHTML = '<span class="err">' + esc(tab.error) + '</span>';
  } else if(tab.data){
    LAST = tab.data;
    render(tab.data, tab.legs);
  }
}

function closeTab(t){
  if(!openTabs[t]) return;
  delete openTabs[t];

  if(activeTab === t){
    const remaining = Object.keys(openTabs);
    activeTab = remaining.length ? remaining[remaining.length - 1] : null;
    if(activeTab){
      switchTab(activeTab);
    } else {
      $('t').value = '';
      $('s').value = '';
      $('f').value = '';
      $('fl').value = '';
      $('st').textContent = '';
      $('out').innerHTML = '<span class="d">Enter a ticker and rally start date, then press Get stats.</span>';
      LAST = null;
      _chartState = null;
      renderTabBar();
    }
  } else {
    renderTabBar();
  }
}

async function run(refresh){
  let t = $('t').value.trim().toUpperCase();
  if(!t && activeTab) t = activeTab;
  if(!t) return;
  if(!$('s').value){
    $('out').innerHTML = '<span class="err">Enter the rally start date.</span>';
    return;
  }

  if(activeTab && openTabs[activeTab] && _chartState){
    openTabs[activeTab].legs = _chartState.legs ? [..._chartState.legs] : [];
  }

  if(!openTabs[t]){
    openTabs[t] = {
      ticker: t,
      start: $('s').value,
      frd: $('f').value,
      float_m: $('fl').value,
      data: null,
      legs: [],
      status: 'loading…',
      error: null,
      isRunning: true
    };
  }

  const tab = openTabs[t];
  tab.start = $('s').value;
  tab.frd = $('f').value;
  tab.float_m = $('fl').value;
  tab.isRunning = true;
  tab.status = 'loading…';
  activeTab = t;
  renderTabBar();

  $('st').textContent = 'loading…';
  if(!tab.data || refresh){
    $('out').innerHTML = `<span class="d">Loading runner stats for <b>${esc(t)}</b>…</span>`;
  }

  const q = new URLSearchParams({
    ticker: t,
    start: tab.start,
    frd: tab.frd || '',
    float_m: tab.float_m || ''
  });

  try {
    const r = await fetch(API_BASE + '/analyze?' + q);
    const j = await r.json();
    tab.isRunning = false;
    if(!r.ok){
      tab.error = j.error || 'error';
      tab.status = 'Failed';
      if(activeTab === t){
        $('out').innerHTML = '<span class="err">' + esc(tab.error) + '</span>';
        $('st').textContent = '';
      }
    } else {
      tab.data = j;
      tab.error = null;
      tab.status = '';
      if(activeTab === t){
        LAST = j;
        render(j, tab.legs);
        $('st').textContent = '';
      }
    }
  } catch(e) {
    tab.isRunning = false;
    tab.error = String(e);
    tab.status = 'Error';
    if(activeTab === t){
      $('out').innerHTML = '<span class="err">' + esc(e) + '</span>';
      $('st').textContent = '';
    }
  }
  renderTabBar();
}

function esc(s){return String(s).replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]))}

function render(j, restoredLegs){
  let h='<div class="hdr">'+esc(j.header)+'</div>';
  if(j.alts&&j.alts.length)h+='<div class="alt">other rallies: '+j.alts.map(esc).join(' · ')+'</div>';
  h+='<h2>Key stats</h2><div class="grid">'+j.p1.map(c=>'<div class="card" '+(c[0]==='Leg'?'id="legCard"':'')+'><div class="k">'+esc(c[0])+'</div><div class="v">'+esc(c[1])+'</div><div class="s">'+esc(c[2]||'')+'</div></div>').join('')+'</div>';
  
  if(j.chart_15&&j.chart_15.bars&&j.chart_15.bars.length){
    h+='<div id="c15section">'
      +'<div style="display:flex;align-items:center;gap:8px;margin:24px 0 6px;flex-wrap:wrap">'
      +'<h2 style="margin:0;border:0;padding:0">15min Structure Chart</h2>'
      +'<span style="color:var(--d);font-size:12px">scroll horizontally if needed</span>'
      +'<div style="margin-left:auto;display:flex;gap:6px;align-items:center;flex-wrap:wrap">'
      +'<button id="btnDraw" class="btn-tool" style="border-color:#ffd23f;color:#ffd23f;background:#282000;font-weight:bold">✏ Draw Leg (D)</button>'
      +'<button id="btnUndo" class="btn-tool">↶ Undo (Z)</button>'
      +'<button id="btnClear" class="btn-tool">🗑 Clear All</button>'
      +'<span style="color:#444;font-size:11px">|</span>'
      +'<button id="btnZoomOut" class="btn-tool" title="Zoom Out (narrow candles)">🔍 -</button>'
      +'<button id="btnZoomIn" class="btn-tool" title="Zoom In (wider candles)">🔍 +</button>'
      +'<button id="btnZoomFit" class="btn-tool" title="Fit Entire Chart">Fit</button>'
      +'<span style="color:#444;font-size:11px">|</span>'
      +'<button id="btnBadgeMode" class="btn-tool" title="Toggle Compact / Full Badges">🏷 Badges</button>'
      +'<button id="btnBases" class="btn-tool" title="Toggle Base Lines On/Off">📐 Bases</button>'
      +'<label style="font-size:12px;color:var(--d);cursor:pointer;display:inline-flex;align-items:center;margin:0;flex-direction:row;gap:4px"><input type="checkbox" id="bz15" style="margin:0;vertical-align:middle"> Bone zone</label>'
      +'<span style="color:#444;font-size:11px">|</span>'
      +'<button id="btnEnlarge" class="btn-tool" style="border-color:var(--c);color:var(--c);font-weight:bold" title="Enlarge Chart to Fullscreen (F)">⛶ Enlarge View</button>'
      +'<button id="btnCopyImg" class="btn-tool" style="border-color:#47d890;color:#47d890;font-weight:bold" title="Copy a high-resolution PNG of the chart">📋 Copy Image</button>'
      +'</div></div>';
    h+='<div id="drawStatus" style="color:#ffd23f;font-size:12px;font-family:Consolas,monospace;min-height:18px;margin:4px 0 4px"></div>';
    h+='<div id="legRibbon" style="display:flex;gap:8px;margin-bottom:6px;flex-wrap:wrap"></div>';
    h+='<div id="c15wrap"></div>';
    h+='</div>'; // end c15section
    h+='<div id="legPanel" style="margin-top:14px"></div>';
  }
  
  h+='<h2>Deep stats</h2><div class="g2">'+j.p2.map(s=>'<div class="sec"><h3>'+esc(s[0])+'</h3>'+s[1].map(r=>'<div class="row"><span>'+esc(r[0])+'</span><span>'+esc(r[1])+'</span></div>').join('')+'</div>').join('')+'</div>';
  h+='<h2>Rally days (start → FRD)</h2><div class="scroll"><table><thead><tr>'+j.table.cols.map(c=>'<th>'+esc(c)+'</th>').join('')+'</tr></thead><tbody>';
  j.table.rows.forEach(r=>{
    const tag=(r[r.length-1]||'').trim();
    let rc='';
    if(tag.includes('LOW'))rc='style="color:var(--g)"';
    else if(tag.includes('HIGH'))rc='style="color:var(--y)"';
    else if(tag.includes('FRD'))rc='style="color:var(--r)"';
    h+='<tr '+rc+'>'+r.map(c=>'<td>'+esc(c)+'</td>').join('')+'</tr>';
  });
  h+='</tbody></table></div>';
  h+=j.notes.map(n=>'<div class="note">* '+esc(n)+'</div>').join('');
  $('out').innerHTML=h;
  
  if(j.chart_15&&j.chart_15.bars&&j.chart_15.bars.length){
    setTimeout(()=>{
      initChart15(j.chart_15);
      if(restoredLegs && restoredLegs.length && _chartState){
        _chartState.legs = [...restoredLegs];
        _renderFrame();
        _updateLegPanel();
      }
    },20);
  }
}

function copyAll(){
  if(!LAST)return;const j=LAST;let t=j.header+'\n\nKEY STATS\n';
  t+=j.p1.map(c=>c[0]+': '+c[1]+(c[2]?'  ('+c[2]+')':'')).join('\n');
  j.p2.forEach(s=>{t+='\n\n'+s[0].toUpperCase()+'\n'+s[1].map(r=>r[0]+': '+r[1]).join('\n')});
  navigator.clipboard.writeText(t);$('st').textContent='copied';setTimeout(()=>$('st').textContent='',1500);
}
$('go').onclick=()=>run(false);
$('rf').onclick=()=>run(true);
$('cp').onclick=copyAll;
async function copyDetailsImage(){
  const source = document.getElementById('out');
  if(!LAST || !source){$('st').textContent='Load a report first.';return;}
  const clone = source.cloneNode(true);
  const sourceCanvases = source.querySelectorAll('canvas'), clonedCanvases = clone.querySelectorAll('canvas');
  sourceCanvases.forEach((canvas,i)=>{
    const target = clonedCanvases[i];
    if(!target) return;
    const image = document.createElement('img');
    try{image.src=canvas.toDataURL('image/png');}catch{return;}
    image.width=canvas.width; image.height=canvas.height;
    image.style.cssText=canvas.getAttribute('style')||'display:block;max-width:100%;height:auto';
    target.replaceWith(image);
  });
  clone.querySelectorAll('button,input,select').forEach(el=>el.remove());
  const css = [...document.querySelectorAll('style')].map(style=>style.textContent).join('\n').replace(/&/g,'&amp;').replace(/</g,'&lt;');
  const width = Math.ceil(Math.max(source.scrollWidth,source.getBoundingClientRect().width));
  const height = Math.ceil(source.scrollHeight);
  const scale = Math.max(1,Math.min(2,16000/Math.max(width,height)));
  const markup = `<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}"><foreignObject width="100%" height="100%"><div xmlns="http://www.w3.org/1999/xhtml" style="width:${width}px;min-height:${height}px;background:#000;color:#e8e8e8"><style>${css}</style>${clone.outerHTML}</div></foreignObject></svg>`;
  const svgUrl = URL.createObjectURL(new Blob([markup],{type:'image/svg+xml;charset=utf-8'}));
  try{
    const image = new Image();
    image.src=svgUrl;
    await new Promise((resolve,reject)=>{image.onload=resolve;image.onerror=reject});
    const output=document.createElement('canvas');
    output.width=Math.ceil(width*scale);output.height=Math.ceil(height*scale);
    const ctx=output.getContext('2d');if(!ctx)throw new Error('Canvas unavailable');
    ctx.fillStyle='#000';ctx.fillRect(0,0,output.width,output.height);ctx.scale(scale,scale);ctx.drawImage(image,0,0,width,height);
    const blob=await new Promise(resolve=>output.toBlob(resolve,'image/png'));
    if(!blob||!navigator.clipboard?.write||!window.ClipboardItem)throw new Error('Clipboard image support unavailable');
    await navigator.clipboard.write([new ClipboardItem({'image/png':blob})]);
    $('st').textContent=`Copied full report image · ${output.width} × ${output.height}px`;
    setTimeout(()=>$('st').textContent='',3000);
  }catch(error){
    $('st').textContent='Could not copy image. Check clipboard permissions and try again.';
  }finally{URL.revokeObjectURL(svgUrl);}
}
$('cpimg').onclick=copyDetailsImage;
['t', 's', 'f', 'fl'].forEach(id => {
  const el = $(id);
  if(el) el.addEventListener('keydown', e => { if(e.key === 'Enter') run(false); });
});

// Auto-read URL params if present
const urlP = new URLSearchParams(window.location.search);
const initT = (urlP.get('ticker') || urlP.get('symbol') || '').trim().toUpperCase();
const initS = urlP.get('start') || '';
if(initT){
  $('t').value = initT;
  if(initS) $('s').value = initS;
  if(urlP.get('frd')) $('f').value = urlP.get('frd');
  if(urlP.get('float_m')) $('fl').value = urlP.get('float_m');
  if(initS) setTimeout(() => run(false), 50);
}

// ================= CANVAS CHART & INTERACTIVE DRAWING =================
function fmtP(v){if(v==null)return '-';return v<1?'$'+v.toFixed(4):v<100?'$'+v.toFixed(2):'$'+v.toFixed(0);}
function fmtV(v){if(!v)return '0';return v>=1e6?(v/1e6).toFixed(1)+'M':v>=1e3?(v/1e3).toFixed(0)+'K':String(v);}
function fmtDv(v){if(!v)return '$0';return v>=1e9?'$'+(v/1e9).toFixed(2)+'B':v>=1e6?'$'+(v/1e6).toFixed(2)+'M':v>=1e3?'$'+(v/1e3).toFixed(0)+'K':'$'+v.toFixed(0);}
function fmtRot(v,flt){if(!flt||!v)return '-';const x=v/flt;return (x*100).toFixed(0)+'% ('+x.toFixed(2)+'x)';}
function niceNum(v){const p=Math.pow(10,Math.floor(Math.log10(v)));const f=v/p;return(f<1.5?1:f<3?2:f<7?5:10)*p;}

function boneZone15(bars){
  const n=bars.length,a9=2/10,a20=2/21;
  const e9=[],e20=[],bu=[],bl=[];
  for(let i=0;i<n;i++){
    const c=bars[i].c;
    e9.push(i?a9*c+(1-a9)*e9[i-1]:c);
    e20.push(i?a20*c+(1-a20)*e20[i-1]:c);
    if(i>=19){
      let s=0;for(let k=i-19;k<=i;k++)s+=bars[k].c;
      const m=s/20;let v=0;for(let k=i-19;k<=i;k++)v+=(bars[k].c-m)**2;
      const sd=Math.sqrt(v/20);bu.push(m+2*sd);bl.push(m-2*sd);
    }else{bu.push(null);bl.push(null);}
  }
  return {e9,e20,bu,bl};
}

let _chartState = null;

function _setStatus(msg){
  const el=document.getElementById('drawStatus');
  if(el) el.textContent = msg || '';
}

function _updateToolbarUI(){
  const s = _chartState;
  const btn = document.getElementById('btnDraw');
  if(!btn || !s) return;
  if(s.drawMode){
    btn.style.borderColor = '#ffd23f';
    btn.style.color = '#ffd23f';
    btn.style.background = '#282000';
    btn.textContent = '✏ Drawing Active (D)';
  } else {
    btn.style.borderColor = '#3ddc84';
    btn.style.color = '#3ddc84';
    btn.style.background = '#0d0d0d';
    btn.textContent = '✏ Draw Leg (D)';
  }
  const btnB = document.getElementById('btnBadgeMode');
  if(btnB) btnB.textContent = s.badgeMode === 'compact' ? '🏷 Compact Badges' : '🏷 Full Badges';
  const btnBase = document.getElementById('btnBases');
  if(btnBase) btnBase.textContent = s.showBases ? '📐 Bases: On' : '📐 Bases: Off';
  const btnEn = document.getElementById('btnEnlarge');
  if(btnEn) btnEn.textContent = s.enlarged ? '🗗 Exit Enlarge (Esc)' : '⛶ Enlarge View';
}

function toggleEnlarge(){
  const sec = document.getElementById('c15section');
  if(!sec || !_chartState) return;
  _chartState.enlarged = !_chartState.enlarged;
  if(_chartState.enlarged){
    sec.classList.add('enlarged');
  } else {
    sec.classList.remove('enlarged');
  }
  _updateToolbarUI();
  _setupAndMountCanvas();
}

async function copyChartImage(){
  const s = _chartState;
  if(!s || !s.cv) return;
  const scale = Math.max(1, Math.min(3, 16000 / Math.max(s.TW, s.TH))), image = document.createElement('canvas');
  image.width = Math.round(s.TW * scale); image.height = Math.round(s.TH * scale);
  const imageCtx = image.getContext('2d');
  if(!imageCtx){_setStatus('Could not create the chart image.');return;}
  imageCtx.scale(scale, scale);
  const liveCtx = s.ctx;
  try{
    s.ctx = imageCtx;
    _renderFrame();
    s.ctx = liveCtx;
    _renderFrame();
    const blob = await new Promise(resolve => image.toBlob(resolve, 'image/png'));
    if(!blob) throw new Error('PNG encoding failed');
    if(!navigator.clipboard?.write || !window.ClipboardItem) throw new Error('Image clipboard is unavailable in this browser');
    await navigator.clipboard.write([new ClipboardItem({'image/png':blob})]);
    _setStatus(`Copied ${image.width} × ${image.height} high-resolution PNG.`);
    setTimeout(()=>_setStatus(''),2500);
  }catch(error){
    s.ctx = liveCtx;
    _renderFrame();
    _setStatus('Could not copy image. Check clipboard permissions and try again.');
  }
}
function initChart15(data){
  const wrap = $('c15wrap');
  if(!wrap || !data || !data.bars || !data.bars.length) return;

  const N = data.bars.length;
  let initialCW = 9;
  if(N > 400) initialCW = 7;
  if(N > 800) initialCW = 5;

  _chartState = {
    data,
    bars: data.bars,
    N,
    candleWidth: initialCW,
    gap: Math.max(1, Math.ceil(initialCW * 0.28)),
    drawMode: true,
    badgeMode: 'compact',
    showBases: true,
    enlarged: false,
    pending: null,
    hoverI: -1,
    dragStartI: null,
    dragStartTime: 0,
    isDragging: false,
    legs: [],
  };

  _setupAndMountCanvas();
  _updateToolbarUI();
  _setStatus('✏ DRAW MODE ACTIVE: Click LOW bar of Leg 1 (or click & drag across to High)');

  // Wire buttons
  const btnDraw = document.getElementById('btnDraw');
  const btnUndo = document.getElementById('btnUndo');
  const btnClear = document.getElementById('btnClear');
  const bz = document.getElementById('bz15');
  const btnZOut = document.getElementById('btnZoomOut');
  const btnZIn = document.getElementById('btnZoomIn');
  const btnZFit = document.getElementById('btnZoomFit');
  const btnBadgeMode = document.getElementById('btnBadgeMode');
  const btnBases = document.getElementById('btnBases');
  const btnEnlarge = document.getElementById('btnEnlarge');
  const btnCopyImg = document.getElementById('btnCopyImg');

  if(btnDraw) btnDraw.onclick = () => {
    _chartState.drawMode = !_chartState.drawMode;
    _chartState.pending = null;
    _updateToolbarUI();
    _setStatus(_chartState.drawMode ? '✏ Click LOW bar of the leg' : 'Draw mode paused. Press "Draw Leg" to resume.');
    _renderFrame();
  };

  if(btnUndo) btnUndo.onclick = () => {
    if(_chartState.pending != null){
      _chartState.pending = null;
      _setStatus('Cancelled pending start point.');
    } else if(_chartState.legs.length){
      _chartState.legs.pop();
      _setStatus(`Undid last leg. ${_chartState.legs.length} leg(s) remaining.`);
    }
    _renderFrame();
    _updateLegPanel();
  };

  if(btnClear) btnClear.onclick = () => {
    _chartState.legs = [];
    _chartState.pending = null;
    _setStatus('Cleared all legs.');
    _renderFrame();
    _updateLegPanel();
  };

  if(bz) bz.onchange = () => _renderFrame();

  if(btnZOut) btnZOut.onclick = () => {
    _chartState.candleWidth = Math.max(3, _chartState.candleWidth - 2);
    _chartState.gap = Math.max(1, Math.ceil(_chartState.candleWidth * 0.28));
    _setupAndMountCanvas();
  };

  if(btnZIn) btnZIn.onclick = () => {
    _chartState.candleWidth = Math.min(26, _chartState.candleWidth + 2);
    _chartState.gap = Math.max(1, Math.ceil(_chartState.candleWidth * 0.28));
    _setupAndMountCanvas();
  };

  if(btnZFit) btnZFit.onclick = () => {
    const wrapW = wrap.clientWidth || 900;
    const cw = Math.max(3, Math.min(14, Math.floor((wrapW - 100) / N) - 1));
    _chartState.candleWidth = cw;
    _chartState.gap = Math.max(1, Math.ceil(cw * 0.25));
    _setupAndMountCanvas();
  };

  if(btnBadgeMode) btnBadgeMode.onclick = () => {
    _chartState.badgeMode = _chartState.badgeMode === 'compact' ? 'full' : 'compact';
    _updateToolbarUI();
    _renderFrame();
  };

  if(btnBases) btnBases.onclick = () => {
    _chartState.showBases = !_chartState.showBases;
    _updateToolbarUI();
    _renderFrame();
  };

  if(btnEnlarge) btnEnlarge.onclick = () => toggleEnlarge();
  if(btnCopyImg) btnCopyImg.onclick = () => copyChartImage();

  // Mouse wheel zoom
  wrap.onwheel = (e) => {
    if(e.ctrlKey || e.altKey){
      e.preventDefault();
      const delta = e.deltaY < 0 ? 1 : -1;
      _chartState.candleWidth = Math.max(3, Math.min(26, _chartState.candleWidth + delta));
      _chartState.gap = Math.max(1, Math.ceil(_chartState.candleWidth * 0.28));
      _setupAndMountCanvas();
    }
  };

  // Global key bindings
  window.onkeydown = (e) => {
    if(e.target.tagName === 'INPUT') return;
    if(e.key === 'Escape'){
      if(_chartState.enlarged){
        toggleEnlarge();
        return;
      }
      if(_chartState.pending != null){
        _chartState.pending = null;
        _setStatus('Cancelled pending start point.');
        _renderFrame();
      }
    } else if(e.key.toLowerCase() === 'f'){
      toggleEnlarge();
    } else if(e.key.toLowerCase() === 'z'){
      if(_chartState.pending != null){
        _chartState.pending = null;
        _setStatus('Cancelled pending start.');
      } else if(_chartState.legs.length){
        _chartState.legs.pop();
        _setStatus(`Undid last leg. ${_chartState.legs.length} remaining.`);
      }
      _renderFrame();
      _updateLegPanel();
    } else if(e.key.toLowerCase() === 'd' || e.key.toLowerCase() === 'l'){
      _chartState.drawMode = !_chartState.drawMode;
      _chartState.pending = null;
      _updateToolbarUI();
      _setStatus(_chartState.drawMode ? '✏ Click LOW bar to start leg' : 'Draw mode paused.');
      _renderFrame();
    }
  };

  _renderFrame();
  _updateLegPanel();
}

function _setupAndMountCanvas(){
  const s = _chartState;
  const wrap = $('c15wrap');
  if(!s || !wrap) return;

  const scrollLeft = wrap.scrollLeft;
  wrap.innerHTML = '';

  const bars = s.bars, N = s.N;
  const CW = s.candleWidth;
  const GAP = s.gap;
  const UNIT = CW + GAP;
  
  // Height adjusts when enlarged!
  const PRICEH = s.enlarged ? Math.max(520, window.innerHeight - 230) : 430;
  const PL = 74, PR = 20, PT = 48, PBOT = 26, VOLH = 86;
  const TH = PT + PRICEH + VOLH + PBOT;
  const TW = Math.max(wrap.clientWidth || 900, PL + N * UNIT + PR);

  const cv = document.createElement('canvas');
  const dpr = window.devicePixelRatio || 1;
  cv.width = Math.floor(TW * dpr);
  cv.height = Math.floor(TH * dpr);
  cv.style.width = TW + 'px';
  cv.style.height = TH + 'px';
  cv.style.display = 'block';
  cv.style.cursor = 'crosshair';

  const tipEl = document.createElement('div');
  tipEl.style.cssText = 'position:absolute;background:rgba(8,8,10,.95);border:1px solid #444;color:#e8e8e8;font:11px Consolas,monospace;padding:6px 12px;pointer-events:none;display:none;z-index:20;white-space:pre;line-height:1.6;border-radius:3px;box-shadow:0 6px 18px rgba(0,0,0,.9)';

  wrap.style.position = 'relative';
  wrap.appendChild(cv);
  wrap.appendChild(tipEl);

  const ctx = cv.getContext('2d');
  ctx.scale(dpr, dpr);

  let maxP = Math.max(...bars.map(b => b.h));
  let minP = Math.min(...bars.map(b => b.l));
  const rawRng = maxP - minP || 1;
  const padTop = rawRng * 0.28;
  const padBot = rawRng * 0.08;
  maxP += padTop;
  minP = Math.max(0, minP - padBot);
  const PRNG = maxP - minP || 1;
  const maxV = Math.max(...bars.map(b => b.v)) || 1;

  const xOf = i => PL + i * UNIT;
  const xM  = i => PL + i * UNIT + CW / 2;
  const yP  = p => PT + (maxP - p) / PRNG * PRICEH;
  const yV  = v => PT + PRICEH + VOLH - (v / maxV) * (VOLH - 4);

  Object.assign(s, {
    CW, GAP, UNIT, PL, PR, PT, PRICEH, VOLH, TW, TH, xOf, xM, yP, yV, maxP, minP, PRNG, maxV,
    cv, ctx, tipEl, wrap, dpr,
  });

  // Attach mouse listeners
  cv.onmousemove = function(ev){
    const rect = cv.getBoundingClientRect();
    const mx = (ev.clientX - rect.left);
    const i = Math.max(0, Math.min(N - 1, Math.floor((mx - PL) / UNIT)));
    s.hoverI = i;

    // Tooltip
    const b = bars[i];
    const chg = i > 0 ? ((b.c / bars[i - 1].c - 1) * 100).toFixed(2) + '%' : '-';
    const sessTag = (b.pm || b.sess === 'pm') ? ' [PRE-MARKET]' : '';
    tipEl.innerHTML = b.t + sessTag + '\nO ' + fmtP(b.o) + '  H ' + fmtP(b.h) + '  L ' + fmtP(b.l) + '  C ' + fmtP(b.c) + '\nVol ' + fmtV(b.v) + '  Chg ' + chg;
    const offX = Math.min(ev.offsetX + 14, wrap.offsetWidth - 230);
    tipEl.style.left = Math.max(10, offX) + 'px';
    tipEl.style.top = Math.max(8, ev.offsetY - 70) + 'px';
    tipEl.style.display = 'block';

    _renderFrame();
  };

  cv.onmouseleave = function(){
    s.hoverI = -1;
    tipEl.style.display = 'none';
    _renderFrame();
  };

  cv.onmousedown = function(ev){
    if(!s.drawMode) return;
    const rect = cv.getBoundingClientRect();
    const mx = (ev.clientX - rect.left);
    const i = Math.max(0, Math.min(N - 1, Math.floor((mx - PL) / UNIT)));
    s.dragStartI = i;
    s.dragStartTime = Date.now();
    s.isDragging = true;

    if(s.pending == null){
      s.pending = i;
      _setStatus(`📍 Leg ${s.legs.length + 1} LOW set: ${bars[i].t} (${fmtP(bars[i].l)}) — Now click or drag to the HIGH bar`);
      _renderFrame();
    }
  };

  cv.onmouseup = function(ev){
    if(!s.drawMode) return;
    const rect = cv.getBoundingClientRect();
    const mx = (ev.clientX - rect.left);
    const i = Math.max(0, Math.min(N - 1, Math.floor((mx - PL) / UNIT)));
    const duration = Date.now() - (s.dragStartTime || 0);

    if(s.isDragging && s.pending != null && i !== s.pending && duration > 160){
      _confirmLeg(s.pending, i);
      s.pending = null;
      s.isDragging = false;
      return;
    }
    s.isDragging = false;
  };

  cv.onclick = function(ev){
    if(!s.drawMode) return;
    const rect = cv.getBoundingClientRect();
    const mx = (ev.clientX - rect.left);
    const i = Math.max(0, Math.min(N - 1, Math.floor((mx - PL) / UNIT)));

    if(s.pending != null && i !== s.pending){
      _confirmLeg(s.pending, i);
      s.pending = null;
    }
  };

  if(scrollLeft) wrap.scrollLeft = scrollLeft;
  _renderFrame();
}

function _confirmLeg(i1, i2){
  const s = _chartState;
  const loI = Math.min(i1, i2), hiI = Math.max(i1, i2);
  const loPx = s.bars[loI].l, hiPx = s.bars[hiI].h;
  s.legs.push({loI, hiI, loPx, hiPx});
  const pctG = ((hiPx / loPx - 1) * 100).toFixed(0);
  _setStatus(`✅ Leg ${s.legs.length} saved (+${pctG}%). Click next bar to start Leg ${s.legs.length + 1}, or click Undo.`);
  _renderFrame();
  _updateLegPanel();
}

function _renderFrame(){
  const s = _chartState;
  if(!s) return;
  const {ctx, bars, N, CW, UNIT, PL, PR, PT, PRICEH, VOLH, TW, TH, xOf, xM, yP, yV, maxP, minP, PRNG, maxV} = s;
  const bzChecked = document.getElementById('bz15') && document.getElementById('bz15').checked;

  // 1. Regular session background (deep obsidian)
  ctx.fillStyle = '#070709';
  ctx.fillRect(0, 0, TW, TH);

  // 2. PREMARKET LIGHT GREY BACKGROUND!
  ctx.fillStyle = 'rgba(255, 255, 255, 0.09)'; // distinct light grey tint
  for(let i = 0; i < N; i++){
    if(bars[i].pm || bars[i].sess === 'pm'){
      ctx.fillRect(xOf(i), PT, UNIT, PRICEH + VOLH);
    }
  }

  // Session tags & 9:30 AM market open vertical markers
  for(let i = 0; i < N; i++){
    const isPM = bars[i].pm || bars[i].sess === 'pm';
    const prevPM = i > 0 && (bars[i-1].pm || bars[i-1].sess === 'pm');
    if(isPM && !prevPM){
      ctx.fillStyle = '#8e8e9e';
      ctx.font = 'bold 9.5px Consolas,monospace';
      ctx.textAlign = 'left';
      ctx.fillText('PRE-MARKET', xOf(i) + 4, PT - 8);
    }
    if(prevPM && !isPM){
      const openX = xOf(i);
      ctx.strokeStyle = 'rgba(255, 210, 63, 0.5)';
      ctx.lineWidth = 1;
      ctx.setLineDash([3, 3]);
      ctx.beginPath();
      ctx.moveTo(openX, PT - 18);
      ctx.lineTo(openX, PT + PRICEH + VOLH);
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = '#ffd23f';
      ctx.font = 'bold 9.5px Consolas,monospace';
      ctx.textAlign = 'left';
      ctx.fillText('OPEN 9:30', openX + 3, PT - 8);
    }
  }

  // Price grid lines & axis labels
  const step = niceNum(PRNG / 6);
  let pp = Math.ceil(minP / step) * step;
  while(pp <= maxP){
    const y = yP(pp);
    ctx.strokeStyle = '#141418';
    ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(PL, y); ctx.lineTo(TW - PR, y); ctx.stroke();
    ctx.fillStyle = '#666';
    ctx.font = '10px Consolas,monospace';
    ctx.textAlign = 'right';
    ctx.fillText(fmtP(pp), PL - 6, y + 3.5);
    pp = Math.round((pp + step) * 1e5) / 1e5;
  }

  // Bone Zone Cloud & Bands (under candles)
  const BZ = bzChecked ? boneZone15(bars) : null;
  const clipPrice = () => { ctx.save(); ctx.beginPath(); ctx.rect(PL, PT, TW - PL - PR, PRICEH); ctx.clip(); };

  if(BZ){
    clipPrice();
    for(let i = 1; i < N; i++){
      if(BZ.e9[i] == null || BZ.e20[i] == null) continue;
      ctx.fillStyle = BZ.e9[i] > BZ.e20[i] ? 'rgba(0,220,100,0.18)' : 'rgba(255,60,60,0.18)';
      ctx.beginPath();
      ctx.moveTo(xM(i - 1), yP(BZ.e9[i - 1]));
      ctx.lineTo(xM(i), yP(BZ.e9[i]));
      ctx.lineTo(xM(i), yP(BZ.e20[i]));
      ctx.lineTo(xM(i - 1), yP(BZ.e20[i - 1]));
      ctx.closePath();
      ctx.fill();
    }
    ['bu', 'bl'].forEach(k => {
      ctx.strokeStyle = 'rgba(160,160,160,0.38)';
      ctx.lineWidth = 1;
      ctx.setLineDash([3, 3]);
      ctx.beginPath();
      let on = false;
      for(let i = 0; i < N; i++){
        if(BZ[k][i] == null){ on = false; continue; }
        const y = yP(BZ[k][i]);
        if(!on){ ctx.moveTo(xM(i), y); on = true; } else ctx.lineTo(xM(i), y);
      }
      ctx.stroke();
      ctx.setLineDash([]);
    });
    ctx.restore();
  }

  // Volume Separator
  ctx.strokeStyle = '#1e1e24';
  ctx.lineWidth = 1;
  ctx.beginPath(); ctx.moveTo(PL, PT + PRICEH); ctx.lineTo(TW - PR, PT + PRICEH); ctx.stroke();
  ctx.fillStyle = '#444';
  ctx.font = '9px Consolas,monospace';
  ctx.textAlign = 'left';
  ctx.fillText('VOL', PL + 4, PT + PRICEH + 11);

  // Volume Bars
  bars.forEach((b, i) => {
    ctx.fillStyle = b.c >= b.o ? 'rgba(38,217,116,0.35)' : 'rgba(255,77,77,0.35)';
    const x = xOf(i), yt = yV(b.v), yb = PT + PRICEH + VOLH - 2;
    ctx.fillRect(x, yt, CW, yb - yt);
  });

  // Solid Candles (crisp green & red fill with 1.2px wicks)
  bars.forEach((b, i) => {
    const isGreen = b.c >= b.o;
    const col = isGreen ? '#26d974' : '#ff4d4d';
    ctx.strokeStyle = col;
    ctx.fillStyle = col;
    ctx.lineWidth = 1.2;
    const xm = xM(i);
    // Wick
    ctx.beginPath(); ctx.moveTo(xm, yP(b.h)); ctx.lineTo(xm, yP(b.l)); ctx.stroke();
    // Solid Body
    const bt = yP(Math.max(b.o, b.c));
    const bh = Math.max(1.5, yP(Math.min(b.o, b.c)) - bt);
    ctx.fillRect(xOf(i), bt, CW, bh);
  });

  // Bone Zone EMA Lines (on top of candles)
  if(BZ){
    clipPrice();
    [['e9', 'rgb(215,255,110)'], ['e20', 'rgb(255,165,0)']].forEach(([k, col]) => {
      ctx.strokeStyle = col;
      ctx.lineWidth = 1.6;
      ctx.beginPath();
      let on = false;
      for(let i = 0; i < N; i++){
        const y = yP(BZ[k][i]);
        if(!on){ ctx.moveTo(xM(i), y); on = true; } else ctx.lineTo(xM(i), y);
      }
      ctx.stroke();
    });
    ctx.restore();
  }

  // Confirmed Legs
  const BC = ['#26d974', '#29a8ff', '#ffd23f', '#ff7850', '#b464ff', '#00e5ff'];
  const flt = s.data.float_val;

  s.legs.forEach((lg, li) => {
    const col = BC[li % BC.length];
    const x1 = xM(lg.loI), x2 = xM(lg.hiI);
    const y1 = yP(lg.loPx), y2 = yP(lg.hiPx);

    // Subtle soft shaded region
    ctx.fillStyle = col + '0c';
    ctx.fillRect(Math.min(x1, x2) - CW / 2, PT, Math.abs(x2 - x1) + CW, PRICEH);

    // Subtle boundary guidelines
    ctx.strokeStyle = col + '55'; ctx.lineWidth = 1; ctx.setLineDash([3, 3]);
    ctx.beginPath(); ctx.moveTo(x1, PT); ctx.lineTo(x1, PT + PRICEH); ctx.stroke();
    ctx.beginPath(); ctx.moveTo(x2, PT); ctx.lineTo(x2, PT + PRICEH); ctx.stroke();
    ctx.setLineDash([]);

    // Solid Connecting Arrow Low -> High
    _drawArrow(ctx, x1, y1, x2, y2, col, 10);

    // Leg Stats & Badge
    const pctG = (lg.hiPx / lg.loPx - 1) * 100;
    const dolG = lg.hiPx - lg.loPx;
    const lo = Math.min(lg.loI, lg.hiI), hi = Math.max(lg.loI, lg.hiI);
    const vol = bars.slice(lo, hi + 1).reduce((a, b) => a + b.v, 0);
    const dv  = bars.slice(lo, hi + 1).reduce((a, b) => a + (b.dv ?? b.v * (b.c || b.h)), 0);
    const barsCount = hi - lo + 1;

    let lines = [];
    if(s.badgeMode === 'compact'){
      lines = [
        `L${li + 1}  +${pctG.toFixed(0)}%  (+$${dolG.toFixed(2)})`,
        `Vol ${fmtV(vol)} · $Vol ${fmtDv(dv)} · Rot ${fmtRot(vol, flt)}`,
      ];
    } else {
      lines = [
        `L${li + 1}  +${pctG.toFixed(0)}%  (+$${dolG.toFixed(2)})`,
        `Vol: ${fmtV(vol)} | $Vol: ${fmtDv(dv)}`,
        `Float rot: ${fmtRot(vol, flt)}`,
        `${bars[lg.loI].t} → ${bars[lg.hiI].t} (${barsCount}b)`,
      ];
    }

    ctx.font = 'bold 11px Consolas,monospace';
    const BW = Math.max(...lines.map(l => ctx.measureText(l).width)) + 16;
    const LH = 15, BH = lines.length * LH + 12;
    let bx = x2 - BW / 2;
    bx = Math.max(PL + 4, Math.min(TW - PR - BW - 4, bx));

    let by = y2 - BH - 12;
    if(by < PT + 2) by = PT + 2;

    ctx.fillStyle = 'rgba(8,8,12,0.88)';
    ctx.fillRect(bx, by, BW, BH);
    ctx.strokeStyle = col;
    ctx.lineWidth = 1.3;
    ctx.strokeRect(bx, by, BW, BH);

    lines.forEach((line, i2) => {
      ctx.fillStyle = i2 === 0 ? col : '#aaa';
      ctx.font = i2 === 0 ? 'bold 11px Consolas,monospace' : '10px Consolas,monospace';
      ctx.textAlign = 'left';
      ctx.fillText(line, bx + 8, by + 12 + i2 * LH);
    });

    // Horizontal Base Support Level
    if(s.showBases && li > 0){
      const prev = s.legs[li - 1];
      if(prev.hiI <= lg.loI){
        const baseBars = bars.slice(prev.hiI, lg.loI + 1);
        const minL = Math.min(...baseBars.map(b => b.l));
        const depth = ((prev.hiPx - minL) / prev.hiPx * 100).toFixed(0);
        const baseCount = lg.loI - prev.hiI;
        const bx1 = xM(prev.hiI);
        const bx2 = xM(lg.loI);
        const byFloor = yP(minL);

        ctx.strokeStyle = 'rgba(255,210,63,0.6)';
        ctx.lineWidth = 1.2;
        ctx.setLineDash([3, 3]);
        ctx.beginPath();
        ctx.moveTo(bx1, byFloor);
        ctx.lineTo(bx2, byFloor);
        ctx.stroke();

        ctx.strokeStyle = 'rgba(255,210,63,0.3)';
        ctx.beginPath();
        ctx.moveTo(bx1, yP(prev.hiPx));
        ctx.lineTo(bx1, byFloor);
        ctx.stroke();
        ctx.setLineDash([]);

        const baseTxt = `Base: -${depth}% (${baseCount}b)`;
        ctx.font = '10px Consolas,monospace';
        const btw = ctx.measureText(baseTxt).width + 8;
        const bmidX = (bx1 + bx2) / 2;
        ctx.fillStyle = 'rgba(10,10,14,0.9)';
        ctx.fillRect(bmidX - btw / 2, byFloor - 9, btw, 17);
        ctx.strokeStyle = 'rgba(255,210,63,0.5)';
        ctx.lineWidth = 1;
        ctx.strokeRect(bmidX - btw / 2, byFloor - 9, btw, 17);
        ctx.fillStyle = '#ffd23f';
        ctx.textAlign = 'center';
        ctx.fillText(baseTxt, bmidX, byFloor + 3.5);
      }
    }
  });

  // Active Pending Start Point
  if(s.pending != null){
    const px = xM(s.pending), py = yP(bars[s.pending].l);
    ctx.strokeStyle = '#ffd23f'; ctx.lineWidth = 1.5; ctx.setLineDash([4, 3]);
    ctx.beginPath(); ctx.moveTo(px, PT); ctx.lineTo(px, PT + PRICEH); ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle = '#ffd23f'; ctx.beginPath(); ctx.arc(px, py, 6, 0, Math.PI * 2); ctx.fill();
    ctx.fillStyle = '#ffd23f'; ctx.font = 'bold 10px Consolas,monospace'; ctx.textAlign = 'center';
    ctx.fillText('LOW', px, py - 10);

    // Live Rubber-band arrow line & floating badge
    if(s.hoverI >= 0){
      const curX = xM(s.hoverI), curY = yP(bars[s.hoverI].h);
      ctx.strokeStyle = '#ffd23f'; ctx.lineWidth = 2; ctx.setLineDash([4, 4]);
      ctx.beginPath(); ctx.moveTo(px, py); ctx.lineTo(curX, curY); ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = '#ffd23f'; ctx.beginPath(); ctx.arc(curX, curY, 5, 0, Math.PI * 2); ctx.fill();

      // Live metric pill
      const lo = Math.min(s.pending, s.hoverI), hi = Math.max(s.pending, s.hoverI);
      const pLo = bars[lo].l, pHi = bars[hi].h;
      const pct = ((pHi / pLo - 1) * 100).toFixed(0);
      const dol = (pHi - pLo).toFixed(2);
      const vol = bars.slice(lo, hi + 1).reduce((a, b) => a + b.v, 0);
      const tag = `L${s.legs.length + 1}: +${pct}% (+$${dol})  |  ${fmtV(vol)}  |  ${fmtRot(vol, flt)}`;

      ctx.font = 'bold 11px Consolas,monospace';
      const tw = ctx.measureText(tag).width + 16;
      let pillX = curX + 14;
      if(pillX + tw > TW - PR) pillX = curX - tw - 14;
      let pillY = curY - 14;
      if(pillY < PT + 14) pillY = curY + 22;

      ctx.fillStyle = 'rgba(20, 16, 0, 0.95)';
      ctx.fillRect(pillX, pillY - 12, tw, 22);
      ctx.strokeStyle = '#ffd23f';
      ctx.lineWidth = 1.5;
      ctx.strokeRect(pillX, pillY - 12, tw, 22);
      ctx.fillStyle = '#ffd23f';
      ctx.textAlign = 'left';
      ctx.fillText(tag, pillX + 8, pillY + 3);
    }
  } else if(s.drawMode && s.hoverI >= 0){
    const px = xM(s.hoverI), py = yP(bars[s.hoverI].l);
    ctx.strokeStyle = 'rgba(255,210,63,0.7)';
    ctx.lineWidth = 1;
    ctx.setLineDash([2, 2]);
    ctx.beginPath(); ctx.arc(px, py, 6, 0, Math.PI * 2); ctx.stroke();
    ctx.setLineDash([]);
  }

  // X Axis Time Labels
  const lev = Math.max(1, Math.ceil(N / Math.max(10, Math.floor(TW / 70))));
  bars.forEach((b, i) => {
    if(i % lev === 0){
      ctx.fillStyle = '#555';
      ctx.font = '9.5px Consolas,monospace';
      ctx.textAlign = 'center';
      ctx.fillText(b.t.substring(0, 5), xM(i), PT + PRICEH + VOLH + 16);
      ctx.strokeStyle = '#121216';
      ctx.lineWidth = 1;
      ctx.beginPath(); ctx.moveTo(xM(i), PT); ctx.lineTo(xM(i), PT + PRICEH); ctx.stroke();
    }
  });

  // Left axis border
  ctx.strokeStyle = '#26262c';
  ctx.lineWidth = 1;
  ctx.beginPath(); ctx.moveTo(PL, PT); ctx.lineTo(PL, PT + PRICEH + VOLH); ctx.stroke();
}

function _drawArrow(ctx, x1, y1, x2, y2, color, headLen=10){
  const angle = Math.atan2(y2 - y1, x2 - x1);
  ctx.strokeStyle = color;
  ctx.fillStyle = color;
  ctx.lineWidth = 2.4;
  ctx.beginPath();
  ctx.moveTo(x1, y1);
  ctx.lineTo(x2, y2);
  ctx.stroke();
  ctx.beginPath();
  ctx.arc(x1, y1, 4.5, 0, Math.PI * 2);
  ctx.fill();
  ctx.beginPath();
  ctx.moveTo(x2, y2);
  ctx.lineTo(x2 - headLen * Math.cos(angle - Math.PI / 6), y2 - headLen * Math.sin(angle - Math.PI / 6));
  ctx.lineTo(x2 - headLen * Math.cos(angle + Math.PI / 6), y2 - headLen * Math.sin(angle + Math.PI / 6));
  ctx.closePath();
  ctx.fill();
}

function _updateLegPanel(){
  const s = _chartState;
  if(!s) return;
  const bars = s.data.bars, flt = s.data.float_val, shares = s.data.shares_out;
  const BC = ['#26d974', '#29a8ff', '#ffd23f', '#ff7850', '#b464ff', '#00e5ff'];

  // 1. Update Ribbon directly above chart
  const ribbonEl = document.getElementById('legRibbon');
  if(ribbonEl){
    if(!s.legs.length){
      ribbonEl.innerHTML = '';
    } else {
      let rh = '';
      s.legs.forEach((lg, li) => {
        const col = BC[li % BC.length];
        const pctG = ((lg.hiPx / lg.loPx - 1) * 100).toFixed(0);
        const dolG = (lg.hiPx - lg.loPx).toFixed(2);
        const lo = Math.min(lg.loI, lg.hiI), hi = Math.max(lg.loI, lg.hiI);
        const vol = bars.slice(lo, hi + 1).reduce((a, b) => a + b.v, 0);
        rh += `<div class="leg-pill" style="border-color:${col}">`
            + `<span style="color:${col};font-weight:bold">Leg ${li + 1}</span>`
            + `<span style="color:#fff">+${pctG}% (+$${dolG})</span>`
            + `<span style="color:var(--d)">Vol: ${fmtV(vol)} · Rot: ${fmtRot(vol, flt)}</span>`
            + `<button onclick="_deleteLeg(${li})" style="background:transparent;border:0;color:#ff5a5a;cursor:pointer;padding:0 2px;font-size:12px">×</button>`
            + `</div>`;
      });
      ribbonEl.innerHTML = rh;
    }
  }

  // 2. Dynamic update of Panel 1 "Leg" card
  const legCard = document.getElementById('legCard');
  if(legCard){
    const vEl = legCard.querySelector('.v');
    const sEl = legCard.querySelector('.s');
    if(!s.legs.length){
      if(vEl) vEl.textContent = '0 legs drawn';
      if(sEl) sEl.textContent = 'Draw legs on 15min chart below (click or drag)';
    } else {
      const firstLo = s.legs[0].loPx;
      const lastHi = Math.max(...s.legs.map(l => l.hiPx));
      const totRun = ((lastHi / firstLo - 1) * 100);
      if(vEl) vEl.textContent = `${s.legs.length} leg${s.legs.length > 1 ? 's' : ''} (+${totRun.toFixed(0)}%)`;
      if(sEl){
        const summaries = s.legs.map((lg, idx) => {
          const g = ((lg.hiPx / lg.loPx - 1) * 100).toFixed(0);
          const d = (lg.hiPx - lg.loPx).toFixed(2);
          const lo = Math.min(lg.loI, lg.hiI), hi = Math.max(lg.loI, lg.hiI);
          const vol = bars.slice(lo, hi + 1).reduce((a, b) => a + b.v, 0);
          const rotStr = fmtRot(vol, flt);
          return `L${idx + 1} +${g}% ($${d}, ${rotStr})`;
        });
        sEl.textContent = summaries.join('  |  ');
      }
    }
  }

  // 3. Dedicated #legPanel below chart
  const el = document.getElementById('legPanel');
  if(!el) return;
  if(!s.legs.length){
    el.innerHTML = '<div style="color:var(--d);font-size:12px;padding:12px;text-align:center;border:1px dashed #222;background:#050505">No legs drawn yet. Click "Draw Leg" above and mark the LOW and HIGH bars on the chart.</div>';
    return;
  }

  let h = '<h3 style="margin:14px 0 8px;font-size:13px;color:var(--c);text-transform:uppercase;letter-spacing:.08em">Manually Drawn Legs (' + s.legs.length + ')</h3>';
  h += '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:10px">';

  s.legs.forEach((lg, li) => {
    const col = BC[li % BC.length];
    const pctG = ((lg.hiPx / lg.loPx - 1) * 100);
    const dolG = lg.hiPx - lg.loPx;
    const lo = Math.min(lg.loI, lg.hiI), hi = Math.max(lg.loI, lg.hiI);
    const vol = bars.slice(lo, hi + 1).reduce((a, b) => a + b.v, 0);
    const dv  = bars.slice(lo, hi + 1).reduce((a, b) => a + (b.dv ?? b.v * (b.c || b.h)), 0);
    const barsCount = hi - lo + 1;
    const mcapMult = (lg.hiPx / lg.loPx).toFixed(1) + 'x';
    const mcapPeak = shares ? (shares * lg.hiPx) : null;

    let baseStr = '-';
    if(li + 1 < s.legs.length){
      const nextLg = s.legs[li + 1];
      if(nextLg.loI >= lg.hiI){
        const baseBars = bars.slice(lg.hiI, nextLg.loI + 1);
        const minL = Math.min(...baseBars.map(b => b.l));
        const depth = ((lg.hiPx - minL) / lg.hiPx * 100).toFixed(0);
        baseStr = `-${depth}% pullback over ${baseBars.length} bars (${fmtP(minL)})`;
      }
    }

    const rows = [
      ['Price range', fmtP(lg.loPx) + ' → ' + fmtP(lg.hiPx)],
      ['% gain', '<strong style="color:' + col + '">+' + pctG.toFixed(0) + '%</strong>'],
      ['$ move / share', '+$' + dolG.toFixed(2)],
      ['Volume', fmtV(vol)],
      ['$ volume', fmtDv(dv)],
      ['Float rotation', fmtRot(vol, flt)],
      ['Market cap multiple', mcapMult + (mcapPeak ? ' (' + fmtDv(mcapPeak) + ')' : '')],
      ['Duration', barsCount + ' bars (15min)'],
      ['Time window (IST)', bars[lo].t + ' → ' + bars[hi].t],
    ];
    if(baseStr !== '-'){
      rows.push(['Base after leg', '<span style="color:#ffd23f">' + baseStr + '</span>']);
    }

    h += '<div style="border:1px solid ' + col + ';padding:12px 14px;background:#0d0d0d;position:relative">';
    h += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">';
    h += '  <span style="color:' + col + ';font-size:14px;font-weight:bold">Leg ' + (li + 1) + '</span>';
    h += '  <button onclick="_deleteLeg(' + li + ')" style="background:transparent;border:1px solid #444;color:#ff5a5a;font-size:11px;padding:2px 8px;height:22px;cursor:pointer">× Delete</button>';
    h += '</div>';

    rows.forEach(([k, v]) => {
      h += '<div style="display:flex;justify-content:space-between;padding:3px 0;border-bottom:1px solid #1a1a1a"><span style="color:#8a8a8a;font-size:12px">' + k + '</span><span style="color:#e8e8e8;font-size:12px;text-align:right">' + v + '</span></div>';
    });

    h += '</div>';
  });

  h += '</div>';
  el.innerHTML = h;
}

function _deleteLeg(index){
  const s = _chartState;
  if(!s || !s.legs) return;
  s.legs.splice(index, 1);
  s.pending = null;
  _setStatus(`Deleted Leg ${index + 1}.`);
  _renderFrame();
  _updateLegPanel();
}
</script></body></html>"""

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5055, debug=False)
