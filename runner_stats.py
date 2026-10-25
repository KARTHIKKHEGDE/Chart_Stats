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


def aggs(tkr, mult, span, frm, to):
    k = (tkr, mult, span, str(frm), str(to))
    if k in _aggs_cache:
        return _aggs_cache[k]
    j = pg(f"/v2/aggs/ticker/{tkr}/range/{mult}/{span}/{frm}/{to}",
           {"adjusted": "true", "sort": "asc", "limit": 50000})
    res = (j or {}).get("results") or []
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
def load_daily(tkr):
    today = datetime.now(ET).date()
    frm = (today - timedelta(days=LOOKBACK_DAYS + 60)).isoformat()
    rows = []
    for b in aggs(tkr, 1, "day", frm, today.isoformat()):
        d = datetime.fromtimestamp(b["t"] / 1000 + 6 * 3600, timezone.utc).date()
        vw = b.get("vw") or (b["h"] + b["l"] + b["c"]) / 3
        rows.append(dict(d=d, o=b["o"], h=b["h"], l=b["l"], c=b["c"], v=b["v"], dv=b["v"] * vw))
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

    # Keep only trading hours: premarket 4am-9:30am ET + RTH 9:30am-4pm ET
    bars = []
    for b in raw:
        t_et = datetime.fromtimestamp(b["t"] / 1000, ET)
        hm = t_et.hour * 60 + t_et.minute
        if 240 <= hm < 960:
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
        }
        for b in bars
    ]

    return chart_bars, legs, ph_indices, pl_indices


def frd_intraday(tkr, d, prev_close):
    bars = aggs(tkr, 1, "minute", d.isoformat(), d.isoformat())
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
    D = load_daily(tkr)
    if len(D) < 30:
        raise RuntimeError("Not enough daily data for " + tkr)
    n = len(D)
    notes, alts_txt = [], []

    # ---- rally window
    if not start_s:
        raise RuntimeError("Enter the rally start date")
    sd = datetime.strptime(start_s, "%Y-%m-%d").date()
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
    leg_txt = "  |  ".join(f"L{i + 1} {pct(p, 0)} / {d}d" for i, (p, d) in enumerate(legs))
    if legs_15:
        m_bases = sum(1 for lg in legs_15 if lg.get("base"))
        leg_summary_15 = "  |  ".join(
            f"L{lg['leg_num']} {pct(lg['pct_gain'], 0)} (${lg['dollar_gain']:.2f}, {rot(lg['float_rot']) if lg.get('float_rot') else '-'})"
            for lg in legs_15
        )
        p1_bases = ["15m Legs & Bases", f"{len(legs_15)} legs / {m_bases} bases", f"{len(legs_15)} legs before falling (15m structural pivot analysis)"]
        p1_leg = ["15m Legs Breakdown", pct(rally_run, 0), f"low {px(D[si]['l'])} -> high {px(D[pi]['h'])} in {ndays}d  |  {leg_summary_15}"]
    else:
        p1_bases = ["Bases", str(bases), f"{len(legs)} legs  (base = >={BASE_MIN_DAYS}d, >={int(BASE_MIN_DEPTH * 100)}% pullback, then new high)"]
        p1_leg = ["Leg", pct(rally_run, 0), f"low {px(D[si]['l'])} -> high {px(D[pi]['h'])} in {ndays}d  |  {leg_txt}"]

    p1 = [
        p1_bases,
        p1_leg,
        ["Float", big(flt), flt_src],
        ["Market cap (before rally)", usd(mcap_before), f"{dstr(pre['d'])} close {px(pre['c'])}"],
        ["Float rotation (start -> high)", rot(div(rally_vol, flt)), f"{big(rally_vol)} sh over {ndays}d"],
        ["Dollar volume (start -> high)", usd(rally_dv), f"avg {usd(rally_dv / ndays)}/day"],
        ["Run O->H (FRD)", pct(mb["o2h"]) if mb else (pct(div(frd["h"] - frd["o"], frd["o"])) if frd else "-"),
         f"open {px(mb['o'] if mb else frd['o'])} -> HOD {px(mb['h'] if mb else frd['h'])}" + (f" @ {mb['hod_t']} IST" if mb else "") if frd else "no FRD yet"],
        ["H->L (FRD)", pct(mb["h2l"]) if mb else (pct(div(frd["l"] - frd["h"], frd["h"])) if frd else "-"),
         ("HOD -> lowest low after HOD" + (f"  (close {pct(mb['close_from_h'])} from HOD)" if mb else "")) if frd else ""],
        ["Gap up (FRD)", pct(mb["gap"]) if mb else (pct(div(frd["o"] - prev_close, prev_close)) if frd else "-"),
         f"prev close {px(prev_close)}" if frd else ""],
        ["Pre-mkt vol (FRD)", big(mb["pm_vol"]) if mb else "-", "13:30-19:00 IST"],
        ["Pre-mkt $ vol (FRD)", usd(mb["pm_dv"]) if mb else "-", f"PM high {px(mb['pm_high'])} @ {mb['pm_high_t']} IST" if mb and mb["pm_high"] else ""],
        ["O->H vol (FRD)", big(mb["o2h_vol"]) if mb else "-", (usd(mb["o2h_dv"]) + "  traded 19:00 -> HOD") if mb else ""],
        ["First red day", dstr(frd["d"]) if frd else "none yet", ("rally high " + dstr(D[pi]["d"])) + (" (same day)" if fi == pi else "")],
    ]

    # ================= PANEL 2 =================
    p2 = []
    p2_prof = [
        ["Rally start (low)", f"{dstr(D[si]['d'])}  {px(D[si]['l'])}"],
        ["Rally high", f"{dstr(D[pi]['d'])}  {px(D[pi]['h'])}"],
        ["Days start -> high", f"{ndays}  ({green} green)"],
        ["Biggest day move", f"{pct(big_move_pct, 0)} on {dstr(D[big_move]['d'])}"],
    ]
    if legs_15:
        p2_prof.append(["15m Legs before falling", f"{len(legs_15)} legs ({m_bases} bases)"])
        p2_prof.append(["15m Leg breakdown", leg_summary_15])
    else:
        p2_prof.append(["Legs", leg_txt])
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
    # ---- one panel per 15min leg ----
    for lg in legs_15:
        base = lg.get("base")
        rows = [
            ["Price range",      f"{px(lg['low_px'])} → {px(lg['high_px'])}"],
            ["% gain",           pct(lg["pct_gain"], 0)],
            ["$ move / share",   f"${lg['dollar_gain']:,.2f}"],
            ["Float rotation",   rot(lg["float_rot"]) if lg["float_rot"] is not None else "-"],
            ["$ volume",         usd(lg["dv"])],
            ["Volume (shares)",  big(lg["vol"])],
            ["Duration (15m bars)", str(lg["bars"])],
            ["Time window (IST)", f"{lg['start_t']} → {lg['end_t']}"],
        ]
        if base:
            rows += [
                ["-" * 20, ""],
                ["Base after leg",      ""],
                ["  Base low",          px(base["low_px"])],
                ["  Pullback depth",    pct(base["depth"], 0)],
                ["  Base duration (15m)", str(base["bars"]) + " bars"],
                ["  Base window (IST)", f"{base['start_t']} → {base['end_t']}"],
            ]
        else:
            rows += [
                ["-" * 20, ""],
                ["Status", "Final leg before rally peak / fall"],
            ]
        p2.append([f"Leg {lg['leg_num']} (15min pivot structure)", rows])
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
            "legs": [{
                "leg_num":      lg["leg_num"],
                "lo_bar_i":     lg["lo_bar_i"],
                "hi_bar_i":     lg["hi_bar_i"],
                "base_lo_bar_i": lg.get("base_lo_bar_i"),
                "low_px":       lg["low_px"],
                "high_px":      lg["high_px"],
                "pct_txt":      pct(lg["pct_gain"], 0),
                "dollar_txt":   "$" + f"{lg['dollar_gain']:,.2f}",
                "rot_txt":      rot(lg["float_rot"]) if lg.get("float_rot") is not None else "-",
                "dv_txt":       usd(lg["dv"]),
                "vol_txt":      big(lg["vol"]),
                "base_depth_txt": pct(lg["base"]["depth"], 0) if lg.get("base") else None,
                "base_bars_txt":  str(lg["base"]["bars"]) + " bars" if lg.get("base") else None,
                "base_lo_px":     lg["base"]["low_px"] if lg.get("base") else None,
            } for lg in legs_15],
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
:root{--g:#3ddc84;--r:#ff5a5a;--y:#ffd23f;--b:#6cc4ff;--c:#29a8ff;--w:#e8e8e8;--d:#8a8a8a;--line:#333}
*{box-sizing:border-box}
body{margin:0;background:#000;color:var(--w);font:15px/1.45 Consolas,"SF Mono",Menlo,monospace}
.bar{display:flex;gap:10px;padding:20px 34px;flex-wrap:wrap;align-items:center;border-bottom:1px solid var(--line)}
input[type=text],input[type=number]{background:#0d0d0d;border:1px solid var(--line);color:#fff;font:inherit;font-size:18px;padding:0 12px;height:48px}
input[type=date]{background:#0d0d0d;border:1px solid var(--line);color:#fff;font:inherit;font-size:14px;padding:0 10px;height:48px;color-scheme:dark}
#t{width:140px;text-transform:uppercase;font-size:20px}
button{background:#0d0d0d;border:1px solid var(--line);color:#fff;font:inherit;font-size:16px;padding:0 16px;height:48px;cursor:pointer}
button:hover{background:#1a1a1a}
label{color:var(--d);font-size:13px;display:flex;flex-direction:column;gap:3px}
#out{margin:0 34px 40px;border:1px solid #222;padding:20px 22px;background:#000}
.hdr{font-size:18px;font-weight:bold;color:var(--w);margin:0 0 4px}
.alt{color:var(--d);font-size:13px;margin-bottom:8px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:10px;margin-top:10px}
.card{border:1px solid var(--line);padding:10px 14px;background:#0d0d0d}
.card .k{color:var(--d);font-size:12px;margin-bottom:3px}
.card .v{font-size:20px;font-weight:bold;color:var(--w);margin-bottom:2px}
.card .s{color:var(--d);font-size:12px;word-break:break-word}
.g2{display:grid;grid-template-columns:repeat(auto-fill,minmax(360px,1fr));gap:12px;margin-top:10px}
.sec{border:1px solid var(--line);padding:10px 14px;background:#0d0d0d}
.sec h3{margin:0 0 8px;font-size:13px;color:var(--c);text-transform:uppercase;letter-spacing:.08em}
.row{display:flex;justify-content:space-between;gap:16px;padding:3px 0;border-bottom:1px solid #1a1a1a}
.row:last-child{border:0}.row span:first-child{color:var(--d)}.row span:last-child{color:var(--w);text-align:right}
.scroll{overflow-x:auto;margin-top:10px}
table{border-collapse:collapse;width:100%;font-size:13px}
th{text-align:left;font-weight:normal;color:var(--w);padding:3px 14px 5px 0;border-bottom:1px solid var(--line);white-space:nowrap}
td{padding:3px 14px 3px 0;white-space:nowrap;color:var(--d)}
td:first-child{color:var(--w)}tr:hover td{background:#0d0d0d}
.note{color:#a08840;font-size:12px;margin-top:4px}.err{color:var(--r)}.status{color:var(--d);font-size:13px;margin-left:4px}
h2{font-size:12px;letter-spacing:.1em;color:var(--c);text-transform:uppercase;margin:22px 0 0;border-bottom:1px solid var(--line);padding-bottom:4px}
.back-btn{margin-left:auto;border-color:var(--c);color:var(--c)}.back-btn:hover{background:#001a33}
#c15wrap{overflow-x:auto;border:1px solid var(--line);background:#050505;position:relative;margin-top:10px}
</style></head><body>
<div class="bar">
  <input id="t" type="text" placeholder="TICKER" autofocus>
  <label>Rally start date<input id="s" type="date"></label>
  <label>FRD (optional)<input id="f" type="date"></label>
  <label>Float M (optional)<input id="fl" type="number" step="any" style="width:110px"></label>
  <button id="go">Get stats</button>
  <button id="rf">Refresh</button>
  <button id="cp">Copy all</button>
  <span id="st" class="status"></span>
  <button class="back-btn" id="backbtn">← Chart Stats</button>
</div>
<div id="out"><span class="d">Enter a ticker and rally start date, then press Get stats.</span></div>
<script>
let LAST=null, API_BASE='/api';
const $=id=>document.getElementById(id);

// detect if served from /runner route (chart_stats integration)
if(location.pathname.startsWith('/runner')) API_BASE='/api/runner';

document.getElementById('backbtn').onclick=()=>{
  window.location.href = location.pathname.startsWith('/runner') ? '/' : 'javascript:history.back()';
};

async function run(){
  const t=$('t').value.trim().toUpperCase(); if(!t) return;
  if(!$('s').value){$('out').innerHTML='<span class="err">Enter the rally start date.</span>';return;}
  const q=new URLSearchParams({ticker:t,start:$('s').value,frd:$('f').value,float_m:$('fl').value});
  $('st').textContent='loading\u2026';$('out').innerHTML='';
  try{
    const r=await fetch(API_BASE+'/analyze?'+q);const j=await r.json();
    if(!r.ok){$('out').innerHTML='<span class="err">'+(j.error||'error')+'</span>';$('st').textContent='';return;}
    LAST=j;render(j);$('st').textContent='';
  }catch(e){$('out').innerHTML='<span class="err">'+e+'</span>';$('st').textContent='';}
}

function esc(s){return String(s).replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]))}

function render(j){
  let h='<div class="hdr">'+esc(j.header)+'</div>';
  if(j.alts&&j.alts.length)h+='<div class="alt">other rallies: '+j.alts.map(esc).join(' \u00b7 ')+'</div>';
  h+='<h2>Key stats</h2><div class="grid">'+j.p1.map(c=>'<div class="card"><div class="k">'+esc(c[0])+'</div><div class="v">'+esc(c[1])+'</div><div class="s">'+esc(c[2]||'')+'</div></div>').join('')+'</div>';
  if(j.chart_15&&j.chart_15.bars&&j.chart_15.bars.length){
    h+='<h2>15min Structure Chart <span style="color:var(--d);font-weight:normal;text-transform:none;letter-spacing:0;font-size:11px">  pivot-swing legs \u2014 scroll horizontally if needed</span></h2>';
    h+='<div id="c15wrap"></div>';
  }
  h+='<h2>Deep stats</h2><div class="g2">'+j.p2.map(s=>'<div class="sec"><h3>'+esc(s[0])+'</h3>'+s[1].map(r=>'<div class="row"><span>'+esc(r[0])+'</span><span>'+esc(r[1])+'</span></div>').join('')+'</div>').join('')+'</div>';
  h+='<h2>Rally days (start \u2192 FRD)</h2><div class="scroll"><table><thead><tr>'+j.table.cols.map(c=>'<th>'+esc(c)+'</th>').join('')+'</tr></thead><tbody>';
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
    setTimeout(()=>drawChart15(j.chart_15),20);
  }
}

function copyAll(){
  if(!LAST)return;const j=LAST;let t=j.header+'\n\nKEY STATS\n';
  t+=j.p1.map(c=>c[0]+': '+c[1]+(c[2]?'  ('+c[2]+')':'')).join('\n');
  j.p2.forEach(s=>{t+='\n\n'+s[0].toUpperCase()+'\n'+s[1].map(r=>r[0]+': '+r[1]).join('\n')});
  navigator.clipboard.writeText(t);$('st').textContent='copied';setTimeout(()=>$('st').textContent='',1500);
}
$('go').onclick=run;$('rf').onclick=run;$('cp').onclick=copyAll;
$('t').addEventListener('keydown',e=>{if(e.key==='Enter')run()});

// ================= CANVAS CHART =================
function fmtP(v){return v<1?'$'+v.toFixed(4):v<100?'$'+v.toFixed(2):'$'+v.toFixed(0);}
function fmtV(v){return v>=1e6?(v/1e6).toFixed(1)+'M':v>=1e3?(v/1e3).toFixed(0)+'K':String(v);}
function niceNum(v){const p=Math.pow(10,Math.floor(Math.log10(v)));const f=v/p;return(f<1.5?1:f<3?2:f<7?5:10)*p;}

function drawChart15(data){
  const wrap=$('c15wrap');
  if(!wrap||!data||!data.bars||!data.bars.length)return;
  wrap.innerHTML='';

  const bars=data.bars,legs=data.legs||[],N=bars.length;
  const pivH=new Set(data.pivot_highs||[]),pivL=new Set(data.pivot_lows||[]);

  const CW=Math.max(3,Math.min(12,Math.floor(880/N)));
  const GAP=Math.max(1,Math.ceil(CW*0.25));
  const UNIT=CW+GAP;
  const PL=72,PR=8,PT=18,PBOT=22,PRICEH=390,VOLH=90;
  const TH=PT+PRICEH+VOLH+PBOT;
  const TW=Math.max(800,PL+N*UNIT+PR);

  const cv=document.createElement('canvas');
  cv.width=TW;cv.height=TH;cv.style.cssText='display:block;cursor:crosshair';
  const tipEl=document.createElement('div');
  tipEl.style.cssText='position:absolute;background:#0d0d0d;border:1px solid #444;color:#e0e0e0;font:11px Consolas;padding:5px 10px;pointer-events:none;display:none;z-index:10;white-space:pre;line-height:1.6;border-radius:2px;box-shadow:0 4px 12px rgba(0,0,0,.8)';
  wrap.style.position='relative';
  wrap.appendChild(cv);wrap.appendChild(tipEl);
  const ctx=cv.getContext('2d');

  let maxP=Math.max(...bars.map(b=>b.h));
  let minP=Math.min(...bars.map(b=>b.l));
  const pad=(maxP-minP)*0.09;maxP+=pad;minP=Math.max(0,minP-pad*0.5);
  const PRNG=maxP-minP;
  const maxV=Math.max(...bars.map(b=>b.v));

  const xOf=i=>PL+i*UNIT;
  const xM=i=>PL+i*UNIT+CW/2;
  const yP=p=>PT+(maxP-p)/PRNG*PRICEH;
  const yV=v=>PT+PRICEH+VOLH-(v/maxV)*(VOLH-4);

  // Background
  ctx.fillStyle='#050505';ctx.fillRect(0,0,TW,TH);

  // Price grid
  const step=niceNum(PRNG/6);
  let pp=Math.ceil(minP/step)*step;
  while(pp<=maxP){
    const y=yP(pp);
    ctx.strokeStyle='#111';ctx.lineWidth=1;
    ctx.beginPath();ctx.moveTo(PL,y);ctx.lineTo(TW-PR,y);ctx.stroke();
    ctx.fillStyle='#555';ctx.font='9px Consolas';ctx.textAlign='right';
    ctx.fillText(fmtP(pp),PL-4,y+3);
    pp=Math.round((pp+step)*1e5)/1e5;
  }

  // Leg colored bands
  const LC=['rgba(61,220,132,.09)','rgba(41,168,255,.09)','rgba(255,210,63,.09)','rgba(255,120,90,.09)','rgba(180,100,255,.09)'];
  legs.forEach((lg,li)=>{
    ctx.fillStyle=LC[li%LC.length];
    const x1=xOf(lg.lo_bar_i),x2=xOf(lg.hi_bar_i)+CW;
    ctx.fillRect(x1,PT,x2-x1,PRICEH+VOLH);
  });
  // Base bands (gray)
  legs.forEach(lg=>{
    if(lg.base_lo_bar_i!=null){
      ctx.fillStyle='rgba(80,80,80,.09)';
      const x1=xOf(lg.hi_bar_i)+CW,x2=xOf(lg.base_lo_bar_i)+CW;
      if(x2>x1)ctx.fillRect(x1,PT,x2-x1,PRICEH);
    }
  });

  // Volume separator line
  ctx.strokeStyle='#1c1c1c';ctx.lineWidth=1;
  ctx.beginPath();ctx.moveTo(PL,PT+PRICEH);ctx.lineTo(TW-PR,PT+PRICEH);ctx.stroke();
  ctx.fillStyle='#333';ctx.font='9px Consolas';ctx.textAlign='left';
  ctx.fillText('VOL',PL+4,PT+PRICEH+11);

  // Volume bars
  bars.forEach((b,i)=>{
    ctx.fillStyle=b.c>=b.o?'rgba(61,220,132,.4)':'rgba(255,90,90,.4)';
    const x=xOf(i),yt=yV(b.v),yb=PT+PRICEH+VOLH-2;
    ctx.fillRect(x,yt,CW,yb-yt);
  });

  // Candlesticks
  bars.forEach((b,i)=>{
    const isG=b.c>=b.o,col=isG?'#3ddc84':'#ff5a5a';
    ctx.strokeStyle=col;ctx.fillStyle=col;ctx.lineWidth=1;
    const xm=xM(i);
    // Wick
    ctx.beginPath();ctx.moveTo(xm,yP(b.h));ctx.lineTo(xm,yP(b.l));ctx.stroke();
    // Body
    const bt=yP(Math.max(b.o,b.c)),bb=yP(Math.min(b.o,b.c));
    const bh=Math.max(1,bb-bt);
    if(isG)ctx.fillRect(xOf(i),bt,CW,bh);
    else ctx.strokeRect(xOf(i),bt,CW,bh);
  });

  // Pivot markers — ▲ at lows (green), ▼ at highs (red)
  const tri=(x,y,up,col)=>{
    ctx.fillStyle=col;ctx.beginPath();
    if(up){ctx.moveTo(x,y-6);ctx.lineTo(x+4,y);ctx.lineTo(x-4,y);}
    else{ctx.moveTo(x,y+6);ctx.lineTo(x+4,y);ctx.lineTo(x-4,y);}
    ctx.closePath();ctx.fill();
  };
  pivL.forEach(i=>{if(i>=0&&i<N)tri(xM(i),yP(bars[i].l)+9,true,'#3ddc84');});
  pivH.forEach(i=>{if(i>=0&&i<N)tri(xM(i),yP(bars[i].h)-9,false,'#ff5a5a');});

  // Leg annotation boxes at each leg high
  const BC=['#3ddc84','#29a8ff','#ffd23f','#ff7850','#b464ff'];
  legs.forEach((lg,li)=>{
    const bcolor=BC[li%BC.length];
    const hx=xM(lg.hi_bar_i),hy=yP(lg.high_px);
    const lines=[
      'Leg '+lg.leg_num+'  '+lg.pct_txt+'  '+lg.dollar_txt,
      'Float rot: '+lg.rot_txt,
      '$ vol: '+lg.dv_txt,
      ...(lg.base_depth_txt?['Base: '+lg.base_depth_txt+' / '+lg.base_bars_txt]:['(last leg \u2014 no base)']),
    ];
    ctx.font='bold 11px Consolas';
    const BW=Math.max(...lines.map(l=>ctx.measureText(l).width))+18;
    const LH=15,BH=lines.length*LH+14;
    let bx=hx-BW/2;
    bx=Math.max(PL+2,Math.min(TW-PR-BW-2,bx));
    let by=hy-BH-14;if(by<PT+2)by=hy+14;

    ctx.fillStyle='rgba(6,6,6,.95)';
    ctx.fillRect(bx,by,BW,BH);
    ctx.strokeStyle=bcolor;ctx.lineWidth=1.5;
    ctx.strokeRect(bx,by,BW,BH);

    lines.forEach((line,i2)=>{
      ctx.fillStyle=i2===0?bcolor:'#aaa';
      ctx.font=i2===0?'bold 11px Consolas':'10px Consolas';
      ctx.textAlign='left';
      ctx.fillText(line,bx+7,by+13+i2*LH);
    });
    // Dashed line: box → pivot high dot
    ctx.strokeStyle=bcolor;ctx.lineWidth=1;ctx.setLineDash([3,3]);
    const ly1=by<hy?by+BH:by;
    ctx.beginPath();ctx.moveTo(hx,ly1);ctx.lineTo(hx,hy);ctx.stroke();
    ctx.setLineDash([]);
    // Dot
    ctx.fillStyle=bcolor;ctx.beginPath();ctx.arc(hx,hy,3,0,Math.PI*2);ctx.fill();
  });

  // X axis time labels + vertical ticks
  const lev=Math.max(1,Math.ceil(N/22));
  bars.forEach((b,i)=>{
    if(i%lev===0){
      ctx.fillStyle='#444';ctx.font='9px Consolas';ctx.textAlign='center';
      ctx.fillText(b.t.substring(0,5),xM(i),PT+PRICEH+VOLH+14);
      ctx.strokeStyle='#0d0d0d';ctx.lineWidth=1;
      ctx.beginPath();ctx.moveTo(xM(i),PT);ctx.lineTo(xM(i),PT+PRICEH);ctx.stroke();
    }
  });

  // Left axis border
  ctx.strokeStyle='#222';ctx.lineWidth=1;
  ctx.beginPath();ctx.moveTo(PL,PT);ctx.lineTo(PL,PT+PRICEH+VOLH);ctx.stroke();

  // Hover tooltip + crosshair
  let lastI=-1;
  cv.onmousemove=function(ev){
    const rect=cv.getBoundingClientRect();
    const scX=cv.width/rect.width,scY=cv.height/rect.height;
    const mx=(ev.clientX-rect.left)*scX,my=(ev.clientY-rect.top)*scY;
    const i=Math.floor((mx-PL)/UNIT);
    if(i<0||i>=N){tipEl.style.display='none';lastI=-1;return;}
    if(i===lastI)return;lastI=i;
    const b=bars[i];
    const chg=i>0?((b.c/bars[i-1].c-1)*100).toFixed(2)+'%':'-';
    tipEl.innerHTML=b.t+'\nO '+fmtP(b.o)+'  H '+fmtP(b.h)+'  L '+fmtP(b.l)+'  C '+fmtP(b.c)+'\nVol '+fmtV(b.v)+'  Chg '+chg;
    const offX=Math.min(ev.offsetX+14,wrap.offsetWidth-200);
    tipEl.style.left=offX+'px';
    tipEl.style.top=Math.max(8,ev.offsetY-70)+'px';
    tipEl.style.display='block';
  };
  cv.onmouseleave=()=>{tipEl.style.display='none';lastI=-1;};
}
</script></body></html>"""

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5055, debug=False)