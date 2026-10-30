"""Runner and first-red-day analysis engine."""
import os, time, math
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import requests

API_KEY = os.getenv("POLYGON_API_KEY") or os.getenv("POLYGON_KEY")


BASE = "https://api.polygon.io"


ET = ZoneInfo("America/New_York")   # used for market session classification (do not change)


IST = ZoneInfo("Asia/Kolkata")       # used for all displayed timestamps


SEC_UA = os.getenv("SEC_USER_AGENT", "RunnerStats admin@example.com")  # SEC EDGAR requires a contact header


LOOKBACK_DAYS = 365      # daily history pulled


RALLY_WIN = 90           # max trading days a rally low can sit before its high


MIN_RUN = 1.0            # 100% low->high to count as a rally candidate


RECENT_BARS = 120        # prefer rallies whose high is within the last N bars


BASE_MIN_DAYS = 3        # a base = >=3 days w/o a new high ...


BASE_MIN_DEPTH = 0.10    # ... and >=10% pullback from the running high, then a new high


RALLY_MAX_DAYS = 120     # when you give a start date: look this far for the high


PIVOT_N = 6              # 15min swing detection: bars required on each side of a pivot high


MIN_LEG_PCT = 0.60       # minimum % gain per structural leg (smaller wiggles get merged forward)


S = requests.Session()


_aggs_cache = {}


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

    # Daily anchored references for the chart. These 15-minute candles are equally spaced,
    # so each typical price contributes one equal time interval to TWAP.
    daily_refs = {}
    for b in bars:
        day_key = b["_et"].date()
        state = daily_refs.setdefault(day_key, {"vol": 0.0, "dollar": 0.0, "time_sum": 0.0,
                                                 "active_sum": 0.0, "active_n": 0, "n": 0,
                                                 "hod": 0.0, "hod_dollar": 0.0, "hod_vol": 0.0,
                                                 "last_min": None, "last_tp": None})
        tp = b["_tp"]
        state["vol"] += float(b["v"])
        state["dollar"] += tp * float(b["v"])
        minute = b["_et"].hour*60+b["_et"].minute
        elapsed = 15 if state["last_min"] is None else max(15, minute-state["last_min"])
        if state["last_tp"] is not None and elapsed > 15:
            state["time_sum"] += state["last_tp"]*(elapsed-15); state["n"] += elapsed-15
        state["time_sum"] += tp*15; state["n"] += 15
        state["last_min"], state["last_tp"] = minute, tp
        if b["v"] > 0:
            state["active_sum"] += tp; state["active_n"] += 1
        b["_vwap"] = state["dollar"] / state["vol"] if state["vol"] else None
        b["_twap"] = state["time_sum"] / state["n"]
        b["_active_twap"] = state["active_sum"] / state["active_n"] if state["active_n"] else None
        b["_spread"] = math.log(b["_vwap"] / b["_twap"]) if b["_vwap"] and b["_twap"] else None
        if b["h"] > state["hod"]:
            state["hod"] = b["h"]; state["hod_dollar"] = 0.0; state["hod_vol"] = 0.0
        state["hod_dollar"] += tp * float(b["v"]); state["hod_vol"] += float(b["v"])
        b["_avh"] = state["hod_dollar"] / state["hod_vol"] if state["hod_vol"] else None

    # Serialize ALL fetched bars for the chart (includes post-peak context through FRD+1)
    chart_bars = [
        {
            "t": b["_t"].strftime("%m/%d %H:%M"),
            "o": round(b["o"], 4),
            "h": round(b["h"], 4),
            "l": round(b["l"], 4),
            "c": round(b["c"], 4),
            "v": int(b["v"]),
            "vwap": round(b["_vwap"], 4) if b["_vwap"] else None,
            "twap": round(b["_twap"], 4),
            "active_twap": round(b["_active_twap"], 4) if b["_active_twap"] else None,
            "avh": round(b["_avh"], 4) if b["_avh"] else None,
            "spread": round(b["_spread"], 5) if b["_spread"] is not None else None,
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
        b["_tp"] = (b["h"] + b["l"] + b["c"]) / 3
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
    refs, bar_by_min = {}, {x["_t"].hour*60+x["_t"].minute:x for x in allb}
    dollar = volume = tw_sum = active_tw_sum = active_n = 0.0
    last_tp = None
    first_price_min = None
    for minute in range(240, 960):
        b = bar_by_min.get(minute)
        if b:
            last_tp = b["_tp"]
            if first_price_min is None: first_price_min = minute
        if last_tp is None: continue
        bar_vol = float(b["v"]) if b else 0.0
        dollar += last_tp * bar_vol
        volume += bar_vol
        tw_sum += last_tp
        if bar_vol > 0:
            active_tw_sum += last_tp
            active_n += 1
        refs[minute] = {"vwap": dollar/volume if volume else None,
                        "twap": tw_sum/(minute-first_price_min+1),
                        "active_twap": active_tw_sum/active_n if active_n else None}
    hod_minute = rth[hi_i]["_t"].hour*60+rth[hi_i]["_t"].minute
    hod_ref = refs.get(hod_minute)
    after_hod = [b for b in rth[hi_i+1:] if refs.get(b["_t"].hour*60+b["_t"].minute, {}).get("twap") is not None
                 and b["l"] <= refs[b["_t"].hour*60+b["_t"].minute]["twap"]]
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
    last_ref = refs.get(rth[-1]["_t"].hour*60+rth[-1]["_t"].minute, {})
    out["vwap_close"] = last_ref.get("vwap")
    out.update(twap_close=last_ref.get("twap"), active_twap_close=last_ref.get("active_twap"),
               hod_vwap=(hod_ref or {}).get("vwap"), hod_twap=(hod_ref or {}).get("twap"),
               hod_active_twap=(hod_ref or {}).get("active_twap"),
               hod_vwap_twap_spread=(math.log(hod_ref["vwap"]/hod_ref["twap"])
                                     if hod_ref and hod_ref.get("vwap") and hod_ref.get("twap") else None),
               twap_touch_after_hod=bool(after_hod),
               twap_touch_time=(after_hod[0]["_t"].astimezone(IST).strftime("%H:%M") if after_hod else None))
    return out


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
        ["Pre-mkt vol (FRD)", big(mb["pm_vol"]) if mb else "-", (f"FRD total {big(mb['tot_vol'])} · PM $Vol {usd(mb['pm_dv'])}") if mb else "04:00-09:30 ET / 13:30-19:00 IST"],
        ["Pre-mkt $ vol (FRD)", usd(mb["pm_dv"]) if mb else "-", (f"FRD total $Vol {usd(mb['tot_dv'])} · PM {pct(div(mb['pm_dv'], mb['tot_dv']), 0)} of day" + (f" · PM high {px(mb['pm_high'])} @ {mb['pm_high_t']} IST" if mb.get("pm_high") else "")) if mb else ""],
        ["O->H vol (FRD)", big(mb["o2h_vol"]) if mb else "-", (usd(mb["o2h_dv"]) + " dollar volume · Float rotation " + rot(div(mb["o2h_vol"], flt)) + " · Market cap @ HOD " + usd(shares * mb["h"] if shares else None)) if mb else ""],
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
            ["VWAP / clock-time TWAP at HOD", f"{px(mb['hod_vwap'])} / {px(mb['hod_twap'])}" if mb and mb.get("hod_twap") else "-"],
            ["ln(VWAP / TWAP) at HOD", pct(mb.get("hod_vwap_twap_spread")) if mb else "-"],
            ["Price vs TWAP at close", pct(div(mb["c"] - mb["twap_close"], mb["twap_close"])) if mb and mb.get("twap_close") else "-"],
            ["Active-minute TWAP at close", px(mb.get("active_twap_close")) if mb else "-"],
            ["TWAP touched after HOD", f"Yes · {mb['twap_touch_time']} IST" if mb and mb.get("twap_touch_after_hod") else ("No" if mb else "-")],
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
        trows.append([dstr(r["d"]), px(r["o"]), px(r["h"]), px(r["l"]), px(r["c"]), usd(shares * r["c"] if shares else None), pct(chg, 0) if chg is not None else "-",
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

    frd_source = mb or frd or {}
    csv_summary = {
        "ticker": tkr,
        "rally_start_date": D[si]["d"].isoformat(),
        "first_red_day_date": frd["d"].isoformat() if frd else None,
        "number_of_days": ndays,
        "float_before_rally": flt,
        "float_rotation_start_to_high": div(rally_vol, flt),
        "market_cap_before_rally": mcap_before,
        "market_cap_at_rally_high": mcap_peak,
        "market_cap_high_to_before_multiple": div(mcap_peak, mcap_before),
        "dollar_volume_start_to_high": rally_dv,
        "frd_gap": mb["gap"] if mb else (div(frd["o"] - prev_close, prev_close) if frd and prev_close else None),
        "frd_premarket_volume": mb["pm_vol"] if mb else None,
        "frd_premarket_dollar_volume": mb["pm_dv"] if mb else None,
        "frd_regular_market_volume": mb["rth_vol"] if mb else (frd["v"] if frd else None),
        "frd_regular_market_dollar_volume": mb["rth_dv"] if mb else (frd["dv"] if frd else None),
        "frd_open_to_high_volume": mb["o2h_vol"] if mb else None,
        "frd_open": frd_source.get("o"),
        "frd_high": frd_source.get("h"),
        "frd_low": frd_source.get("l"),
        "frd_close": frd_source.get("c"),
        "frd_hod_time_ist": mb["hod_t"] if mb else None,
    }

    return dict(
        ticker=tkr,
        header=f"{tkr}  |  rally {dstr(D[si]['d'])} -> {dstr(D[pi]['d'])}  |  FRD {dstr(frd['d']) if frd else 'none yet'}",
        alts=alts_txt, p1=p1, p2=p2, csv_summary=csv_summary,
        chart_15=chart_15,
        table=dict(cols=["Date", "Open", "High", "Low", "Close", "Market Cap (close)", "Chg", "Volume", "$ Vol", "Cum float rot", ""], rows=trows),
        notes=notes,
    )

