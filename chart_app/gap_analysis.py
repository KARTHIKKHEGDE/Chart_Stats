"""Gap Analysis calculations and formatting."""
from typing import Any

import chart_app.application as app
from chart_app.market_data import (
    chart_pack,
    ctx_feats,
    float_at,
    hod_bucket,
    med,
    metrics,
    minute_df,
    pbucket,
    recent_hod_times,
    recent_open_hod_stats,
)

ET = app.ET
GAP_MIN = app.GAP_MIN
IST = app.IST
LOOKBACK_DAYS = app.LOOKBACK_DAYS
MAX_EVENTS = app.MAX_EVENTS
MIN_DAY_VOL = app.MIN_DAY_VOL
MIN_PM_VOL = app.MIN_PM_VOL
PM_GRADE_MIN_SPIKE = 0.20
MIN_PRICE = app.MIN_PRICE
STOP_PCT = app.STOP_PCT
dt = app.dt
math = app.math
np = app.np
quote = app.quote

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
    pm_high_pct = (m["pm_high"] / pc - 1) if m.get("pm_high") and pc > 0 else None
    is_gap_up = (m["open"] / pc - 1) >= GAP_MIN
    is_pm_spike = pm_high_pct is not None and pm_high_pct >= PM_GRADE_MIN_SPIKE and m["pm_vol"] > 0
    if not is_gap_up and not is_pm_spike:
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
        "pm_high_pct": pm_high_pct, "pm_spike": is_pm_spike,
        "o2h": hod / o - 1, "o2l": lod / o - 1, "o2c": o2c, "hod_close": h2c,
        "close_pos": m["close_pos"], "close_vs_lod": m["close_vs_lod"], "gap_fill": m["gap_fill"],
        "lost_min": m["lost_min"], "vb_min": m["vb_min"], "vb_to_close": m["vb_to_close"],
        "vb_to_high": m["vb_to_high"],
        "f2_ret": m["f2_ret"], "f2_red": m["f2_red"], "f2_l2c": m["f2_l2c"],
        "f5_ret": m["f5_ret"], "f5_red": m["f5_red"], "f5_l2c": m["f5_l2c"], "f5_rng": m["f5_rng"],
        "orl_broke": m["orl_broke"],
        "below_pm": (o < m["pm_high"]) if m["pm_high"] else None,
        "pm_vol": m["pm_vol"], "pm_dv": m["pm_dv"], "rg_dv": m.get("rg_dv"), "pm_fade": (o / m["pm_high"] - 1) if m["pm_high"] else None,
        "pm_range": m["pm_range"], "pm_spikes": m["pm_spikes"], "pm_dv_hi": m["pm_dv_hi"],
        "pm_vb": m["pm_vb"], "pm_vr": m["pm_vr"], "pm_vr_all": m["pm_vr_all"],
        "pmb_to_open": (o / m["pm_vb_price"] - 1) if m["pm_vb_price"] else None,
        "early_hod": m["hod_min"] < 600, "faded15": h2c <= -0.15, "below_open": o2c < 0,
        "below_vwap": (close < m["vwap_close"]) if m["vwap_close"] else None,
        "vwap_never": m["vwap_never"], "vwap_reject": m["vwap_reject"],
        "hod_price_vs_vwap_pct": m.get("hod_price_vs_vwap_pct"),
        "failed_bo": m["failed_bo"], "consol_bd": m["consol_bd"],
        "float": fl, "stale": stale,
        "cum_dv_to_hod": m.get("cum_dv_to_hod"),
        "downlegs": m.get("downlegs", []),
        "rot_hod": (m["cum_to_hod"] / fl) if fl else None,
        "rot_pm": (m["pm_vol"] / fl) if fl else None,
        "pm_rot_hi": (m["pm_cum_hi"] / fl) if (fl and m["pm_cum_hi"]) else None,
        "rot_day": ((m["pm_vol"] + m["rg_vol"]) / fl) if fl else None,
        "pm_rel": (m["pm_vol"] / cf["avgvol20"]) if cf["avgvol20"] else None,
        "bucket": pbucket(o), "models": m["models"],
        "outcome": outcome(h2c, o2c, close, hod),
    }
    e["vwap_dev_1000"] = m.get("vwap_dev_1000")
    e["vwap_below_frac"] = m.get("vwap_below_frac")
    e["vwap_failed_reclaim"] = m.get("vwap_failed_reclaim")
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
        is_gap_up = pc > 0 and r["o"] / pc - 1 >= GAP_MIN and r["o"] >= MIN_PRICE
        possible_pm_spike = pc > 0 and r.get("h", 0) / pc - 1 >= PM_GRADE_MIN_SPIKE and r.get("h", 0) >= MIN_PRICE
        if pc > 0 and (is_gap_up or possible_pm_spike) and r["v"] >= MIN_DAY_VOL:
            out.append(i)
    return out[-MAX_EVENTS:]


def nn(x):
    return x is not None and not (isinstance(x, float) and math.isnan(x))


def vals(S, k):
    return [e[k] for e in S if nn(e.get(k))]


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
    st: dict[str, Any] = {"n": len(S)}
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
    st["med_vwap_dev_1000"] = med(vals(S, "vwap_dev_1000"))
    st["med_vwap_below_frac"] = med(vals(S, "vwap_below_frac"))
    st["vwap_failed_reclaim"] = frac(S, "vwap_failed_reclaim")
    st["med_hod_price_vs_vwap_pct"] = med(vals(S, "hod_price_vs_vwap_pct"))
    st["p25_hod_price_vs_vwap_pct"] = pctl(vals(S, "hod_price_vs_vwap_pct"), 25)
    st["p75_hod_price_vs_vwap_pct"] = pctl(vals(S, "hod_price_vs_vwap_pct"), 75)
    return st


def premarket_spike_stats(events):
    fades = [e.get("pm_fade") for e in events if e.get("pm_spike") and nn(e.get("pm_fade"))]
    meaningful_fades = sum(value <= -0.10 for value in fades)
    return {"n": len(fades), "fade_rate": meaningful_fades / len(fades) if fades else None,
            "median_fade": med(fades)}


def grade(st, pm_stats=None):
    n = st["n"]
    pm_stats = pm_stats or {"n": 0, "fade_rate": None, "median_fade": None}
    pm_n = pm_stats.get("n", 0)
    if n < 1 and pm_n < 1:
        return None
    pm_magnitude = clamp(abs(pm_stats.get("median_fade") or 0) / .30) * 100
    s_pm = (.70 * (pm_stats.get("fade_rate") or 0) + .20 * pm_magnitude / 100 +
            .10 * min(pm_n / 10, 1)) * 100 if pm_n else None
    comps = []
    if n:
        fade_rate = rate(st["below_open"])
        s_fade = (fade_rate if fade_rate is not None else 0.0) * 100
        s_o2c = clamp(-(st["med_o2c"] or 0) / 0.20) * 100
        s_sq = clamp(1 - (st["p75_o2h"] or 0) / 0.60) * 100
        s_cons = (((rate(st["early_hod"]) or 0) + (rate(st["vwap_never"]) or 0)) / 2) * 100
        d1_rate = rate(st["d1_red"])
        s_d2 = d1_rate * 100 if d1_rate is not None else 50
        s_n = min(n / 20, 1) * 100
        s_vwap = (rate(st.get("vwap_failed_reclaim", (0, 0))) or 0) * 100
        s_spread = clamp(((st.get("med_hod_vwap_twap_spread") or 0) + .02) / .08) * 100
        s_twap_touch = (rate(st.get("twap_touch_after_hod", (0, 0))) or 0) * 100
        # Rebalance the removed VWAP/TWAP comparison weights across retained signals.
        base_score = (.29*s_fade + .18*s_o2c + .18*s_sq + .13*s_cons + .09*s_d2 + .06*s_n +
                      .07*s_vwap)
        score = .75 * base_score + .25 * s_pm if s_pm is not None else base_score
        comps = [("Fade rate (red close vs open)", 29, s_fade), ("Median open→close", 18, s_o2c),
                 ("Squeeze risk (p75 open→HOD)", 18, s_sq), ("Early HOD / VWAP-loss consistency", 13, s_cons),
                 ("Day-1 follow-through", 9, s_d2), ("Sample size", 6, s_n),
                 ("Failed VWAP reclaim", 7, s_vwap)]
        if s_pm is not None:
            comps = [(name, weight * .75, value) for name, weight, value in comps]
    else:
        score = s_pm
    if s_pm is not None:
        comps.append(("Premarket spike faded ≥10% before open", 25 if n else 100, s_pm))
    score = round(score)
    letter = "F"
    for th, l in [(85, "A"), (80, "A-"), (75, "B+"), (70, "B"), (65, "B-"), (60, "C+"), (55, "C"), (50, "C-"), (40, "D")]:
        if score >= th:
            letter = l
            break
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
    return (f"{fr:.0%} red · Median open→close: {pct(st['med_o2c'], 0)} · "
            f"Median squeeze (open→high): {pct(st['med_o2h'], 0)}")


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
    for name in ["2m ORB low break", "5m ORB low break", "Short at open"]:
        ms = [e["models"][name] for e in S if name in e["models"]]
        if not ms:
            continue
        eff = [x["pnl"] for x in ms]
        wins = [p for p in eff if p > 0]
        loss = [p for p in eff if p <= 0]
        wr = len(wins) / len(eff)
        exp = float(np.mean(eff))
        maes = [x["mae"] for x in ms]
        avg_risk = float(np.mean([x["stop_pct"] for x in ms]))
        stop_rule = {"2m ORB low break": "First 2m candle high",
                     "5m ORB low break": "First 5m candle high",
                     "Short at open": "20% above entry"}[name]
        rows.append([C(name), C(stop_rule), C(str(len(ms))), C(f"{wr:.0%}", "g" if wr >= .55 else "y" if wr >= .45 else "r", wr),
                     C(pct(np.mean(wins)) if wins else "—", "g"), C(pct(np.mean(loss)) if loss else "—", "r"),
                     C(pct(exp), sc(exp), exp), C(f"{exp / avg_risk:+.2f}R" if avg_risk else "—", sc(exp), exp),
                     C(pct(med(maes), 0), "y", med(maes)), C(pct(max(maes), 0), "r", max(maes))])
    return rows


def fdate(d):
    try:
        return dt.date.fromisoformat(str(d)).strftime("%m/%d/%Y")
    except Exception:
        return str(d)


def build_report(t, live, lm, events, S, notes, daily, hod_times=None):
    simset = {id(e) for e in S}
    st = stats(S)
    open_hod = recent_open_hod_stats(daily)
    if hod_times is None:
        hod_times = recent_hod_times(t, daily)
    gap_events = [e for e in events if e.get("gap", 0) >= GAP_MIN]
    pm_events = [e for e in events if e.get("pm_spike")]
    pm_st = premarket_spike_stats(pm_events)
    g = grade(st, pm_st)
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
    mcap = live["mcap"] or ((live["so"] or fl_now or 0) * cur) or None

    day_obj = live["date"]
    day_str = str(day_obj)
    day_name = day_obj.strftime("%A") if hasattr(day_obj, "strftime") else ""
    day_display = f"{fdate(day_str)} ({day_name})" if day_name else fdate(day_str)
    next_session = day_obj + dt.timedelta(days=1)
    while next_session.weekday() >= 5:
        next_session += dt.timedelta(days=1)

    # ---- [1] current day
    left = [
        ["Current Day (session)", day_display, "c"],
        ["Gap %", pct(gap_live), sc(gap_live)],
        ["Float Rotation (so far)", f"{rot_live:.2f}x" if (rot_live is not None and rot_live > 0) else "—", "b"],
        ["Volume (so far)", f"{big(vol)} shares" if vol else "0 shares", "b"],
        ["Dollar Volume (so far)", "$" + big(dvol) if dvol else "$0", "b"],
    ]
    right = [
        ["Sector", live["sector"], "b"],
        ["Float", f"{big(fl_now)} shares" if fl_now else "—", "b"],
        ["Market Cap (latest price)", "$" + big(mcap) if mcap else "—", "b"],
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
    for i, e in enumerate(gap_events, 1):
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
    spike_cols = ["Date", "Premkt Spike %", "Gap Up (RTH)", "PM High Time (IST)", "O → H (RTH)", "O → C (RTH)", "H → L (RTH)", "$ Vol Till High", "Premkt Vol", "$ Vol Reg Hr / $ Premkt"]
    spike_rows = []
    for e in pm_events:
        pm_high_pct = e.get("pm_high_pct")
        pm_dv = e.get("pm_dv")
        if pm_high_pct is None or pm_high_pct < PM_GRADE_MIN_SPIKE or pm_dv is None or pm_dv <= 0:
            continue
        ratio = None
        if e.get("pm_dv") and e.get("rg_dv") not in (None, 0):
            ratio = e["rg_dv"] / e["pm_dv"]
        spike_rows.append([
            C(fdate(e["date"]), "w", e["date"]),
            C(pct(pm_high_pct, 0), "g", pm_high_pct),
            C(pct(e.get("gap"), 0), sc(e.get("gap")), e.get("gap")),
            C(hhmm(e.get("pm_hi_min"), e["date"]), "w", e.get("pm_hi_min")),
            C(pct(e.get("o2h"), 0), sc(e.get("o2h")), e.get("o2h")),
            C(pct(e.get("o2c"), 0), sc(e.get("o2c")), e.get("o2c")),
            C(pct(e.get("o2l"), 0), sc(e.get("o2l")), e.get("o2l")),
            C(dv(e.get("pm_dv_hi")), "b", e.get("pm_dv_hi")),
            C(big(e.get("pm_vol")), "b", e.get("pm_vol")),
            C(f"{ratio:.2f}x" if ratio is not None else "—", "b", ratio),
        ])
    if not spike_rows:
        spike_rows = [[C("—", "w", None) for _ in spike_cols]]
    tables = [
        {"title": "[2A] PREMARKET SPIKE DAYS (≥20% rise with premarket trading volume)", "cols": spike_cols, "rows": spike_rows, "dim": [], "foot": "Premarket spike = PM high versus previous close. Gap Up (RTH) = RTH open versus previous close. O→H, O→C, and H→L use RTH prices; O→C is the RTH close versus RTH open."},
        {"title": f"[2] HISTORICAL GAP UPS (last 1 year, gap ≥ {GAP_MIN*100:.0f}%) — click headers to sort",
         "cols": cols2, "rows": R2, "dim": dim, "foot": foot2},
        {"title": "[3] PREMARKET ANALYSIS", "cols": cols3, "rows": R3, "dim": dim, "foot": foot3},
        {"title": "[4] INTRADAY STRUCTURE", "cols": cols4, "rows": R4, "dim": dim, "foot": foot4},
        {"title": "[5] MULTIDAY STRUCTURE", "cols": cols5, "rows": R5, "dim": dim, "foot": foot5},
    ]

    # ---- [6] per-event 2-min charts (today first)
    evch = []
    if live["df"] is not None and len(live["df"]):
        today_chart_df=live["df"]
        chart_source='live intraday data'
        try:
            from chart_app.market_data import yahoo_recent_live_bars
            yahoo_recent=yahoo_recent_live_bars(t, timeout=3)
            if yahoo_recent is not None and not yahoo_recent.empty:
                yahoo_recent=yahoo_recent[yahoo_recent.index.date==day_obj]
                if not yahoo_recent.empty:
                    cutoff=yahoo_recent.index.min()
                    today_chart_df=today_chart_df[today_chart_df.index<cutoff]
                    today_chart_df=pd.concat([today_chart_df,yahoo_recent]).sort_index()
                    today_chart_df=today_chart_df[~today_chart_df.index.duplicated(keep='last')]
                    chart_source='live intraday + recent Yahoo bars'
        except Exception:
            pass
        evch.append({"title": f"TODAY {day_str}  gap {pct(gap_live, 0)}  (live · {chart_source})", "date": day_str, "sim": True,
                     "chart": chart_pack(today_chart_df, {**lm, "_refs": None}, pc, day_obj, fl_now)})
    for i, e in enumerate(events, 1):
        evch.append({"title": f"#{i}  {fdate(e['date'])}  gap {pct(e['gap'], 0)}  O→C {pct(e['o2c'], 0)}", "date": e["date"],
                     "sim": id(e) in simset})

    # ---- extras: stats / patterns
    summary = summary_text(st)
    if g:
        verdict = verdict_text(g, st) if st["n"] else "Premarket-only grade · " + (
            f"{pm_st['fade_rate']:.0%} of {pm_st['n']} spikes faded ≥10% before the open; median high→open {pct(pm_st['median_fade'], 0)}")
        if st["n"] and pm_st["n"]:
            verdict += f" · Premarket fade ≥10%: {pm_st['fade_rate']:.0%} ({pm_st['n']} spikes)"
        gcls = "g" if g["letter"][0] in "AB" else "y" if g["letter"][0] == "C" else "r"
        head = {"grade": g["letter"], "score": f"{g['score']}/100", "gcls": gcls,
                "verdict": verdict, "total": len(gap_events), "pm_n": pm_st["n"]}
    else:
        head = {"grade": "N/A", "score": "—", "gcls": "r", "verdict": "Not enough history.", "total": len(gap_events), "pm_n": pm_st["n"]}

    mcols = ["Entry (short; exit at close / stop)", "Stop loss", "Trades", "Win %", "Avg win", "Avg loss", "Expectancy",
             "Avg R (per-model stop)", "Median squeeze", "Worst squeeze"]
    mrows = model_rows(S)
    mfoot = ("2m/5m ORB shorts enter on a break below the opening-range low; stop = the high of the first "
             "2-minute/5-minute candle, respectively. Short at open stop = 20% above entry.")

    head.update(ticker=t, n=st["n"], summary=summary,
                sub=f"Chart-Based Gap Up Analysis — Current Day: {day_display} · Times in IST", notes=notes)
    T = {
        "p1": f"[1] CURRENT DAY SETUP ({day_display} — latest data)",
        "p6": "[6] 2-MIN CHARTS — today + historical premarket spike sessions, premarket → close, volume + VWAP + running-HOD AVWAP",
        "pd": "[D] ENTRY MODELS (similar setups)",
    }
    dilution_url = f"https://finviz.com/stock?b=2&p=m&t={quote(t, safe='')}&ta=1&ty=di"
    text = [f"Ticker: {t}  |  Finviz Dilution: {dilution_url}", f"Current Day: {day_display}", head["sub"], "=" * 42,
            f"Chart-Based Fade Grade: {head['grade']} ({head['score']})",
            f"Verdict: {head['verdict']}", "", T["p1"], kv_text(left), kv_text(right)]
    if mrows:
        text += ["", T["pd"], tbl_text(mcols, mrows), mfoot]
    return {"head": head, "titles": T, "p1": {"left": left, "right": right},
            "open_hod_stats": open_hod, "hod_times": hod_times, "tables": tables,
            "evcharts": evch, "session_dates": [r["date"] for r in daily] + [day_str, next_session.isoformat()],
            "model_names": ["2m ORB low break", "5m ORB low break", "Short at open"],
        "pd": {"cols": mcols, "rows": mrows, "foot": mfoot}, "premarket_grade": pm_st,
            "dilution_url": dilution_url, "text": "\n".join(text)}


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
