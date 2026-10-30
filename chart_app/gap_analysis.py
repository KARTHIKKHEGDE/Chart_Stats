"""Gap Analysis calculations and formatting."""
from chart_app.application import (
    ET,
    GAP_MIN,
    GAP_REL_END,
    GAP_REL_START,
    IST,
    LOOKBACK_DAYS,
    MAX_EVENTS,
    MIN_DAY_VOL,
    MIN_PM_VOL,
    MIN_PRICE,
    NPTS,
    PM_START,
    RG_START,
    STOP_PCT,
    dt,
    math,
    np,
    quote,
)
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
    twap_frame_paths,
    vwap_frame_paths,
)

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
        "hod_vwap_twap_spread": m.get("hod_vwap_twap_spread"),
        "hod_vwap": m.get("hod_vwap"), "hod_twap": m.get("hod_twap"),
        "hod_price_vs_vwap_pct": m.get("hod_price_vs_vwap_pct"),
        "hod_price_vs_twap_pct": m.get("hod_price_vs_twap_pct"),
        "twap_touch_after_hod": m.get("twap_touch_after_hod"),
        "twap_touch_min": m.get("twap_touch_min"),
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
        "path_vwap": m.get("path_vwap"), "path_vwap_dev": m.get("path_vwap_dev"),
        "path_twap": m.get("path_twap"), "path_twap_dev": m.get("path_twap_dev"),
        "path_vwap_twap_spread": m.get("path_vwap_twap_spread"),
        "chart": chart_pack(df, m, pc, row["date"], fl),
        "outcome": outcome(h2c, o2c, close, hod),
    }
    dv = e["path_vwap_dev"] or []
    rth_dev = [x for x in dv[(RG_START-PM_START)//5:] if x is not None]
    first_loss = next((j for j, x in enumerate(rth_dev) if x < 0), None)
    reclaimed = first_loss is not None and any(x >= 0 for x in rth_dev[first_loss+1:])
    e["vwap_dev_1000"] = dv[(600-PM_START)//5] if len(dv) > (600-PM_START)//5 else None
    e["vwap_below_frac"] = (sum(x < 0 for x in rth_dev)/len(rth_dev)) if rth_dev else None
    e["vwap_failed_reclaim"] = bool(reclaimed and any(x < 0 for x in rth_dev[first_loss+1:])) if first_loss is not None else None
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
    st = {"n": len(S)}
    for k in ("faded15", "below_open", "below_vwap", "early_hod", "vwap_never", "vwap_reject", "failed_bo",
              "consol_bd", "d1_red", "below_pm", "orl_broke", "f5_red", "pm_vb", "pm_vr", "pm_vr_all"):
        st[k] = frac(S, k)
    st["twap_touch_after_hod"] = frac(S, "twap_touch_after_hod")
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
    st["med_hod_price_vs_twap_pct"] = med(vals(S, "hod_price_vs_twap_pct"))
    st["p25_hod_price_vs_twap_pct"] = pctl(vals(S, "hod_price_vs_twap_pct"), 25)
    st["p75_hod_price_vs_twap_pct"] = pctl(vals(S, "hod_price_vs_twap_pct"), 75)
    st["med_hod_vwap_twap_spread"] = med(vals(S, "hod_vwap_twap_spread"))
    st["p25_hod_vwap_twap_spread"] = pctl(vals(S, "hod_vwap_twap_spread"), 25)
    st["p75_hod_vwap_twap_spread"] = pctl(vals(S, "hod_vwap_twap_spread"), 75)
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
    s_vwap = (rate(st.get("vwap_failed_reclaim", (0, 0))) or 0) * 100
    s_spread = clamp(((st.get("med_hod_vwap_twap_spread") or 0) + .02) / .08) * 100
    s_twap_touch = (rate(st.get("twap_touch_after_hod", (0, 0))) or 0) * 100
    score = .26*s_fade + .16*s_o2c + .16*s_sq + .12*s_cons + .08*s_d2 + .05*s_n + .06*s_vwap + .05*s_spread + .06*s_twap_touch
    score = round(score)
    letter = "F"
    for th, l in [(85, "A"), (80, "A-"), (75, "B+"), (70, "B"), (65, "B-"), (60, "C+"), (55, "C"), (50, "C-"), (40, "D")]:
        if score >= th:
            letter = l
            break
    comps = [("Fade rate (red close vs open)", 26, s_fade), ("Median open→close", 16, s_o2c),
             ("Squeeze risk (p75 open→HOD)", 16, s_sq), ("Early HOD / VWAP-loss consistency", 12, s_cons),
             ("Day-1 follow-through", 8, s_d2), ("Sample size", 5, s_n),
             ("Failed VWAP reclaim", 6, s_vwap), ("VWAP premium to TWAP at HOD", 5, s_spread),
             ("TWAP touch after HOD", 6, s_twap_touch)]
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
        ["VWAP / TWAP (session)", f"{usd(lm.get('vwap_close'))} / {usd(lm.get('twap_close'))}", "b"],
        ["Price vs VWAP / TWAP", f"{pct(cur/lm['vwap_close']-1, 1) if lm.get('vwap_close') else '—'} / {pct(cur/lm['twap_close']-1, 1) if lm.get('twap_close') else '—'}", "b"],
        ["ln(VWAP / TWAP)", pct(lm.get("vwap_twap_spread_close"), 1), sc(lm.get("vwap_twap_spread_close"))],
        ["Float Rotation (so far)", f"{rot_live:.2f}x" if (rot_live is not None and rot_live > 0) else "—", "b"],
        ["Volume (so far)", f"{big(vol)} shares" if vol else "0 shares", "b"],
        ["Dollar Volume (so far)", "$" + big(dvol) if dvol else "$0", "b"],
    ]
    right = [
        ["Premarket High → Open Fade", pct(pm_fade) if pm_fade is not None else ("— (premarket)" if not open_ else "—"), sc(pm_fade) if pm_fade is not None else "d"],
        ["Intraday volume source (VWAP)", live.get("volume_source", "Yahoo 1-minute volume"), "b"],
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
        ["Median price vs VWAP at 10:00", pct(st["med_vwap_dev_1000"], 1), sc(st["med_vwap_dev_1000"])],
        ["Median RTH bars below VWAP", fmt("rate", st["med_vwap_below_frac"]), "r"],
        frow("Failed VWAP reclaim after loss", st["vwap_failed_reclaim"]),
        ["VWAP−TWAP spread at HOD (median, p25–p75)",
         f"{pct(st['med_hod_vwap_twap_spread'], 1)} · {pct(st['p25_hod_vwap_twap_spread'], 1)} to {pct(st['p75_hod_vwap_twap_spread'], 1)}", "b"],
        ["HOD price vs VWAP at HOD time (median, p25–p75)",
         f"{pct(st['med_hod_price_vs_vwap_pct'], 1)} · {pct(st['p25_hod_price_vs_vwap_pct'], 1)} to {pct(st['p75_hod_price_vs_vwap_pct'], 1)}", "b"],
        ["HOD price vs TWAP at HOD time (median, p25–p75)",
         f"{pct(st['med_hod_price_vs_twap_pct'], 1)} · {pct(st['p25_hod_price_vs_twap_pct'], 1)} to {pct(st['p75_hod_price_vs_twap_pct'], 1)}", "b"],
        frow("TWAP touched after HOD", st["twap_touch_after_hod"]),
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
    def frame_band(key, q):
        out = []
        for k in range(NPTS + 1):
            col = [e.get(key, [None] * (NPTS+1))[k] for e in S if len(e.get(key) or []) > k]
            col = [v for v in col if v is not None and np.isfinite(v)]
            out.append(round(float(np.percentile(col, q)), 4) if col else None)
        return out
    labels = [hhmm(PM_START + 5 * k, live["date"]) for k in range(NPTS + 1)]
    # Project V and price/VWAP deviation separately, weighting histories by today's as-of VWAP state.
    # Recompose expected price as P=V*(1+D), anchored to today's observed values.
    now_k = max((k for k, v in enumerate(lpath) if v is not None), default=None)
    _live_p, live_v, live_d = vwap_frame_paths(live["df"], pc, False) if live.get("df") is not None else ([None]*(NPTS+1),)*3
    live_tw, live_dt, live_spread, live_active_tw = twap_frame_paths(live["df"], pc, False) if live.get("df") is not None else ([None]*(NPTS+1),)*4
    def vwap_state_weight(event):
        """Prefer analogs whose price/VWAP relationship matches today at the cutoff."""
        history = event.get("path_vwap_dev") or []
        today_state = live_d[now_k] if now_k is not None and now_k < len(live_d) else None
        if today_state is None:
            return 1.0
        if now_k is None or len(history) <= now_k or history[now_k] is None:
            return 0.0
        # Differences are fractional returns: 0.025 means 2.5 percentage points.
        distance = abs(float(history[now_k]) - float(today_state))
        return math.exp(-0.5 * (distance / 0.025) ** 2)

    def weighted_median(values, weights):
        if not values:
            return None
        ordered = sorted(zip(values, weights), key=lambda pair: pair[0])
        total_weight = sum(weight for _, weight in ordered)
        if total_weight <= 0:
            return float(np.median(values))
        halfway = total_weight / 2
        running = 0.0
        for value, weight in ordered:
            running += weight
            if running >= halfway:
                return float(value)
        return float(ordered[-1][0])

    def project_frame(history_key, live_frame):
        out = [None] * (NPTS + 1)
        if now_k is None or live_frame[now_k] is None: return out
        out[now_k] = live_frame[now_k]
        for k in range(now_k + 1, NPTS + 1):
            pairs = [(e[history_key][k] - e[history_key][now_k], vwap_state_weight(e)) for e in S
                     if len(e.get(history_key) or []) > k and e[history_key][k] is not None
                     and e[history_key][now_k] is not None]
            if pairs:
                changes, weights = zip(*pairs)
                projected_change = weighted_median(list(changes), list(weights))
                out[k] = round(live_frame[now_k] + projected_change, 4)
        return out
    vwap_proj = project_frame("path_vwap", live_v)
    dev_proj = project_frame("path_vwap_dev", live_d)
    twap_proj = project_frame("path_twap", live_tw)
    dt_proj = project_frame("path_twap_dev", live_dt)
    spread_proj = project_frame("path_vwap_twap_spread", live_spread)
    proj = [round((1 + v) * (1 + d) - 1, 4) if v is not None and d is not None else None
            for v, d in zip(vwap_proj, dev_proj)]
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
             "vwap_med": frame_band("path_vwap", 50), "vwap_p25": frame_band("path_vwap", 25), "vwap_p75": frame_band("path_vwap", 75),
             "dev_paths": [e.get("path_vwap_dev") for e in S], "dev_med": frame_band("path_vwap_dev", 50),
             "dev_p25": frame_band("path_vwap_dev", 25), "dev_p75": frame_band("path_vwap_dev", 75),
             "twap_med": frame_band("path_twap", 50), "twap_p25": frame_band("path_twap", 25), "twap_p75": frame_band("path_twap", 75),
             "twap_paths": [e.get("path_twap") for e in S],
             "dt_med": frame_band("path_twap_dev", 50), "dt_p25": frame_band("path_twap_dev", 25), "dt_p75": frame_band("path_twap_dev", 75),
             "dt_paths": [e.get("path_twap_dev") for e in S],
             "spread_med": frame_band("path_vwap_twap_spread", 50), "spread_p25": frame_band("path_vwap_twap_spread", 25), "spread_p75": frame_band("path_vwap_twap_spread", 75),
             "spread_paths": [e.get("path_vwap_twap_spread") for e in S],
             "vwap_today": live_v, "dev_today": live_d, "twap_today": live_tw, "active_twap_today": live_active_tw, "dt_today": live_dt, "spread_today": live_spread,
             "vwap_proj": vwap_proj, "dev_proj": dev_proj, "twap_proj": twap_proj, "dt_proj": dt_proj, "spread_proj": spread_proj,
             "today": lpath, "proj": proj, "gap_labels": gap_labels, "gap_paths": historical_gap_paths,
             "gap_med": gap_band(50), "gap_p25": gap_band(25), "gap_p75": gap_band(75),
             "gap_today": gap_today, "gap_proj": gap_proj, "premarket_gap_pct": live_gap_size,
             "gap_now_k": gap_now_k, "gap_open_k": gap_open_k,
             "normalization": "prev_close", "forecast_reference": "vwap_relative_weighted_analogs",
             "now_k": now_k, "open_k": (RG_START - PM_START) // 5}
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
        "p6": "[6] 2-MIN CHARTS — premarket → close, volume + VWAP + TWAP + running-HOD AVWAP, SELL = short entry, BUY = cover",
        "p7": "[7] VWAP-CONDITIONED EXPECTED PATH",
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
