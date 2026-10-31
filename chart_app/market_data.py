"""Market Data calculations and formatting."""
from chart_app.application import (
    ET,
    GAP_MIN,
    HISTORY_DAYS,
    IST,
    NPTS,
    PM_START,
    RG_END,
    RG_START,
    SEC_UA,
    STOP_PCT,
    cget,
    cput,
    dt,
    math,
    np,
    os,
    pd,
    poly_get,
    requests,
    yf,
)


def med(xs):
    values = sorted(x for x in xs if x is not None)
    if not values:
        return None
    middle = len(values) // 2
    return values[middle] if len(values) % 2 else (values[middle - 1] + values[middle]) / 2


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

    volume_source = "Alpaca SIP" if alp_df is not None and not alp_df.empty else "Yahoo 1-minute volume"
    return {"date": day, "df": d, "pc": hist[-1]["c"], "hist": hist,
            "avg20": float(np.mean([r["v"] for r in hist[-20:]])), "cur": float(d["c"].iloc[-1]),
            "float": info.get("floatShares"), "so": info.get("sharesOutstanding"),
            "session_vol": session_vol, "volume_source": volume_source,
            "sector": info.get("industry") or info.get("sector") or "—", "mcap": info.get("marketCap"),
            "latest_time_ist": df.index[-1].astimezone(IST).strftime("%H:%M IST")}


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


def vwap_frame_paths(df, pc, full=True):
    """Previous-close price (P), cumulative 04:00 ET VWAP (V), and price/VWAP deviation (D)."""
    frame = vwap_twap_frame(df)
    if frame.empty or pc <= 0:
        empty = [None] * (NPTS + 1)
        return empty, empty.copy(), empty.copy()
    p_out, v_out, d_out = [0.0], [None], [None]
    mins = frame.index.hour * 60 + frame.index.minute
    observed = frame.attrs.get("observed_last", frame.index[-1])
    observed_min = int(observed.hour*60 + observed.minute)
    arr_close, arr_vw = frame.close.to_numpy(), frame.vwap.to_numpy()
    for k in range(1, NPTS + 1):
        end = PM_START + 5 * k
        if not full and observed_min < end - 5:
            p_out.append(None); v_out.append(None); d_out.append(None)
            continue
        idx = int(np.searchsorted(mins, end, side="left")) - 1
        if idx < 0:
            p_out.append(0.0); v_out.append(None); d_out.append(None)
            continue
        price, vwap = float(arr_close[idx]), float(arr_vw[idx]) if np.isfinite(arr_vw[idx]) else None
        p_out.append(round(price / pc - 1, 4))
        v_out.append(round(vwap / pc - 1, 4) if vwap else None)
        d_out.append(round(price / vwap - 1, 4) if vwap else None)
    return p_out, v_out, d_out


def vwap_twap_frame(df):
    """Canonical 1-minute 04:00-16:00 ET references; empty minutes carry price, never volume."""
    if df is None or df.empty:
        return pd.DataFrame()
    mm = df.index.hour * 60 + df.index.minute
    s = df.loc[(mm >= PM_START) & (mm < RG_END)]
    if s.empty:
        return pd.DataFrame()
    start = dt.datetime.combine(s.index[0].date(), dt.time(PM_START // 60, PM_START % 60), tzinfo=ET)
    grid = pd.date_range(start, periods=(RG_END-PM_START), freq="min")
    typical = ((s.h + s.l + s.c) / 3).groupby(level=0).last().reindex(grid).ffill()
    close = s.c.groupby(level=0).last().reindex(grid).ffill()
    volume = s.v.groupby(level=0).sum().reindex(grid).fillna(0).clip(lower=0)
    valid = typical.notna()
    tp = typical[valid]
    vol = volume[valid]
    cumvol = vol.cumsum()
    vwap = ((tp * vol).cumsum() / cumvol.replace(0, np.nan)).ffill()
    twap = tp.expanding().mean()
    active_sum = (tp * (vol > 0)).cumsum()
    active_count = (vol > 0).cumsum().replace(0, np.nan)
    active_twap = (active_sum / active_count).ffill()
    result = pd.DataFrame({"close": close.loc[valid], "tp": tp, "volume": vol,
                           "vwap": vwap, "twap": twap, "active_twap": active_twap}, index=tp.index)
    result["d_vwap"] = result.close / result.vwap - 1
    result["d_twap"] = result.close / result.twap - 1
    result["spread"] = np.log(result.vwap / result.twap)
    result["cumvol"] = result.volume.cumsum()
    result.attrs["observed_last"] = s.index[-1]
    return result


def twap_frame_paths(df, pc, full=True):
    """5-minute TWAP height, price/TWAP deviation and VWAP-TWAP spread paths."""
    frame = vwap_twap_frame(df)
    empty = [None] * (NPTS + 1)
    if frame.empty or pc <= 0:
        return empty, empty.copy(), empty.copy(), empty.copy()
    mm = (frame.index.hour * 60 + frame.index.minute).to_numpy()
    observed = frame.attrs.get("observed_last", frame.index[-1])
    observed_min = int(observed.hour*60 + observed.minute)
    tw, dtp, spread, active = [None], [None], [None], [None]
    for k in range(1, NPTS + 1):
        end = PM_START + 5*k
        if not full and observed_min < end-5:
            tw.append(None); dtp.append(None); spread.append(None); active.append(None); continue
        idx = int(np.searchsorted(mm, end, side="left"))-1
        if idx < 0:
            tw.append(None); dtp.append(None); spread.append(None); active.append(None); continue
        row = frame.iloc[idx]
        tw.append(round(float(row.twap/pc-1), 4))
        dtp.append(round(float(row.d_twap), 4))
        spread.append(round(float(row.spread), 4))
        active.append(round(float(row.active_twap/pc-1), 4) if pd.notna(row.active_twap) else None)
    return tw, dtp, spread, active


def bars2(df, include_after_hours=False):
    """1-minute OHLCV + continuous VWAP for interactive chart timeframes."""
    mm = df.index.hour * 60 + df.index.minute
    end = 1200 if include_after_hours else RG_END
    s = df[(mm >= PM_START) & (mm < end)]
    if s.empty:
        return None
    tp = (s.h + s.l + s.c) / 3
    vw = ((tp * s.v).cumsum() / s.v.cumsum().replace(0, np.nan)).ffill()
    b = s
    vb = vw.reindex(b.index)
    refs = vwap_twap_frame(df)
    tw = refs["twap"].reindex(b.index)
    atw = refs["active_twap"].reindex(b.index)
    avh, running_high, anchor_vol, anchor_dollar = [], -float("inf"), 0.0, 0.0
    for _, row in b.iterrows():
        if float(row.h) > running_high:
            running_high, anchor_vol, anchor_dollar = float(row.h), 0.0, 0.0
        v = max(0.0, float(row.v or 0))
        anchor_vol += v
        anchor_dollar += float((row.h+row.l+row.c)/3)*v
        avh.append(anchor_dollar/anchor_vol if anchor_vol else None)
    t = (b.index.hour * 60 + b.index.minute).tolist()
    return {"t": [int(x) for x in t], "o": b.o.round(4).tolist(), "h": b.h.round(4).tolist(),
            "l": b.l.round(4).tolist(), "c": b.c.round(4).tolist(),
            "v": [int(x) for x in b.v.round(0).tolist()], "vw": vb.round(4).tolist(),
            "tw": tw.round(4).tolist(), "atw": atw.round(4).tolist(), "avh": [round(x,4) if x is not None else None for x in avh]}


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
    refs = vwap_twap_frame(df)

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
    ref_hod = refs.loc[:hod_ts].iloc[-1] if not refs.empty and len(refs.loc[:hod_ts]) else None
    after_refs = refs.loc[hod_ts:] if ref_hod is not None else pd.DataFrame()
    twap_touch_row = after_refs[after_refs.index.isin(rg.loc[hod_ts:].iloc[1:].index)] if not after_refs.empty else after_refs
    if not twap_touch_row.empty:
        lows_after_hod = rg.loc[hod_ts:, "l"].reindex(twap_touch_row.index)
        touch_rows = twap_touch_row[lows_after_hod <= twap_touch_row["twap"]]
    else:
        touch_rows = pd.DataFrame()
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
    _, path_vwap, path_vwap_dev = vwap_frame_paths(df, pc, full)
    path_twap, path_twap_dev, path_vwap_twap_spread, path_active_twap = twap_frame_paths(df, pc, full)
    ref_close = refs.loc[:rg.index[-1]].iloc[-1] if not refs.empty and len(refs.loc[:rg.index[-1]]) else None
    r.update(
        rg=True, open=o, hod=hod, lod=lod, close=close, hod_min=hod_ts.hour * 60 + hod_ts.minute,
        close_min=int(mm[-1]) + 1,
        cum_to_hod=r["pm_vol"] + float(rg.v.loc[:hod_ts].sum()),
        cum_dv_to_hod=r["pm_dv"] + float((tp.loc[:hod_ts] * rg.v.loc[:hod_ts]).sum()),
        downlegs=downlegs,
        rg_vol=float(rg.v.sum()), rg_dv=float((tp * rg.v).sum()), vwap_close=vwap_close,
        twap_close=(float(ref_close["twap"]) if ref_close is not None else None),
        active_twap_close=(float(ref_close["active_twap"]) if ref_close is not None and pd.notna(ref_close["active_twap"]) else None),
        vwap_twap_spread_close=(float(math.log(ref_close["vwap"]/ref_close["twap"]))
                                if ref_close is not None and ref_close["vwap"] and ref_close["twap"] else None),
        hod_vwap=(float(ref_hod["vwap"]) if ref_hod is not None and pd.notna(ref_hod["vwap"]) else None),
        hod_twap=(float(ref_hod["twap"]) if ref_hod is not None and pd.notna(ref_hod["twap"]) else None),
        hod_price_vs_vwap_pct=((hod / float(ref_hod["vwap"]) - 1)
                               if ref_hod is not None and pd.notna(ref_hod["vwap"]) and ref_hod["vwap"] else None),
        hod_price_vs_twap_pct=((hod / float(ref_hod["twap"]) - 1)
                               if ref_hod is not None and pd.notna(ref_hod["twap"]) and ref_hod["twap"] else None),
        hod_active_twap=(float(ref_hod["active_twap"]) if ref_hod is not None and pd.notna(ref_hod["active_twap"]) else None),
        hod_vwap_twap_spread=(float(ref_hod["spread"]) if ref_hod is not None and pd.notna(ref_hod["spread"]) else None),
        twap_touch_after_hod=bool(len(touch_rows)),
        twap_touch_min=(int(touch_rows.index[0].hour*60+touch_rows.index[0].minute) if len(touch_rows) else None),
        lost_min=lost_min, vwap_never=never,
        vwap_reject=bool(lost_min is not None and lost_min < 660 and vwap_close is not None and close < vwap_close),
        vb_min=vb_min, vb_to_close=vb_to_close, vb_to_high=vb_to_high,
        close_pos=(close - lod) / rng if rng > 0 else None, close_vs_lod=close / lod - 1,
        gap_fill=min(max((o - lod) / (o - pc), 0.0), 1.0) if o > pc else None,
        f2_ret=f2_ret, f2_red=bool(f2_ret < 0), f2_l2c=f2_l2c, f5_ret=f5_ret, f5_l2c=f5_l2c,
        f5_red=bool(f5_ret < 0), f5_rng=float(first5.h.max() / first5.l.min() - 1),
        orl_broke=orl_broke, failed_bo=failed_bo, consol_bd=consol_bd, models=models,
        path=path_full(df, pc, full), path_vwap=path_vwap, path_vwap_dev=path_vwap_dev,
        path_twap=path_twap, path_twap_dev=path_twap_dev,
        path_vwap_twap_spread=path_vwap_twap_spread, path_active_twap=path_active_twap)
    return r


def chart_pack(df, m, pc, day, float_shares=None, include_after_hours=False):
    """Everything the browser needs for one 2-min chart (bars + VWAP + entry-model signals)."""
    sig = [{"name": k, "m": v["m"], "p": v["p"], "stop_m": v["stop_m"], "pnl": v["pnl"]}
           for k, v in (m.get("models") or {}).items()]
    return {"bars": bars2(df, include_after_hours), "date": str(day), "pc": pc, "open": m.get("open"), "pmh": m.get("pm_high"),
            "off": ist_off(day), "stop": STOP_PCT, "sig": sig, "float_shares": float_shares,
            "cover": ({"m": m["close_min"], "p": m["close"]} if m.get("rg") else None),
            "after_hours": bool(include_after_hours)}


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

