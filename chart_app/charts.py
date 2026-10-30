"""Charts feature module."""
from chart_app.application import (
    RG_END,
    RG_START,
    _db_validate_session,
    app,
    bars2,
    cget,
    chart_pack,
    cput,
    dt,
    jsonify,
    live_snapshot,
    load_daily,
    metrics,
    minute_df,
    pct,
    pd,
    poly_frame,
    poly_get,
    re,
    request,
)
from chart_app import auth as _auth

@app.route("/api/session-chart")
def session_chart():
    """Load any trading session on demand so chart arrows can visit non-gap-up days."""
    user = (request.args.get("user") or "").strip().lower()
    token = (request.args.get("token") or "").strip()
    ticker = (request.args.get("ticker") or "").strip().upper()
    day = (request.args.get("date") or "").strip()
    if user not in _auth.ALLOWED_USERS or not _db_validate_session(user, token):
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
    if user not in _auth.ALLOWED_USERS or not _db_validate_session(user,token): return jsonify({"error":"Unauthorized"}),401
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
