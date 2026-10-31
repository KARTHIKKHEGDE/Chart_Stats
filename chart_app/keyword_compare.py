"""Keyword Compare feature module."""
from chart_app.application import (
    HTML,
    KEYWORD_HTML,
    NPTS,
    PM_START,
    RG_END,
    RG_START,
    Response,
    SEC_UA,
    STOP_PCT,
    _db_validate_session,
    app,
    cget,
    chart_pack,
    cput,
    ctx_feats,
    dt,
    hashlib,
    ist_off,
    json,
    jsonify,
    live_snapshot,
    load_daily,
    load_shares_out,
    load_splits,
    log_keyword_search,
    math,
    metrics,
    minute_df,
    np,
    pd,
    quote,
    random,
    re,
    request,
    requests,
    threading,
    time,
    twap_frame_paths,
)
from chart_app import auth as _auth

_KW_FINANCING_FORMS = ("8-K,6-K,S-1,S-3,S-4,F-1,F-3,F-4,8-A,424B1,424B2,424B3,424B4,424B5,424B6,424B7,10-K,10-Q,20-F,40-F")


_kw_sec_request_lock = threading.Lock()


_kw_sec_last_request = 0.0


def _keyword_sec_get(url, params=None, timeout=25):
    """Pace SEC request starts below 10 req/s while allowing downloads to overlap."""
    global _kw_sec_last_request
    for attempt in range(3):
        with _kw_sec_request_lock:
            wait = 0.13 - (time.monotonic() - _kw_sec_last_request)
            if wait > 0:
                time.sleep(wait)
            _kw_sec_last_request = time.monotonic()
        try:
            response = requests.get(url, params=params,
                                    headers={"User-Agent": SEC_UA, "Accept-Encoding": "gzip, deflate"},
                                    timeout=timeout)
            if response.status_code not in (429, 503) or attempt == 2:
                return response
        except requests.RequestException:
            if attempt == 2:
                raise
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
    if user not in _auth.ALLOWED_USERS or not _db_validate_session(user, token):
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
    if user not in _auth.ALLOWED_USERS or not _db_validate_session(user, token):
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
    cache_key = f"kwcompare_v5_{ticker}_{day}_{window}"
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
        # Filled from cumulative session dollars/volume below, anchored at 04:00 ET.
        vwap = None
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
        point["vwap"] = cumulative_dollars / cumulative_volume if cumulative_volume else None
        point["cumvol"] = cumulative_volume / total_intraday_volume
        point["cumdollar"] = cumulative_dollars / total_intraday_dollars
        point["vwap_rel"] = ((pc * (1 + point["prev"] / 100) / point["vwap"] - 1) * 100
                             if point["prev"] is not None and point["vwap"] else None)
    intraday_by_time = {point["t"]: point for point in intraday}
    intraday = [intraday_by_time.get(minute, {"t": minute, "prev": None, "open": None, "high": None, "low": None,
                                               "vwap": None, "vwap_rel": None, "vol": None, "dollar": None,
                                               "cumvol": None, "cumdollar": None})
                for minute in range(240, 960, 5)]
    twap_height, dt_curve, spread_curve, active_twap_height = twap_frame_paths(df, pc, not today_live)
    for point in intraday:
        k = (point["t"]-240)//5
        if point.get("prev") is not None and k < len(twap_height) and twap_height[k] is not None:
            point["twap"] = pc * (1 + twap_height[k])
            point["twap_rel"] = dt_curve[k] * 100 if dt_curve[k] is not None else None
            point["vwap_twap_spread"] = spread_curve[k] * 100 if spread_curve[k] is not None else None
            point["active_twap"] = pc * (1 + active_twap_height[k]) if active_twap_height[k] is not None else None
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
        "hod_price_vs_vwap_pct": (m.get("hod_price_vs_vwap_pct") * 100
                                   if m.get("hod_price_vs_vwap_pct") is not None else None),
        "hod_price_vs_twap_pct": (m.get("hod_price_vs_twap_pct") * 100
                                   if m.get("hod_price_vs_twap_pct") is not None else None),
        "hod_vwap_twap_spread": m.get("hod_vwap_twap_spread"),
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
        "close_vs_twap": ((close/float(m["twap_close"])-1)*100 if close and m.get("twap_close") else None),
        "vwap_twap_spread_close": (math.log(float(m["vwap_close"])/float(m["twap_close"]))*100
                                   if m.get("vwap_close") and m.get("twap_close") else None),
        "close_location": close_location,
        "pullback_count": pullback_count if intraday else None,
        "pullback_max_depth": pullback_depth if intraday else None,
        "context_features": cf, "days_since_50pct_move": days_since_50,
        "daily_log_return_curve": daily_log_returns,
        "daily_path_curve": daily_path_curve,
        "volume_curve": [x.get("cumvol") for x in intraday],
        "dollar_volume_curve": [x.get("cumdollar") for x in intraday],
        "vwap_relationship_curve": [x.get("vwap_rel") for x in intraday],
        "twap_relationship_curve": [x.get("twap_rel") for x in intraday],
        "vwap_twap_spread_curve": [x.get("vwap_twap_spread") for x in intraday],
        "pre_path": pre_path, "pre_rvol": pre_vol, "pre_trend_slope": pre_slope,
        "pre_dist_high": pre_dist_high, "pre_dist_low": pre_dist_low, "pre_streak": pre_streak,
        "pre_dollar_volume": [math.log10(max(1.0, float(x.get("v") or 0)*float(x.get("vw") or x.get("c") or 0))) for x in pre_rows[1:]],
        "pre_ranges": pre_ranges,
        "post_path": post_path, "post_open_gap": post_open_gap, "post_vol": vols,
        "post_drawdown": post_drawdown, "post_runup": post_runup,
        "post_low_day": post_low_day, "follow_through": follow_through,
        "pre_coverage": min(window, max(0, len(pre_rows)-1)), "post_coverage": len(post_rows),
        "chart": chart_pack(df, m, pc, dt.date.fromisoformat(day)),
        "source": (f"Gap-Up Fade live_snapshot ({live.get('volume_source', 'Yahoo volume')}; price bars from live feed)"
                   if today_live else "Polygon 1-minute and daily aggregate candles")
    }
    features["feature_mask"] = {key: (value is not None) for key, value in features.items()
                                if key not in ("chart", "intraday", "feature_mask")}
    features["feature_masks"] = {
        "intraday": [{key: point.get(key) is not None for key in ("prev", "open", "vwap_rel", "twap_rel", "vwap_twap_spread", "cumvol", "cumdollar", "float_rotation")}
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
            for key in ("prev", "open", "high", "low", "vwap", "vwap_rel", "twap", "twap_rel", "vwap_twap_spread", "active_twap", "vol", "dollar", "cumvol", "cumdollar"):
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
    if cutoff_minute is not None and (feature.get("hod_min") is None or feature["hod_min"] + RG_START > cutoff_minute):
        out["hod_min"] = None
        for key in ("hod_price_vs_vwap_pct", "hod_price_vs_twap_pct", "hod_vwap_twap_spread"):
            out[key] = None
    out["intraday"] = bars
    out["volume_curve"] = [p.get("cumvol") for p in bars]
    out["dollar_volume_curve"] = [p.get("cumdollar") for p in bars]
    out["vwap_relationship_curve"] = [p.get("vwap_rel") for p in bars]
    out["twap_relationship_curve"] = [p.get("twap_rel") for p in bars]
    out["vwap_twap_spread_curve"] = [p.get("vwap_twap_spread") for p in bars]
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
        latest_twap = next((p["twap"] for p in reversed(price_points) if p.get("twap") is not None), None)
        out["close_vs_twap"] = (out["close"]/latest_twap-1)*100 if latest_twap else None
        out["vwap_twap_spread_close"] = (math.log(latest_vwap/latest_twap)*100 if latest_vwap and latest_twap else None)
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
    def amplitude_score(x, y, scale=5.0):
        pairs = [(float(u), float(v)) for u, v in zip(x or [], y or []) if u is not None and v is not None]
        if len(pairs) < 3: return None
        # Preserve D(t) amplitude; exponential weights emphasize the last hour.
        xx, yy = np.array([p[0] for p in pairs]), np.array([p[1] for p in pairs])
        w = np.exp(np.linspace(-2.0, 0.0, len(pairs))); w /= w.sum()
        rmse = float(np.sqrt(np.sum(w * (xx-yy)**2)))
        return max(0.0, 100.0 * (1.0-rmse/scale))
    dv_score = amplitude_score(a.get("vwap_relationship_curve"), b.get("vwap_relationship_curve"), 5.0)
    dt_score = amplitude_score(a.get("twap_relationship_curve"), b.get("twap_relationship_curve"), 7.5)
    s_score = amplitude_score(a.get("vwap_twap_spread_curve"), b.get("vwap_twap_spread_curve"), 4.0)
    d_scores = [x for x in (dv_score, dt_score, s_score) if x is not None]
    d_score = float(np.mean(d_scores)) if d_scores else None
    state_scores = [scalar_score(["close_vs_vwap", "close_vs_twap"], 15.0),
                    scalar_score(["vwap_twap_spread_close"], 4.0),
                    scalar_score(["hod_price_vs_vwap_pct", "hod_price_vs_twap_pct"], 15.0),
                    scalar_score(["hod_vwap_twap_spread"], 4.0),
                    scalar_score(["gap", "gap_retention", "hod_fade", "open_close"], 50.0)]
    state_scores = [x for x in state_scores if x is not None]
    state_score = float(np.mean(state_scores)) if state_scores else None
    if state_score is not None:
        mismatches = 0
        for key in ("close_vs_vwap", "close_vs_twap"):
            if a.get(key) is not None and b.get(key) is not None and (a[key] < 0) != (b[key] < 0): mismatches += 1
        def spread_bucket(x): return 1 if x > 2 else -1 if x < -2 else 0
        if a.get("vwap_twap_spread_close") is not None and b.get("vwap_twap_spread_close") is not None:
            mismatches += spread_bucket(a["vwap_twap_spread_close"]) != spread_bucket(b["vwap_twap_spread_close"])
        state_score = max(0.0, state_score - 20.0*mismatches)
    intraday = [curve_score([x["prev"] for x in a["intraday"]], [x["prev"] for x in b["intraday"]]),
                curve_score([x.get("open") for x in a["intraday"]], [x.get("open") for x in b["intraday"]]),
                curve_score([x.get("cumvol") for x in a["intraday"]], [x.get("cumvol") for x in b["intraday"]]),
                curve_score([x.get("cumdollar") for x in a["intraday"]], [x.get("cumdollar") for x in b["intraday"]]),
                d_score,
                scalar_score(["gap", "gap_retention", "hod_fade", "open_close", "pm_vs_open", "close_vs_vwap"], 50.0),
                scalar_score(["event_rvol", "event_rvol60"], 10.0),
                scalar_score(["opening_share"], 1.0),
                scalar_score(["hod_min"], 120.0)]
    volume_score = float(np.mean([v for v in (intraday[2], intraday[3], intraday[6], intraday[7]) if v is not None])) if any(v is not None for v in (intraday[2], intraday[3], intraday[6], intraday[7])) else None
    price_score = float(np.mean([v for v in (intraday[0], intraday[1], intraday[5], intraday[8]) if v is not None])) if any(v is not None for v in (intraday[0], intraday[1], intraday[5], intraday[8])) else None
    parts = [(d_score, .40), (state_score, .25), (volume_score, .20), (price_score, .15)]
    A = sum(v*w for v,w in parts if v is not None)/sum(w for v,w in parts if v is not None) if any(v is not None for v,w in parts) else None
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
    if user not in _auth.ALLOWED_USERS or not _db_validate_session(user, token):
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
    if user not in _auth.ALLOWED_USERS or not _db_validate_session(user, token):
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
        "target_source": (f"Gap-Up Fade live_snapshot ({target.get('source', 'live intraday feed')})"
                                         if partial_day else "Polygon historical daily and 1-minute candles"),
                        "match_source": "Polygon historical daily and 1-minute candles",
                        "weights": weights, "weight_report": weight_report,
                        "window": window, "target_post_coverage": target["post_coverage"],
                        "consensus": {"faded": sum(1 for x in sample_matches if (x["features"].get("hod_fade") or 0) < -20),
                                      "ground_higher": sum(1 for x in sample_matches if (x["features"].get("post_path") or [0])[-1] > 0),
                                      "early_hod": sum(1 for x in sample_matches if x["features"].get("hod_min") is not None and x["features"]["hod_min"] < 90)},
                        "formula": "A allocates 40% to recency-weighted price/VWAP, price/TWAP and VWAP/TWAP spread curves; 25% to close and HOD VWAP/TWAP relationships plus gap state; 20% to volume curves; and 15% to price paths. HOD relationships are measured at each event's own HOD time. B compares pre-gap return, relative-volume, log-dollar-volume, ranges and context; C compares post-event behavior. Available A/B/C components are combined with the reported learned/default weights.",
                        "source": "Polygon intraday and daily aggregate candles; SEC keyword-search rows supplied by the current page."})
    except Exception as exc:
        app.logger.exception("Keyword compare failed for %s %s", ticker, day)
        return jsonify({"error": f"Compare data unavailable: {type(exc).__name__}: {exc}"}), 502


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
                       "window.aggregateKeywordBars=aggregateEvBars;\n"
                       "window.bindUnifiedChartFeatures=bindUnifiedChartFeatures;\n"
                       "window.chartFeatureUi=chartFeatureUi;\n"
                       "window.cancelUnifiedChartSelection=cancelUnifiedChartSelection;\n})();</script>")
    page = KEYWORD_HTML.replace("<script>", shared_renderer + "<script>", 1)
    return Response(page, mimetype="text/html")
