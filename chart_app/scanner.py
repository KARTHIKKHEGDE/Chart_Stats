"""Scanner feature module with live parallel Yahoo Finance scraping."""
from chart_app.application import (
    ET,
    IST,
    ROOT_DIR,
    ThreadPoolExecutor,
    _db_validate_session,
    _get_app_ui_visibility,
    app,
    as_completed,
    dt,
    jsonify,
    np,
    os,
    pd,
    re,
    request,
    requests,
    threading,
    time,
    yf,
)
from chart_app.yahoo_limiter import YAHOO_REQUEST_SEMAPHORE

_scanner_cache = {
    "at": 0.0,
    "rows": [],
    "session_date": "",
    "scanning": False,
    "candidates_count": 0,
    "error": "",
}
_scanner_cache_lock = threading.Lock()
_scanner_scan_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="scanner-master")
_scanner_tracked_symbols = set()
_SCANNER_RESCAN_SECONDS = 300

# Persistent requests session for parallel scraping
_http_session = requests.Session()
_http_adapter = requests.adapters.HTTPAdapter(pool_connections=50, pool_maxsize=50)
_http_session.mount("https://", _http_adapter)
_http_session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
})


def _scanner_log(message):
    print(f"[premarket-scanner] {message}", flush=True)


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


def _load_tc2000_universe():
    """Load symbols from us_common_stocks.txt."""
    path = ROOT_DIR / "us_common_stocks.txt"
    if not path.is_file():
        return []
    symbols = []
    seen = set()
    try:
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
    except Exception as exc:
        _scanner_log(f"Error reading TC2000 universe: {exc}")
    return symbols


def _get_scanner_candidate_symbols():
    """Scan the bundled full US ticker universe, prioritizing fresh movers."""
    candidates = []
    seen = set()

    def add_symbol(symbol):
        if not symbol or symbol.endswith((".WS", ".WT", ".RT", ".U")):
            return
        symbol = symbol.replace(".", "-").upper()
        if symbol not in seen:
            seen.add(symbol)
            candidates.append(symbol)
    
    # 1. Screen top percentage gainers on Yahoo
    try:
        q_gainers = yf.EquityQuery("and", [
            yf.EquityQuery("eq", ["region", "us"]),
            yf.EquityQuery("gt", ["percentchange", 10]),
        ])
        with YAHOO_REQUEST_SEMAPHORE:
            res_gainers = yf.screen(q_gainers, offset=0, size=250, sortField="percentchange", sortAsc=False)
        for quote in (res_gainers.get("quotes") or []):
            add_symbol(quote.get("symbol"))
    except Exception as exc:
        _scanner_log(f"Yahoo gainer screen notice: {exc}")

    # 2. Screen high dollar / share volume movers
    try:
        q_volume = yf.EquityQuery("and", [
            yf.EquityQuery("eq", ["region", "us"]),
            yf.EquityQuery("gt", ["dayvolume", 2_000_000]),
        ])
        with YAHOO_REQUEST_SEMAPHORE:
            res_vol = yf.screen(q_volume, offset=0, size=150, sortField="dayvolume", sortAsc=False)
        for quote in (res_vol.get("quotes") or []):
            add_symbol(quote.get("symbol"))
    except Exception as exc:
        _scanner_log(f"Yahoo volume screen notice: {exc}")

    # 3. Retain previously found qualifiers so any intraday stock is never dropped
    global _scanner_tracked_symbols
    with _scanner_cache_lock:
        tracked = sorted(_scanner_tracked_symbols)
    for symbol in tracked:
        add_symbol(symbol)

    # The Yahoo screens above only return a small capped subset. Append every
    # bundled US symbol so the scanner actually covers the full universe.
    universe = _load_tc2000_universe()
    for symbol in universe:
        add_symbol(symbol)
    _scanner_log(f"Universe loaded: {len(universe)} bundled symbols; {len(candidates)} unique scan candidates")

    return candidates


def _scrape_yahoo_premarket_chart(ticker):
    """Scrape 1m premarket data using Yahoo's chart endpoint:
    https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=2d&interval=1m&includePrePost=true
    Compares against yesterday's true regular session closing candle.
    """
    clean_sym = ticker.replace(".", "-")
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{clean_sym}?range=2d&interval=1m&includePrePost=true"
    try:
        with YAHOO_REQUEST_SEMAPHORE:
            resp = _http_session.get(url, timeout=5)
        if resp.status_code != 200:
            return None
        data = resp.json()
        result_list = (data.get("chart") or {}).get("result")
        if not result_list:
            return None
        chart = result_list[0]
        meta = chart.get("meta") or {}

        pre = (meta.get("currentTradingPeriod") or {}).get("pre") or {}
        p_start = pre.get("start", 0)
        p_end = pre.get("end", 0)
        reg = (meta.get("currentTradingPeriod") or {}).get("regular") or {}
        r_start = reg.get("start", 0)
        now_ts = time.time()

        # Determine true yesterday close price:
        # Before regular market opens (now_ts < r_start), Yahoo's meta.regularMarketPrice
        # holds yesterday's official regular session closing candle!
        # During/after regular market hours, meta.previousClose holds yesterday's close candle.
        is_premarket_now = (now_ts < r_start) if r_start else True
        if is_premarket_now and meta.get("regularMarketPrice"):
            prev_close = float(meta["regularMarketPrice"])
        else:
            prev_close = float(meta.get("previousClose") or meta.get("chartPreviousClose") or 0.0)

        timestamps = chart.get("timestamp") or []
        quote = ((chart.get("indicators") or {}).get("quote") or [{}])[0]
        highs = quote.get("high") or []
        closes = quote.get("close") or []
        volumes = quote.get("volume") or []

        # If still missing or zero, extract the last regular session bar before today's premarket
        if not prev_close or prev_close <= 0:
            reg_periods = (meta.get("tradingPeriods") or {}).get("regular") or []
            y_sessions = [p[0] for p in reg_periods if p and p[0].get("start", 0) < p_start]
            if y_sessions:
                last_y_sess = y_sessions[-1]
                y_bars = [c for t, c in zip(timestamps, closes) if last_y_sess.get("start", 0) <= t <= last_y_sess.get("end", 0) and c is not None]
                if y_bars:
                    prev_close = float(y_bars[-1])

        if not prev_close or prev_close <= 0:
            return None

        pm_high = 0.0
        pm_vol = 0
        pm_dvol = 0.0
        pm_high_time = 0
        today_closes = []

        # Scan premarket window
        for ts, h, c, v in zip(timestamps, highs, closes, volumes):
            if p_start <= ts < p_end:
                bar_price = h if (h is not None and h > 0) else (c or 0.0)
                if bar_price > pm_high:
                    pm_high = bar_price
                    pm_high_time = ts
                vol = v or 0
                pm_vol += vol
                if c is not None and vol > 0:
                    pm_dvol += float(c) * float(vol)
            if ts >= p_start and c is not None and c > 0:
                today_closes.append(c)

        # Current price: most recent bar today, or fulldayPrice / regularMarketPrice
        if today_closes:
            cur_price = today_closes[-1]
        elif not is_premarket_now and meta.get("regularMarketPrice"):
            cur_price = float(meta["regularMarketPrice"])
        else:
            cur_price = float(meta.get("fulldayPrice") or pm_high or prev_close)

        # If Yahoo 1m feed did not stream individual 1m volume ticks during premarket,
        # fallback to the cumulative market volume recorded in meta
        eff_vol = max(pm_vol, meta.get("regularMarketVolume") or 0)
        eff_dvol = max(pm_dvol, eff_vol * cur_price)

        if pm_high <= 0:
            return None

        # Premarket spike % and current gain % vs YESTERDAY'S CLOSE CANDLE
        pm_spike_pct = ((pm_high - prev_close) / prev_close) * 100.0
        current_pct = ((cur_price - prev_close) / prev_close) * 100.0

        # Condition 1: Premarket spike of at least 50% today
        # Condition 2: Premarket dollar volume >= $5,000,000 ($5M)
        if pm_spike_pct >= 50.0 and eff_dvol >= 5_000_000:
            status = "green" if current_pct >= 50.0 else "yellow"
            
            # Size intensity for green scaling (more size -> deeper/brighter green)
            if current_pct >= 200.0:
                green_intensity = 4
            elif current_pct >= 120.0:
                green_intensity = 3
            elif current_pct >= 75.0:
                green_intensity = 2
            else:
                green_intensity = 1

            return {
                "ticker": ticker,
                "price": round(float(cur_price), 4),
                "previous_close": round(float(prev_close), 4),
                "current_pct": round(float(current_pct), 2),
                "pm_high": round(float(pm_high), 4),
                "pm_spike_pct": round(float(pm_spike_pct), 2),
                "pm_high_time": pm_high_time,
                "volume": int(eff_vol),
                "dollar_volume": round(float(eff_dvol)),
                "status": status,
                "is_above_50": bool(current_pct >= 50.0),
                "green_intensity": green_intensity,
            }
    except Exception:
        pass
    return None


def _run_scanner_cycle():
    """Execute a single parallel premarket scan cycle."""
    session_date = _scanner_session_date()
    t0 = time.time()
    with _scanner_cache_lock:
        _scanner_cache["scanning"] = True

    try:
        candidates = _get_scanner_candidate_symbols()
        with _scanner_cache_lock:
            _scanner_cache["candidates_count"] = len(candidates)

        _scanner_log(f"Starting parallel Yahoo chart scan for {len(candidates)} candidates...")
        results = []
        with ThreadPoolExecutor(max_workers=16, thread_name_prefix="yahoo-pm-chart") as pool:
            # Keep only a bounded set of futures; candidate universes can contain
            # hundreds of symbols and eager submission retains every Future.
            candidates_iter=iter(candidates)
            pending={}
            for _ in range(min(16,len(candidates))):
                try:symbol=next(candidates_iter)
                except StopIteration:break
                pending[pool.submit(_scrape_yahoo_premarket_chart,symbol)]=symbol
            while pending:
                future=next(as_completed(tuple(pending)))
                pending.pop(future,None)
                item=future.result()
                if item:
                    results.append(item)
                    partial=sorted(results,key=lambda x:(0 if x["status"]=="green" else 1,
                        -x["current_pct"] if x["status"]=="green" else -x["pm_spike_pct"],-x["dollar_volume"]))
                    with _scanner_cache_lock:
                        _scanner_cache.update(at=time.time(),rows=list(partial),session_date=session_date,scanning=True,error="")
                try:symbol=next(candidates_iter)
                except StopIteration:continue
                pending[pool.submit(_scrape_yahoo_premarket_chart,symbol)]=symbol

        # Sort: Green first (descending by current_pct), then Yellow (descending by pm_spike_pct)
        results.sort(
            key=lambda x: (
                0 if x["status"] == "green" else 1,
                -x["current_pct"] if x["status"] == "green" else -x["pm_spike_pct"],
                -x["dollar_volume"],
            )
        )

        global _scanner_tracked_symbols
        with _scanner_cache_lock:
            for r in results:
                _scanner_tracked_symbols.add(r["ticker"])
            if len(_scanner_tracked_symbols)>500:
                _scanner_tracked_symbols=set(sorted(_scanner_tracked_symbols)[:500])
            _scanner_cache.update(
                at=time.time(),
                rows=results,
                session_date=session_date,
                scanning=False,
                error="",
            )

        green_c = sum(1 for r in results if r["status"] == "green")
        yellow_c = sum(1 for r in results if r["status"] == "yellow")
        _scanner_log(f"Scan completed in {time.time()-t0:.2f}s: {len(results)} matches ({green_c} green, {yellow_c} yellow)")
        # Resolve this after application.py has finished importing scanner.py;
        # importing it at module scope creates a circular-import startup crash.
        from chart_app import application as _application
        _application._schedule_local_warmup()
        # Enrich results asynchronously so slow news providers never block prices.
        for item in results[:8]:
            with _scanner_details_lock:
                if item["ticker"] in _scanner_details_inflight:continue
                _scanner_details_inflight.add(item["ticker"])
            _scanner_details_pool.submit(_attach_scanner_news, item["ticker"])

    except Exception as exc:
        _scanner_log(f"Scan error: {exc}")
        with _scanner_cache_lock:
            _scanner_cache["scanning"] = False
            _scanner_cache["error"] = str(exc)


def _scanner_background_loop():
    """Background daemon loop that re-scans every 60 seconds."""
    time.sleep(2)
    _run_scanner_cycle()
    while True:
        try:
            time.sleep(_SCANNER_RESCAN_SECONDS)
            _run_scanner_cycle()
        except Exception as exc:
            _scanner_log(f"Background worker loop error: {exc}")
            time.sleep(5)


# Start background worker thread
_scanner_worker_thread = threading.Thread(target=_scanner_background_loop, daemon=True, name="premarket-scanner-daemon")
_scanner_worker_thread.start()


# Retain details cache and helpers for news / tech analysis
_scanner_details = {}
_scanner_details_lock = threading.RLock()
_scanner_details_inflight = set()
_scanner_details_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="scanner-details")


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


def _fetch_yahoo_news(ticker, minimum_count=2):
    items = []
    minimum_count = max(1, min(100, int(minimum_count or 2)))
    press_wire_pattern = re.compile(
        r"globe.?newswire|pr\s*newswire|business\s*wire|newsfile|access\s*newswire|"
        r"the.?newswire|newsdirect|eqs.?news|cnw.?group", re.I
    )
    for query in (ticker, f"{ticker} press release"):
        try:
            with YAHOO_REQUEST_SEMAPHORE:
                search = yf.Search(query=query, news_count=20, enable_fuzzy_query=False, timeout=8)
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
        except Exception:
            pass
        if len(items) >= minimum_count:
            break

    unique, seen = [], set()
    for item in items:
        key = re.sub(r"\W+", " ", item["title"].lower()).strip() or item["url"]
        if key not in seen:
            unique.append(item)
            seen.add(key)
    return unique[:minimum_count]


def _get_scanner_news(ticker, limit=2):
    limit = max(1, min(100, int(limit or 2)))
    now = time.time()
    with _scanner_details_lock:
        cached = _scanner_details.get(ticker) or {}
        cached_news = list(cached.get("news") or [])
        if now - cached.get("news_at", 0) < 60 and len(cached_news) >= limit:
            return cached_news[:limit]
    fetched = _fetch_yahoo_news(ticker, minimum_count=limit)
    with _scanner_details_lock:
        cached = _scanner_details.setdefault(ticker, {})
        cached.update(news=fetched, news_at=now,
                      news_status=("Latest press releases found on Yahoo Finance" if fetched else "No press releases found"))
        if len(_scanner_details)>500:
            oldest=sorted(_scanner_details,key=lambda key:_scanner_details[key].get('news_at',0))[:len(_scanner_details)-400]
            for old_key in oldest:_scanner_details.pop(old_key,None)
    return fetched[:limit]


def _attach_scanner_news(ticker):
    try:
        news_items = _get_scanner_news(ticker, limit=1)
        headline = news_items[0]["title"] if news_items else ""
    except Exception:
        headline = ""
    with _scanner_cache_lock:
        for row in _scanner_cache.get("rows", []):
            if row.get("ticker") == ticker:
                row["latest_news"] = headline
                break
    with _scanner_details_lock:
        _scanner_details_inflight.discard(ticker)


@app.route("/api/scanner-news-more")
def scanner_news_more_api():
    username = (request.args.get("user") or "").strip().lower()
    token = (request.args.get("token") or "").strip()
    ticker = (request.args.get("ticker") or "").strip().upper()
    try:
        offset = int(request.args.get("offset") or 0)
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid news offset."}), 400
    if not username or not _db_validate_session(username, token):
        return jsonify({"error": "Session expired. Please sign in again."}), 401
    all_news = _get_scanner_news(ticker, limit=offset + 3)
    page = all_news[offset:offset + 2]
    return jsonify({"ticker": ticker, "offset": offset, "news": page, "has_more": len(all_news) > offset + 2})


@app.route("/api/scanner")
def scanner_market_scan():
    user = (request.args.get("user") or "").strip().lower()
    token = (request.args.get("token") or "").strip()
    if not user or not token or not _db_validate_session(user, token):
        return jsonify({"error": "Session expired. Please sign in again."}), 401
    if not _get_app_ui_visibility(user).get("scanner", True):
        return jsonify({"error": "Scanner access is disabled for this account."}), 403

    now = time.time()
    force = request.args.get("force") == "1"

    # Trigger at most one scan if the cache is stale or the user forced refresh.
    with _scanner_cache_lock:
        cache_age = now - _scanner_cache["at"]
        if (force or cache_age >= _SCANNER_RESCAN_SECONDS) and not _scanner_cache.get("scanning", False):
            _scanner_cache["scanning"] = True
            _scanner_scan_pool.submit(_run_scanner_cycle)
        is_scanning = _scanner_cache.get("scanning", False)
        cached_rows = list(_scanner_cache["rows"])
        updated_at = _scanner_cache["at"]
        err = _scanner_cache.get("error", "")
        candidates_count = _scanner_cache.get("candidates_count", 0)

    green_count = sum(1 for r in cached_rows if r.get("status") == "green")
    yellow_count = sum(1 for r in cached_rows if r.get("status") == "yellow")

    return jsonify({
        "tickers": cached_rows,
        "count": len(cached_rows),
        "green_count": green_count,
        "yellow_count": yellow_count,
        "updated": updated_at,
        "scanning": is_scanning,
        "candidates_count": candidates_count,
        "rescan_seconds": _SCANNER_RESCAN_SECONDS,
        "session_date": _scanner_session_date(),
        "error": err,
    })
