"""Scanner feature module."""
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

_scanner_cache = {"at": 0.0, "rows": []}


_scanner_cache_lock = threading.Lock()


_scanner_scan_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="scanner-universe")


_scanner_scan_state = {"running": False, "session_date": "", "done": 0, "total": 0,
                       "symbols_done": 0, "symbols_total": 0, "universe_total": 0,
                       "phase": "", "error": "", "completed_at": 0.0}


_SCANNER_RESCAN_SECONDS = 60


def _scanner_log(message):
    print(f"[gap-scanner] {message}", flush=True)


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


def _scanner_is_premarket_window(now=None):
    """Use the premarket mover source from the user's 13:30 IST reset to the open."""
    now = now or dt.datetime.now(IST)
    local = now.astimezone(IST)
    eastern = now.astimezone(ET)
    return (local.time() >= dt.time(13, 30) and
            eastern.weekday() < 5 and eastern.time() < dt.time(9, 30))


def _load_tc2000_scanner_symbols():
    """Load the exact US Common Stocks universe exported from the user's TC2000 list."""
    path = ROOT_DIR / "us_common_stocks.txt"
    if not path.is_file():
        raise FileNotFoundError(f"TC2000 universe file is missing: {path}")
    symbols = []
    seen = set()
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
    if not symbols:
        raise RuntimeError(f"TC2000 universe file contains no symbols: {path}")
    return symbols


def _alpaca_premarket_volumes(tickers, session_date):
    """Return cumulative 04:00–09:30 ET volume for candidate symbols from Alpaca bars."""
    key = os.getenv("ALPACA_API_KEY")
    secret = os.getenv("ALPACA_API_SECRET")
    if not key or not secret:
        raise RuntimeError("Alpaca API credentials are not configured")

    start_et = dt.datetime.combine(dt.date.fromisoformat(session_date), dt.time(4, 0), tzinfo=ET)
    start_utc = start_et.astimezone(dt.timezone.utc)
    headers = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}
    feed = os.getenv("ALPACA_FEED", "sip")
    volumes = {ticker: 0.0 for ticker in tickers}
    batch_size = 40
    for offset in range(0, len(tickers), batch_size):
        params = {
            "symbols": ",".join(tickers[offset:offset + batch_size]),
            "timeframe": "1Min",
            "start": start_utc.isoformat().replace("+00:00", "Z"),
            "limit": 10000,
            "feed": feed,
            "adjustment": "raw",
            "sort": "asc",
        }
        while True:
            response = requests.get("https://data.alpaca.markets/v2/stocks/bars",
                                    headers=headers, params=params, timeout=20)
            if response.status_code != 200:
                raise RuntimeError(f"Alpaca bars request failed ({response.status_code}): "
                                   f"{response.text[:300]}")
            payload = response.json()
            for ticker, bars in (payload.get("bars") or {}).items():
                for bar in bars:
                    stamp = dt.datetime.fromisoformat(bar["t"].replace("Z", "+00:00")).astimezone(ET)
                    if (stamp.date() == start_et.date() and
                            dt.time(4, 0) <= stamp.time() < dt.time(9, 30)):
                        volumes[ticker] = volumes.get(ticker, 0.0) + max(0.0, float(bar.get("v") or 0))
            page_token = payload.get("next_page_token")
            if not page_token:
                break
            params["page_token"] = page_token
    return volumes


def _yahoo_scanner_rows():
    """Fetch Yahoo's free gainers screener and apply the scanner's local filters."""
    query = yf.EquityQuery("and", [
        yf.EquityQuery("eq", ["region", "us"]),
        yf.EquityQuery("gt", ["percentchange", 40]),
    ])

    rows = []
    offset = 0
    page_size = 250
    while offset < 5000:
        result = yf.screen(query, offset=offset, size=page_size,
                           sortField="percentchange", sortAsc=False)
        quotes = result.get("quotes") or []
        for quote in quotes:
            state = str(quote.get("marketState") or "").upper()
            et_now = dt.datetime.now(ET)
            premarket = (state in ("PRE", "PREPRE") or
                         dt.time(4, 0) <= et_now.time() < dt.time(9, 30))
            postmarket = (not premarket and
                          (state in ("POST", "POSTPOST") or
                           dt.time(16, 0) <= et_now.time() < dt.time(20, 0)))
            if premarket:
                # The screener's percentchange/dayvolume fields describe the
                # regular session and can belong to yesterday. Require a
                # timestamped quote and volume from today's premarket instead.
                price = quote.get("preMarketPrice")
                volume = quote.get("preMarketVolume")
                quote_time = quote.get("preMarketTime")
                if not price or not volume or not quote_time:
                    continue
                try:
                    quote_date = dt.datetime.fromtimestamp(float(quote_time), ET).date()
                except (TypeError, ValueError, OSError, OverflowError):
                    continue
                if quote_date != et_now.date():
                    continue
            elif postmarket:
                price = quote.get("postMarketPrice") or quote.get("regularMarketPrice") or quote.get("intradayprice")
                volume = quote.get("postMarketVolume") or quote.get("regularMarketVolume") or quote.get("dayvolume")
            else:
                price = quote.get("regularMarketPrice") or quote.get("intradayprice")
                volume = quote.get("regularMarketVolume") or quote.get("dayvolume")
            previous_close = quote.get("regularMarketPreviousClose") or quote.get("previousClose")
            try:
                price = float(price)
                previous_close = float(previous_close)
                volume = float(volume)
            except (TypeError, ValueError):
                continue
            if price <= 0 or previous_close <= 0:
                continue

            gap_pct = (price / previous_close - 1) * 100
            dollar_volume = price * volume
            ticker = str(quote.get("symbol") or "").upper()
            if ticker and 0.40 < price < 20 and gap_pct > 40 and dollar_volume >= 500_000:
                rows.append({
                    "ticker": ticker,
                    "price": round(price, 4),
                    "previous_close": round(previous_close, 4),
                    "gap_pct": round(gap_pct, 2),
                    "dollar_volume": round(dollar_volume),
                    "volume": round(volume),
                })

        offset += len(quotes)
        total = result.get("total")
        if not quotes or len(quotes) < page_size or (total is not None and offset >= int(total)):
            break

    rows.sort(key=lambda row: (-row["gap_pct"], -row["dollar_volume"], row["ticker"]))
    return rows


def _yahoo_all_premarket_rows(session_date):
    """Check today's premarket bars for every symbol in the supplied TC2000 universe."""
    scan_started = time.monotonic()
    symbols = _load_tc2000_scanner_symbols()
    total = len(symbols)
    with _scanner_cache_lock:
        _scanner_scan_state.update(done=0, total=1, symbols_done=0,
                                   symbols_total=total, universe_total=total,
                                   phase="history", error="")
    _scanner_log(f"TC2000 US Common Stocks scan started for {session_date}; "
                 f"universe contains {total} unique symbols; testing all symbols via Yahoo 1m history")

    with _scanner_cache_lock:
        _scanner_scan_state["done"] = 0

    today = dt.date.fromisoformat(session_date)
    candidates = {}
    counts = {"history_with_bars": 0, "history_without_today": 0,
              "history_without_previous_close": 0, "history_errors": 0,
              "history_not_gap": 0, "history_error_types": {}, "alpaca_errors": 0}

    def scan_symbol(ticker):
        try:
            yahoo_ticker = ticker.replace(".", "-")
            bars = yf.Ticker(yahoo_ticker).history(period="2d", interval="1m", prepost=True,
                                             auto_adjust=False, timeout=10)
            if bars is None or bars.empty or "Close" not in bars or bars.index.tz is None:
                return ticker, None, "history_without_today"
            eastern_index = bars.index.tz_convert(ET)
            current_mask = eastern_index.date == today
            current = bars.loc[current_mask]
            if current.empty:
                return ticker, None, "history_without_today"
            times = current.index.tz_convert(ET).time
            current = current.loc[[dt.time(4, 0) <= t < dt.time(9, 30) for t in times]]
            if current.empty:
                return ticker, None, "history_without_today"
            previous_mask = [(d < today and dt.time(9, 30) <= t < dt.time(16, 0))
                             for d, t in zip(eastern_index.date, eastern_index.time)]
            previous = bars.loc[previous_mask]
            previous_close = float(previous["Close"].iloc[-1]) if not previous.empty else None
            if previous_close is None or previous_close <= 0:
                return ticker, None, "history_without_previous_close"
            price = float(current["Close"].iloc[-1])
            if price <= 0:
                return ticker, None, "history_without_today"
            gap_pct = (price / previous_close - 1) * 100
            if 0.40 < price < 20 and gap_pct > 40:
                return ticker, {"ticker": ticker, "price": round(price, 4),
                                "previous_close": round(previous_close, 4),
                                "gap_pct": round(gap_pct, 2)}, "candidate"
            return ticker, None, "history_not_gap"
        except Exception as exc:
            return ticker, None, ("history_error", type(exc).__name__)

    batch_size = 240
    workers = 6
    for start in range(0, total, batch_size):
        batch = symbols[start:start + batch_size]
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="scanner-yahoo-history") as pool:
            futures = [pool.submit(scan_symbol, ticker) for ticker in batch]
            for future in as_completed(futures):
                ticker, row, outcome = future.result()
                if row:
                    candidates[ticker] = row
                elif isinstance(outcome, tuple) and outcome[0] == "history_error":
                    counts["history_errors"] += 1
                    error_type = outcome[1]
                    counts["history_error_types"][error_type] = counts["history_error_types"].get(error_type, 0) + 1
                elif outcome == "history_without_today":
                    counts["history_without_today"] += 1
                elif outcome == "history_without_previous_close":
                    counts["history_without_previous_close"] += 1
                else:
                    counts["history_not_gap"] += 1
                counts["history_with_bars"] += outcome in ("history_not_gap", "candidate")
                with _scanner_cache_lock:
                    _scanner_scan_state["symbols_done"] += 1
                    done = _scanner_scan_state["symbols_done"]
                if done % 250 == 0 or done == total:
                    _scanner_log(f"Yahoo history tested {done}/{total} TC2000 tickers; "
                                 f"Yahoo gap candidates={len(candidates)}; "
                                 f"request errors={counts['history_errors']}")

    _scanner_log(f"Yahoo gap scan finished; universe={total}, "
                 f"histories tested={_scanner_scan_state['symbols_done']}, "
                 f"histories with today's PM bars={counts['history_with_bars']}, "
                 f"no current bars={counts['history_without_today']}, "
                 f"no previous close={counts['history_without_previous_close']}, "
                 f"request errors={counts['history_errors']} {counts['history_error_types']}, "
                 f"nonmatching tickers={counts['history_not_gap']}, "
                 f"gap candidates={len(candidates)}")

    with _scanner_cache_lock:
        _scanner_scan_state.update(phase="alpaca", candidates=len(candidates))
    _scanner_log(f"Checking Alpaca {os.getenv('ALPACA_FEED', 'sip')} premarket volume "
                 f"for {len(candidates)} Yahoo gap candidates only")
    rows = []
    if candidates:
        try:
            volumes = _alpaca_premarket_volumes(list(candidates), session_date)
            for ticker, candidate in candidates.items():
                volume = float(volumes.get(ticker, 0.0))
                dollar_volume = candidate["price"] * volume
                if volume > 0 and dollar_volume >= 500_000:
                    rows.append({**candidate, "volume": round(volume),
                                 "dollar_volume": round(dollar_volume)})
                else:
                    _scanner_log(f"Filtered {ticker}: Alpaca premarket volume={volume:,.0f}; "
                                 f"dollar volume=${dollar_volume:,.0f}")
        except Exception as exc:
            counts["alpaca_errors"] = len(candidates)
            _scanner_log(f"Alpaca premarket volume check failed for {len(candidates)} candidates: "
                         f"{type(exc).__name__}: {exc}")
    rows.sort(key=lambda row: (-row["gap_pct"], -row["dollar_volume"], row["ticker"]))
    _scanner_log(f"TC2000 hybrid scan finished in {time.monotonic() - scan_started:.1f}s; "
                 f"universe={total}, Yahoo histories checked={_scanner_scan_state['symbols_done']}, "
                 f"Yahoo candidates={len(candidates)}, Alpaca volume errors={counts['alpaca_errors']}, "
                 f"qualified matches={len(rows)}")
    with _scanner_cache_lock:
        issues = []
        if counts["history_errors"]:
            issues.append(f"Yahoo history failed for {counts['history_errors']}/{total} symbols")
        if counts["alpaca_errors"]:
            issues.append(f"Alpaca volume failed for {counts['alpaca_errors']}/{len(candidates)} candidates")
        _scanner_scan_state["error"] = "; ".join(issues)
        _scanner_scan_state["done"] = 1
    for row in rows:
        _scanner_log(f"MATCH {row['ticker']} gap=+{row['gap_pct']:.1f}% "
                     f"price=${row['price']:.2f} volume={row['volume']:,} "
                     f"dollar_volume=${row['dollar_volume']:,}")
    return rows


def _refresh_yahoo_premarket_universe(session_date):
    """Scan Yahoo's US screener pages and publish the current premarket gap list."""
    try:
        rows = _yahoo_all_premarket_rows(session_date)
        with _scanner_cache_lock:
            if (_scanner_cache.get("session_date") == session_date and
                    _scanner_cache.get("source") == "premarket"):
                _scanner_cache.update(at=time.time(), rows=rows)
    except Exception as exc:
        with _scanner_cache_lock:
            _scanner_scan_state["error"] = str(exc)
        _scanner_log(f"Yahoo premarket scan failed for {session_date}: {type(exc).__name__}: {exc}")
    finally:
        with _scanner_cache_lock:
            if _scanner_scan_state.get("session_date") == session_date:
                _scanner_scan_state["running"] = False
                _scanner_scan_state["completed_at"] = time.time()
                _scanner_log(f"Yahoo premarket scan finished for {session_date}; "
                             f"tickers checked={_scanner_scan_state['symbols_done']}/"
                             f"{_scanner_scan_state['symbols_total']}, "
                             f"matches={len(_scanner_cache['rows'])}, "
                             f"error={_scanner_scan_state['error'] or 'none'}")


_scanner_details = {}


_scanner_details_lock = threading.RLock()


_scanner_details_inflight = set()


_scanner_details_pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="scanner-details")


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


def _fetch_yahoo_news(ticker):
    items = []
    errors = []
    press_wire_pattern = re.compile(
        r"globe.?newswire|pr\s*newswire|business\s*wire|newsfile|access\s*newswire|"
        r"the.?newswire|newsdirect|eqs.?news|cnw.?group", re.I
    )
    # Yahoo's ticker search returns issuer wire releases; appending "press
    # release" can produce an empty result even when the ticker search has PRs.
    for query in (ticker, f"{ticker} press release"):
        try:
            search = yf.Search(query=query, news_count=20,
                               enable_fuzzy_query=False, timeout=8)
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
        except Exception as exc:
            errors.append(f"Yahoo '{query}': {type(exc).__name__}: {exc}")
        if len(items) >= 2:
            break

    # Search may omit the ticker's own Yahoo Finance news feed, so also inspect
    # Yahoo's ticker news endpoint before deciding that no releases are present.
    if len(items) < 2:
        try:
            for article in (yf.Ticker(ticker.replace(".", "-")).news or []):
                content = article.get("content") if isinstance(article.get("content"), dict) else article
                provider = content.get("provider") or article.get("publisher") or ""
                if isinstance(provider, dict):
                    provider = provider.get("displayName") or provider.get("name") or ""
                if not press_wire_pattern.search(str(provider)):
                    continue
                item = _normalize_yahoo_news(article, "Press release")
                if item:
                    items.append(item)
        except Exception as exc:
            errors.append(f"Yahoo ticker news: {type(exc).__name__}: {exc}")

    unique, seen = [], set()
    for item in items:
        # Yahoo search and its ticker feed can return the same wire release;
        # deduplicate on normalized headline, falling back to the URL.
        key = re.sub(r"\W+", " ", item["title"].lower()).strip()
        if not key:
            key = item["url"]
        if key not in seen:
            unique.append(item)
            seen.add(key)
    def published_at(item):
        try:
            stamp = pd.Timestamp(item.get("published"))
            if stamp.tzinfo is None:
                stamp = stamp.tz_localize("UTC")
            return stamp.timestamp()
        except Exception:
            return 0
    unique.sort(key=published_at, reverse=True)
    now_ts = time.time()
    # Show the latest dated releases regardless of age; ignore undated and
    # future-dated items so every displayed release can carry a useful date tag.
    unique = [item for item in unique if 0 < published_at(item) <= now_ts]
    if not unique:
        app.logger.warning("No press releases found for %s. News fetch diagnostics: %s",
                           ticker, " | ".join(errors) if errors else "sources returned no matching press releases")
    return unique[:2]


def _get_scanner_news(ticker):
    now = time.time()
    with _scanner_details_lock:
        cached = _scanner_details.get(ticker) or {}
        # Keep the newest two releases current; a five-minute cache made new
        # releases invisible to the scanner for too long.
        news_ttl = 60
        if now - cached.get("news_at", 0) < news_ttl:
            return list(cached.get("news") or [])
    news = _fetch_yahoo_news(ticker)
    with _scanner_details_lock:
        cached = _scanner_details.setdefault(ticker, {})
        cached.update(news=news, news_at=now,
                      news_status=("Latest press releases found on Yahoo Finance" if news else
                                   "No press releases found on Yahoo Finance"))
    return news


def _scanner_approaches(ticker, previous_close):
    """Return first-pullback approach alerts before price crosses VWAP/21 EMA."""
    bars = yf.Ticker(ticker.replace(".", "-")).history(
        period="1d", interval="1m", prepost=True, auto_adjust=False
    )
    if bars is None or bars.empty:
        return []
    if bars.index.tz is None:
        bars.index = bars.index.tz_localize(ET)
    else:
        bars.index = bars.index.tz_convert(ET)
    bars = bars.rename(columns={"Open": "o", "High": "h", "Low": "l", "Close": "c", "Volume": "v"})
    bars = bars[["o", "h", "l", "c", "v"]].dropna(subset=["c"])
    latest_day = bars.index[-1].date()
    minute_of_day = bars.index.hour * 60 + bars.index.minute
    bars = bars[(bars.index.date == latest_day) & (minute_of_day >= 240) & (minute_of_day < 1200)]
    if len(bars) < 3:
        return []
    price = float(bars.c.iloc[-1])
    high_since_open = float(bars.h.max())
    try:
        previous_close = float(previous_close)
    except (TypeError, ValueError):
        previous_close = float(bars.o.iloc[0])
    close_values = bars.c.to_numpy()
    rally_pullback = (previous_close > 0 and high_since_open >= previous_close * 1.02
                      and price < high_since_open * 0.999
                      and close_values[-1] <= close_values[-2])
    if not rally_pullback:
        return []

    previous_price = float(close_values[-2])
    levels = []
    typical = (bars.h + bars.l + bars.c) / 3
    total_volume = bars.v.clip(lower=0).cumsum()
    weighted_volume = (typical * bars.v.clip(lower=0)).cumsum()
    if total_volume.iloc[-1] > 0:
        vwap_series = weighted_volume / total_volume.replace(0, np.nan)
        levels.append(("VWAP", "approaching VWAP from above",
                       float(vwap_series.iloc[-1]), float(vwap_series.iloc[-2])))

    alerts = []
    for code, label, level, previous_level in levels:
        if level > 0 and previous_level > 0 and price > level and previous_price > previous_level:
            distance_pct = (price - level) / price * 100
            previous_distance_pct = (previous_price - previous_level) / previous_price * 100
            # Alert only on a genuine pullback that is getting closer while still
            # holding above support; the browser applies each user's threshold.
            if 0 <= distance_pct <= 10.0 and previous_distance_pct > distance_pct:
                alerts.append({"code": code, "label": label, "level": round(level, 4),
                               "distance_pct": round(distance_pct, 2),
                               "previous_distance_pct": round(previous_distance_pct, 2)})
    return alerts


def _refresh_scanner_details(ticker, previous_close):
    try:
        now = time.time()
        with _scanner_details_lock:
            cached = _scanner_details.setdefault(ticker, {})
            technical_at = cached.get("technical_at", 0)
            news_at = cached.get("news_at", 0)
            news_ttl = 60
        if now - technical_at >= 20:
            try:
                approaches = _scanner_approaches(ticker, previous_close)
            except Exception:
                approaches = []
            with _scanner_details_lock:
                _scanner_details.setdefault(ticker, {}).update(approaches=approaches, technical_at=now)
        if now - news_at >= news_ttl:
            try:
                _get_scanner_news(ticker)
            except Exception:
                pass
    finally:
        with _scanner_details_lock:
            _scanner_details_inflight.discard(ticker)


def _queue_scanner_details(row):
    ticker = row["ticker"]
    now = time.time()
    with _scanner_details_lock:
        cached = _scanner_details.setdefault(ticker, {})
        due = (now - cached.get("technical_at", 0) >= 20
               or now - cached.get("news_at", 0) >= 60)
        if not due or ticker in _scanner_details_inflight:
            return
        _scanner_details_inflight.add(ticker)
    _scanner_details_pool.submit(_refresh_scanner_details, ticker, row.get("previous_close"))


@app.route("/api/scanner")
def scanner_market_scan():
    return jsonify({"error": "Scanner has been removed."}), 404
    user = (request.args.get("user") or "").strip().lower()
    token = (request.args.get("token") or "").strip()
    if not user or not token or not _db_validate_session(user, token):
        return jsonify({"error": "Session expired. Please sign in again."}), 401
    if not _get_app_ui_visibility(user).get("scanner", True):
        return jsonify({"error": "Scanner access is disabled for this account."}), 403

    now = time.time()
    session_date = _scanner_session_date()
    premarket_scan = _scanner_is_premarket_window()
    with _scanner_cache_lock:
        if _scanner_cache.get("session_date") != session_date:
            _scanner_cache.update(at=0.0, rows=[], session_date=session_date, source="")
        if premarket_scan:
            scan_due = (_scanner_scan_state.get("session_date") != session_date or
                        (not _scanner_scan_state.get("running") and
                         now - _scanner_scan_state.get("completed_at", 0) >= _SCANNER_RESCAN_SECONDS))
            _scanner_cache["source"] = "premarket"
            if scan_due:
                _scanner_scan_state.update(running=True, session_date=session_date,
                                           done=0, total=0, symbols_done=0, symbols_total=0,
                                           universe_total=0, phase="history", candidates=0, error="")
                _scanner_scan_pool.submit(_refresh_yahoo_premarket_universe, session_date)
        elif (_scanner_cache.get("source") != "regular" or
              now - _scanner_cache["at"] >= 25):
            try:
                rows = _yahoo_scanner_rows()
                _scanner_cache.update(at=now, rows=rows, session_date=session_date, source="regular")
            except Exception as exc:
                return jsonify({"error": str(exc)}), 502
        output = []
        for row in _scanner_cache["rows"]:
            _queue_scanner_details(row)
            with _scanner_details_lock:
                details = dict(_scanner_details.get(row["ticker"]) or {})
            output.append({**row,
                           "news": details.get("news") or [],
                           "news_status": details.get("news_status"),
                           "approaches": details.get("approaches") or []})
        return jsonify({"tickers": output, "updated": _scanner_cache["at"],
                        "session_date": session_date,
                        "scanning": premarket_scan and _scanner_scan_state.get("running", False),
                        "scan_done": _scanner_scan_state.get("done", 0),
                        "scan_total": _scanner_scan_state.get("total", 0),
                        "scan_symbols_done": _scanner_scan_state.get("symbols_done", 0),
                        "scan_symbols_total": _scanner_scan_state.get("symbols_total", 0),
                        "scan_universe_total": _scanner_scan_state.get("universe_total", 0),
                        "scan_phase": _scanner_scan_state.get("phase", ""),
                        "scan_candidates": _scanner_scan_state.get("candidates", 0),
                        "scan_error": _scanner_scan_state.get("error", "")})
