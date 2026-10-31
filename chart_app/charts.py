"""Charts feature module."""
from chart_app.application import (
    PM_START,
    RG_END,
    RG_START,
    _db_validate_session,
    app,
    bars2,
    cget,
    chart_pack,
    clean,
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
import threading
import uuid
import json
from pathlib import Path

GAP_DEEPDIVE_TICKERS = list(dict.fromkeys("VIOT PPBT FAMI BIAF GELS RDAC GYGY WETO CLGN RDHL WHLR QNRX LGPS VNRX PPCB CELU BRNX SXTC VRAX SKK BIVI TNON CDTG ZSTK YJ EJH SGLY ONFO CIGL YMT DKI HKIT HUIZ GAUZ CELZ CLRO YXT ZYBT HYFM FCUV FFAI EDBL CJMB AKAN INLF GVH DXST EHGO ZBAO YMAT GMM YRD DXF CDT APWC IHT ICCM CIIT BEEM CRE CAST AIDX BYAH DSY BQ CHNR CCTG CGTL AZI CHAI BATL DAIC ANYBAOS BOXL BYSI CHOW DOGZ FRGT FXHO HCTI INM ITOC IVF JZXN KUST KXIN LGHL LICN LNAI LRHC LXEH MBRX MIMI MLEC MTEN NAII NCRA NEXR NIXX NUWE NVVE NXTS ODYS OFAL OMH OPTX ORIS PAVS PCLA PCSA PN PWCM RGNT RLYB RMAX RMSG RUBI RYOJ SAFX SBFM SDOT SFHG SILO SKYQ SLGB SLND SOAR SOBR SSM STG TDIC TRNR TURB TWG UK UPC VEEE VIVK VIVS".split()))
GAP_DEEPDIVE_JOBS = {}
GAP_DEEPDIVE_LOCK = threading.Lock()
_GAP_DEEPDIVE_MANIFESTS = {}


def _saved_gap_deepdive_manifest(year):
    if year in _GAP_DEEPDIVE_MANIFESTS:
        return _GAP_DEEPDIVE_MANIFESTS[year]
    path = Path(__file__).resolve().parent.parent / 'data' / f'gap_deepdive_{year}.json'
    try:
        manifest = json.loads(path.read_text(encoding='utf-8'))
        saved = manifest if manifest.get('status') == 'complete' and manifest.get('results') is not None else None
    except (OSError, ValueError, TypeError):
        saved = None
    _GAP_DEEPDIVE_MANIFESTS[year] = saved
    return saved


def _extract_explicit_financing_party(excerpt, role_phrase):
    """Fail closed: return a firm only when the filing explicitly links it to this role."""
    role = re.escape(role_phrase)
    suffix = r'(?:L\.L\.C\.?|LLC|Incorporated|Inc\.?|Corporation|Corp\.?|Limited|Ltd\.?|L\.P\.?|LP|LLP|PLC|N\.A\.?|Bank|Company|Co\.?|S\.A\.?)'
    entity = rf"[A-Z0-9][A-Za-z0-9&.'’()\-/]*(?:\s+[A-Za-z0-9&.'’()\-/]+){{0,5}}\s+{suffix}"
    role_match = re.search(rf'\b{role}\b', excerpt or '', re.I)
    if not role_match:
        return None
    left, right = excerpt[:role_match.start()], excerpt[role_match.end():]
    connectors = r"\s*[,(:\-]*\s*(?:(?:is|was|were|will be|has been|have been|acts? as|acting as|acted as|serves? as|served as|appointed as|engaged as|retained as|selected as|designated as|will act as|to be|as)\s*)*(?:(?:the|its|our|sole|exclusive|lead)\s+)*$"
    found = set()
    # Firm followed by an explicit role label, e.g. "Northstar LLC is the placement agent".
    for end_match in re.finditer(suffix, left, re.I):
        bridge = left[end_match.end():]
        if not re.fullmatch(connectors, bridge, re.I):
            continue
        starts = [0] + [m.end() for m in re.finditer(r'\b(?:retained|engaged|appointed|selected|designated|named|identified|is|was|were|has been|have been|will be|acting|acts|acted|serves|served|as)\b', left[:end_match.start()], re.I)]
        for start in starts:
            name = left[start:end_match.end()].strip(" ,:;\"'()")
            name = re.sub(r'^(?:the company|we|our company)\s+', '', name, flags=re.I).strip()
            if re.fullmatch(entity, name):
                found.add(name)
    # Role followed by a named firm, e.g. "placement agent: Northstar LLC".
    after = re.match(rf"\s*(?:[,(:\-]\s*)?(?:(?:is|was|will be|shall be|:|of)\s+)?(?:the\s+)?(?P<name>{entity})\b", right, re.I)
    if after:
        found.add(after.group('name').strip())
    return next(iter(found)) if len(found) == 1 else None


def _sec_verified_gap_player(ticker, event_day):
    """Find a named placement agent/underwriter in issuer SEC filings; ambiguity yields no tag."""
    from chart_app.keyword_compare import _keyword_match_excerpt, _keyword_sec_get, _keyword_ticker_map
    cache_key = f"gap_sec_player_{ticker}_{event_day}"
    cached = cget(cache_key, 30 * 86400)
    if cached is not None:
        return cached
    result = {'verified': False}
    try:
        ticker_map = _keyword_ticker_map()
        cik, company = next(((cik, info) for cik, info in ticker_map.items()
                             if str(info.get('ticker') or '').upper() == ticker), (None, None))
        if not cik or not company:
            cput(cache_key, result)
            return result
        day = dt.date.fromisoformat(event_day)
        start = (day - dt.timedelta(days=180)).isoformat()
        end = min(day + dt.timedelta(days=3), dt.date.today()).isoformat()
        forms = '8-K,6-K,S-1,S-3,F-1,F-3,424B1,424B2,424B3,424B4,424B5,424B6,424B7'
        disclosures = {}
        for phrase, role_label in (('placement agent', 'Placement agent'), ('underwriter', 'Underwriter')):
            response = _keyword_sec_get('https://efts.sec.gov/LATEST/search-index',
                params={'q': f'"{company["name"]}" AND "{phrase}"', 'dateRange': 'custom',
                        'startdt': start, 'enddt': end, 'forms': forms, 'from': 0, 'size': 100}, timeout=25)
            response.raise_for_status()
            hits = ((response.json().get('hits') or {}).get('hits') or [])
            for hit in hits:
                source = hit.get('_source') or {}
                raw_ciks = source.get('ciks') or source.get('cik') or []
                raw_ciks = raw_ciks if isinstance(raw_ciks, list) else [raw_ciks]
                normalized_ciks=set()
                for value in raw_ciks:
                    try: normalized_ciks.add(str(int(str(value).lstrip('0') or '0')).zfill(10))
                    except (TypeError,ValueError): continue
                if cik not in normalized_ciks:
                    continue
                # Search highlights can omit the firm's name or the words linking
                # it to the role. Read the filing context so the tag rests on an
                # explicit relationship in the document itself.
                document_hit = dict(hit)
                document_hit['highlight'] = {}
                document_hit['_source'] = {**source, 'highlight': {}}
                excerpt = _keyword_match_excerpt(document_hit, phrase, fetch_document=True)
                firm = _extract_explicit_financing_party(excerpt, phrase)
                if not firm:
                    continue
                accession_match = re.search(r'\d{10}-\d{2}-\d{6}', str(hit.get('_id') or ''))
                accession = accession_match.group(0) if accession_match else ''
                if not accession:
                    continue
                filed = str(source.get('file_date') or source.get('display_date_filed') or '')
                filing_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/{accession}-index.html"
                disclosures[(role_label, firm)] = {'verified': True, 'role': role_label, 'name': firm,
                    'filing_date': filed, 'filing_url': filing_url, 'evidence': excerpt[:420]}
        result = next(iter(disclosures.values())) if len(disclosures) == 1 else result
    except Exception:
        app.logger.exception('SEC placement/underwriter verification failed for %s %s', ticker, event_day)
    cput(cache_key, result)
    return result


def _run_gap_deepdive(job_id):
    from chart_app.market_data import PM_START
    from chart_app.application import clean, cput
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import numpy as np
    job = GAP_DEEPDIVE_JOBS[job_id]
    year = dt.date.today().year
    # Only include completed sessions so each saved chart is a full trading day.
    year_start, year_end = f"{year}-01-01", (dt.date.today()-dt.timedelta(days=1)).isoformat()
    found, errors = [], []
    def scan_ticker(ticker):
        daily = load_daily(ticker)
        candidates = [(i, row) for i, row in enumerate(daily) if i and year_start <= row['date'] <= year_end
                      and row.get('h', 0) >= daily[i-1].get('c', 1) * 1.5]
        days = []
        for ix, row in candidates:
            day = row['date']
            try:
                frame = minute_df(ticker, day)
                if frame is None or frame.empty: continue
                mm = frame.index.hour * 60 + frame.index.minute
                pm = frame[(mm >= PM_START) & (mm < 570)]
                if pm.empty: continue
                # Daily range is only a prefilter; validate the PM high and $ volume on minute bars.
                prev = float(daily[ix-1]['c'])
                pm_high = float(pm.h.max())
                move = pm_high / prev - 1 if prev > 0 else 0
                tp = (pm.h + pm.l + pm.c) / 3
                dollar_volume = float((tp * pm.v).sum())
                if move >= .70 and dollar_volume >= 10_000_000:
                    # minute_df has persisted the raw 1-minute candles to disk. Keep only
                    # metadata in the scan response; cards request their chart on visibility.
                    days.append({'ticker': ticker, 'date': day, 'previous_close': prev, 'move': move, 'pm_dollar_volume': dollar_volume,
                                 'title': f"{ticker} · {day} · PM high +{move:.0%} · ${dollar_volume/1e6:.1f}M"})
            except Exception:
                continue
        return days
    try:
        job.update(status='running', message=f'Scanning {len(GAP_DEEPDIVE_TICKERS)} tickers for {year}…', done=0, total=len(GAP_DEEPDIVE_TICKERS), min_pm_gap_pct=70)
        cput(f"gap_deepdive_job_{job_id}", job)
        with ThreadPoolExecutor(max_workers=min(12, max(1, len(GAP_DEEPDIVE_TICKERS)))) as pool:
            futures = {pool.submit(scan_ticker, ticker): ticker for ticker in GAP_DEEPDIVE_TICKERS}
            for future in as_completed(futures):
                ticker = futures[future]
                try: found.extend(future.result())
                except Exception as exc: errors.append(f"{ticker}: {exc}")
                job['done'] += 1
                job['message'] = f"Scanned {job['done']} / {job['total']} tickers · {len(found)} qualifying sessions"
                cput(f"gap_deepdive_job_{job_id}", {**job, 'results': []})
        found.sort(key=lambda x: (x['date'], x['ticker']), reverse=True)
        job.update(status='complete', message=f"Complete · {len(found)} qualifying sessions", results=clean(found), errors=errors[:20])
        cput(f"gap_deepdive_job_{job_id}", job)
        cput(f"gap_deepdive_latest_{year}", {'job':job_id,'scanned_on':dt.date.today().isoformat(),'min_pm_gap_pct':70,**job})
    except Exception as exc:
        job.update(status='error', message=f"{type(exc).__name__}: {exc}")
        cput(f"gap_deepdive_job_{job_id}", job)


@app.route('/api/gap-deepdive/start', methods=['POST'])
def gap_deepdive_start():
    try:
        user=(request.args.get('user') or '').strip().lower(); token=(request.args.get('token') or '').strip()
        if user not in _auth.ALLOWED_USERS or not _db_validate_session(user, token): return jsonify({'error':'Unauthorized'}),401
        from chart_app.application import cget, cput
        year=dt.date.today().year
        latest=cget(f"gap_deepdive_latest_{year}")
        if latest and latest.get('status')=='complete' and float(latest.get('min_pm_gap_pct',0))>=70:
            return jsonify({'job':latest['job'],'cached':True})
        latest=_saved_gap_deepdive_manifest(year)
        if latest:
            cput(f"gap_deepdive_job_{latest['job']}", latest)
            cput(f"gap_deepdive_latest_{year}", latest)
            return jsonify({'job':latest['job'],'cached':True})
        job_id=uuid.uuid4().hex
        GAP_DEEPDIVE_JOBS[job_id]={'status':'queued','message':'Queued historical scan…','done':0,'total':len(GAP_DEEPDIVE_TICKERS),'results':[]}
        cput(f"gap_deepdive_job_{job_id}", GAP_DEEPDIVE_JOBS[job_id])
        threading.Thread(target=_run_gap_deepdive,args=(job_id,),daemon=True).start()
        return jsonify({'job':job_id})
    except Exception as exc:
        app.logger.exception('Could not start Gap Up Deepdive scan')
        return jsonify({'error':f'Could not start scan: {type(exc).__name__}: {exc}'}),500


@app.route('/api/gap-deepdive/<job_id>')
def gap_deepdive_status(job_id):
    try:
        user=(request.args.get('user') or '').strip().lower(); token=(request.args.get('token') or '').strip()
        if user not in _auth.ALLOWED_USERS or not _db_validate_session(user, token): return jsonify({'error':'Unauthorized'}),401
        from chart_app.application import cget
        job=cget(f"gap_deepdive_job_{job_id}") or GAP_DEEPDIVE_JOBS.get(job_id)
        if not job:
            for year in (dt.date.today().year,):
                manifest=_saved_gap_deepdive_manifest(year)
                if manifest and manifest.get('job')==job_id:
                    job=manifest
                    break
        if not job: return jsonify({'error':'Scan not found; please start a new scan.'}),404
        return jsonify(clean(job))
    except Exception as exc:
        app.logger.exception('Could not read Gap Up Deepdive scan status')
        return jsonify({'error':f'Could not read scan status: {type(exc).__name__}: {exc}'}),500


@app.route('/api/gap-deepdive/<job_id>/chart')
def gap_deepdive_chart(job_id):
    user=(request.args.get('user') or '').strip().lower(); token=(request.args.get('token') or '').strip()
    ticker=(request.args.get('ticker') or '').strip().upper(); day=(request.args.get('date') or '').strip()
    if user not in _auth.ALLOWED_USERS or not _db_validate_session(user, token): return jsonify({'error':'Unauthorized'}),401
    if not re.fullmatch(r'[A-Z.\-]{1,10}',ticker): return jsonify({'error':'Invalid ticker'}),400
    try: dt.date.fromisoformat(day)
    except ValueError: return jsonify({'error':'Invalid session date'}),400
    from chart_app.application import cget
    job=cget(f"gap_deepdive_job_{job_id}") or GAP_DEEPDIVE_JOBS.get(job_id)
    if not job:
        manifest=_saved_gap_deepdive_manifest(dt.date.today().year)
        if manifest and manifest.get('job')==job_id:
            job=manifest
    if not job or job.get('status')!='complete': return jsonify({'error':'Scan is not complete or has expired.'}),404
    scan_item=next((item for item in job.get('results',[]) if item.get('ticker')==ticker and item.get('date')==day),None)
    if scan_item is None:
        return jsonify({'error':'Session is not part of this scan.'}),404
    try:
        return jsonify({'chart':_gap_deepdive_chart_payload(ticker,day,scan_item)})
    except Exception as exc:
        return jsonify({'error':f'{type(exc).__name__}: {exc}'}),502


def _gap_deepdive_chart_payload(ticker,day,scan_item):
    # Raw one-minute candles are cached by minute_df, but packing candles and
    # derived chart fields on every open still costs time. Persist this ready-
    # to-render payload as well so repeat chart visits are disk-cache hits.
    from chart_app.application import cget, cput
    import hashlib
    payload_key = 'gap_chart_payload_' + hashlib.sha1(f'{ticker.upper()}|{day}'.encode()).hexdigest()
    cached = cget(payload_key)
    if cached is not None:
        return cached
    pc=scan_item.get('previous_close')
    if not pc:
        saved=_saved_gap_deepdive_manifest(dt.date.today().year)
        saved_item=next((item for item in (saved or {}).get('results',[]) if item.get('ticker')==ticker and item.get('date')==day),None)
        pc=(saved_item or {}).get('previous_close')
    if not pc:
        daily=load_daily(ticker)
        index=next((i for i,row in enumerate(daily) if row['date']==day),None)
        if index is None or index==0: raise ValueError('No previous close is available for this session.')
        pc=float(daily[index-1]['c'])
    frame=minute_df(ticker,day)
    if frame is None or frame.empty: raise ValueError('Cached intraday candles are unavailable.')
    mm=frame.index.hour*60+frame.index.minute
    pm=frame[(mm>=PM_START)&(mm<RG_START)]
    packed=chart_pack(frame,{'models':{},'pm_high':float(pm.h.max()) if not pm.empty else None},
                      float(pc),dt.date.fromisoformat(day),include_after_hours=True)
    packed['ticker']=ticker
    cput(payload_key, packed)
    return packed


@app.route('/api/gap-deepdive/<job_id>/charts',methods=['POST'])
def gap_deepdive_charts(job_id):
    user=(request.args.get('user') or '').strip().lower(); token=(request.args.get('token') or '').strip()
    if user not in _auth.ALLOWED_USERS or not _db_validate_session(user,token): return jsonify({'error':'Unauthorized'}),401
    from chart_app.application import cget
    job=cget(f"gap_deepdive_job_{job_id}") or GAP_DEEPDIVE_JOBS.get(job_id)
    if not job:
        manifest=_saved_gap_deepdive_manifest(dt.date.today().year)
        if manifest and manifest.get('job')==job_id: job=manifest
    if not job or job.get('status')!='complete': return jsonify({'error':'Scan is not complete or has expired.'}),404
    requested=(request.get_json(silent=True) or {}).get('charts') or []
    if not isinstance(requested,list) or not requested or len(requested)>64: return jsonify({'error':'Request between 1 and 64 charts.'}),400
    results={};errors={};valid=[]
    for entry in requested:
        if not isinstance(entry,dict): continue
        ticker=(entry.get('ticker') or '').strip().upper();day=(entry.get('date') or '').strip();key=f'{ticker}|{day}'
        try: dt.date.fromisoformat(day)
        except ValueError: errors[key]='Invalid session date';continue
        if not re.fullmatch(r'[A-Z.\-]{1,10}',ticker): errors[key]='Invalid ticker';continue
        scan_item=next((item for item in job.get('results',[]) if item.get('ticker')==ticker and item.get('date')==day),None)
        if scan_item is None: errors[key]='Session is not part of this scan';continue
        valid.append((key,ticker,day,scan_item))
    # Candle reads are disk cached; run independent chart packing tasks
    # concurrently to reduce time-to-first-render for a viewport batch.
    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=min(12, max(1,len(valid)))) as pool:
        futures={pool.submit(_gap_deepdive_chart_payload,ticker,day,item):key for key,ticker,day,item in valid}
        for future in as_completed(futures):
            key=futures[future]
            try: results[key]=future.result()
            except Exception as exc: errors[key]=f'{type(exc).__name__}: {exc}'
    return jsonify({'charts':results,'errors':errors})


@app.route('/api/gap-deepdive/<job_id>/float')
def gap_deepdive_float(job_id):
    user=(request.args.get('user') or '').strip().lower(); token=(request.args.get('token') or '').strip()
    ticker=(request.args.get('ticker') or '').strip().upper()
    if user not in _auth.ALLOWED_USERS or not _db_validate_session(user,token): return jsonify({'error':'Unauthorized'}),401
    if not re.fullmatch(r'[A-Z.\-]{1,10}',ticker): return jsonify({'error':'Invalid ticker'}),400
    from chart_app.application import cget,cput
    job=cget(f"gap_deepdive_job_{job_id}") or GAP_DEEPDIVE_JOBS.get(job_id)
    if not job:
        manifest=_saved_gap_deepdive_manifest(dt.date.today().year)
        if manifest and manifest.get('job')==job_id: job=manifest
    if not job or not any(item.get('ticker')==ticker for item in job.get('results',[])):
        return jsonify({'error':'Ticker is not part of this scan.'}),404
    key=f"gap_deepdive_float_{ticker}"
    cached=cget(key,30*86400)
    if cached is None:
        try:
            from chart_app.runner import poly_float
            shares,asof,percent,error=poly_float(ticker)
            cached={'float_shares':shares,'asof':asof,'float_percent':percent,'error':error}
        except Exception as exc:
            cached={'float_shares':None,'error':str(exc)}
        cput(key,cached)
    return jsonify(cached)


@app.route('/api/gap-deepdive/<job_id>/player')
def gap_deepdive_player(job_id):
    user=(request.args.get('user') or '').strip().lower(); token=(request.args.get('token') or '').strip()
    ticker=(request.args.get('ticker') or '').strip().upper(); day=(request.args.get('date') or '').strip()
    if user not in _auth.ALLOWED_USERS or not _db_validate_session(user, token): return jsonify({'error':'Unauthorized'}),401
    if not re.fullmatch(r'[A-Z.\-]{1,10}',ticker): return jsonify({'error':'Invalid ticker'}),400
    try: dt.date.fromisoformat(day)
    except ValueError: return jsonify({'error':'Invalid session date'}),400
    from chart_app.application import cget
    job=cget(f"gap_deepdive_job_{job_id}") or GAP_DEEPDIVE_JOBS.get(job_id)
    if not job:
        manifest=_saved_gap_deepdive_manifest(dt.date.today().year)
        if manifest and manifest.get('job')==job_id: job=manifest
    if not job or not any(item.get('ticker')==ticker and item.get('date')==day for item in job.get('results',[])):
        return jsonify({'error':'Session is not part of this saved scan.'}),404
    return jsonify(_sec_verified_gap_player(ticker,day))

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
    include_after_hours=request.args.get("postmarket") == "1"
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
            packed=bars2(dayframe, include_after_hours=include_after_hours)
            if packed:
                packed["d"]=[date]*len(packed["t"]); chunks.append(packed)
        if not chunks: return jsonify({"error":"No intraday bars are available for this range"}),404
        bars={k:sum((p[k] for p in chunks),[]) for k in ("t","d","o","h","l","c","v","vw")}
        return jsonify({"bars":bars,"dates":[p["d"][0] for p in chunks],"daily":False,"metrics":daily_metrics,"after_hours":include_after_hours})
    except Exception as exc: return jsonify({"error":f"{type(exc).__name__}: {exc}"}),502
