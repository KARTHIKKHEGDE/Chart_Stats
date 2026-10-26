"""Ticker Notes - enter a symbol, get float, R/S history, filings, gap-up history.
Free sources only: Yahoo Finance (yfinance) + SEC EDGAR (Polygon/Massive optional).
Run:  pip install flask yfinance pandas requests
      python app.py   ->  http://localhost:5000

Speed notes (this version):
  * one pooled HTTP session + thread-safe SEC rate limiter (8 req/s)
  * companyfacts / submissions JSON downloaded once and cached
  * filing text cached on disk forever (filings never change)
  * filings + exhibits downloaded in parallel, junk forms/docs skipped
  * yfinance calls run in parallel; SEC prefetch overlaps with Yahoo/Polygon
  * Polygon: ONE call per gap day (04:00-15:59) instead of two, run in parallel
  * whole result cached for 5 minutes (add &refresh=1 to bypass)
"""
import hashlib
import html as _html
import os
import re
import statistics as st
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf
import sys
from flask import Flask, g, jsonify, request

_this_dir = os.path.dirname(os.path.abspath(__file__))
if _this_dir not in sys.path:
    sys.path.insert(0, _this_dir)

import financing as fin
import gemini_notes as gn


def load_local_env():
    """Load simple KEY=VALUE entries from the project-local .env file."""
    for candidate in [Path(__file__).with_name(".env"), Path.cwd() / ".env"]:
        if candidate.exists():
            for raw_line in candidate.read_text(encoding="utf-8").splitlines():
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key, value = key.strip(), value.strip()
                if key and key not in os.environ:
                    os.environ[key] = value.strip("\"'")


load_local_env()

# SEC requires a descriptive User-Agent. Put your own email here.
SEC_HEADERS = {"User-Agent": "TickerNotes you@example.com"}
GAP_MIN = 0.30  # gap-up threshold for "Gap History"
FINANCING_FEE_RATE = 0.08
PREMARKET_MIN_TURNOVER = 2_000_000.0
DISPLAY_TIMEZONE = "Asia/Kolkata"
SEC_LOOKBACK_YEARS = 3          # exchange notices, shelves, XBRL facts
DILUTION_LOOKBACK_DAYS = 365    # dilution-frequency scan: last 12 months only

MASSIVE_KEY = os.environ.get("MASSIVE_API_KEY") or os.environ.get("POLYGON_API_KEY")
POLYGON_MIN_INTERVAL = float(os.environ.get("POLYGON_MIN_INTERVAL", "12.5"))  # set 0 on a paid plan
CACHE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cache")
TEXT_CACHE = Path(CACHE) / "sec_text"
TEXT_CACHE.mkdir(parents=True, exist_ok=True)
RESULT_TTL = 300  # seconds

app = Flask(__name__)
_cik_map = {}
_sec_index_cache = {}
_sec_text_cache = {}
_financing_cache = {}
_result_cache = {}


# --------------------------------------------------------------------------
# Terminal logging: timestamped, thread-safe progress lines
# --------------------------------------------------------------------------
_log_lock = threading.Lock()


def log(msg, tag="app"):
    with _log_lock:
        print(f"[{time.strftime('%H:%M:%S')}] [{tag:<7}] {msg}", flush=True)


def short(url):
    """Last two path parts of a URL, for compact log lines."""
    return "/".join(url.split("?")[0].rstrip("/").split("/")[-2:])


@contextmanager
def step(msg, tag="app"):
    """Log start, then done/FAILED with elapsed seconds."""
    log(f"{msg} ...", tag)
    t0 = time.time()
    try:
        yield
    except Exception as e:
        log(f"{msg} FAILED after {time.time() - t0:.1f}s: {e}", tag)
        raise
    log(f"{msg} done ({time.time() - t0:.1f}s)", tag)


# --------------------------------------------------------------------------
# Networking helpers: pooled session, rate limiter, cached JSON, cached text
# --------------------------------------------------------------------------
SESSION = requests.Session()
SESSION.headers.update(SEC_HEADERS)
SESSION.mount("https://", requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20))


class RateLimiter:
    """Thread-safe: at most `per_sec` calls per second across all threads."""

    def __init__(self, per_sec):
        self.interval = 1.0 / per_sec
        self.next = 0.0
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            now = time.monotonic()
            slot = max(now, self.next)
            self.next = slot + self.interval
        if slot > now:
            time.sleep(slot - now)


SEC_LIMIT = RateLimiter(8)  # SEC allows 10/s
_json_cache = {}  # url -> (timestamp, data)


def sec_get(url, timeout=25):
    SEC_LIMIT.wait()
    return SESSION.get(url, timeout=timeout)


def sec_json(url, ttl=3600):
    """GET + parse JSON from SEC with an in-memory TTL cache."""
    hit = _json_cache.get(url)
    if hit and time.time() - hit[0] < ttl:
        return hit[1]
    if not url.endswith("index.json"):
        log(f"GET {short(url)}", "sec")
    data = sec_get(url).json()
    _json_cache[url] = (time.time(), data)
    return data


def fetch_text(url):
    """Download a filing document, strip tags, cache on disk forever."""
    fp = TEXT_CACHE / (hashlib.md5(url.encode()).hexdigest() + ".txt")
    if fp.exists():
        return fp.read_text(encoding="utf-8")
    log(f"downloading {short(url)}", "sec")
    r = sec_get(url, 20)
    if r.status_code != 200:
        log(f"  HTTP {r.status_code} for {short(url)} (skipped)", "sec")
        return ""  # never cache failures
    t = re.sub(r"<[^>]+>", " ", r.text)
    t = re.sub(r"\s+", " ", _html.unescape(t))
    fp.write_text(t, encoding="utf-8")
    return t


def get_cik(sym):
    if not _cik_map:
        data = sec_json("https://www.sec.gov/files/company_tickers.json", ttl=86400)
        for v in data.values():
            _cik_map[v["ticker"].upper()] = str(v["cik_str"]).zfill(10)
    return _cik_map.get(sym.upper())


def pct(x):
    return f"{x*100:+.0f}%"


def ist_time(timestamp):
    """Format a timezone-aware market timestamp in India Standard Time."""
    return pd.Timestamp(timestamp).tz_convert(DISPLAY_TIMEZONE).strftime("%H:%M")


def risk_state():
    if not hasattr(g, "risk_state"):
        g.risk_state = {}
    return g.risk_state


DIL_KEYS = re.compile(r"(warrant|exercise price|conversion|convertible|registered direct|"
                      r"private placement|purchase price|at-the-market|offering)", re.I)
PLAYER_KEYS = re.compile(r"(underwriter|placement agent|selling agent|investment bank|"
                         r"financial advisor|dealer manager|bookrunner)", re.I)
TAGS = {  # catalyst label -> regex on headline
    "Partnerships": r"partner|collaborat|alliance|agreement|joint venture",
    "AI": r"\bAI\b|artificial intelligence|machine learning",
    "Contracts": r"contract|award|purchase order|order from",
    "Offering": r"offering|private placement|registered direct|warrants",
    "Earnings": r"earnings|quarter|revenue|results",
    "Regulatory": r"FDA|approval|clinical|trial|patent",
    "Crypto": r"bitcoin|crypto|blockchain|token",
}


def parse_dilution(text):
    """Heuristic: keep sentences that mention a dilution term AND a $ amount or share count."""
    sents = re.split(r"(?<=[.;]) ", text)
    hits = [s for s in sents if DIL_KEYS.search(s) and (
        re.search(r"\b[\d,]+(?:\.\d+)?\s+(shares?|warrants?)\b", s, re.I) or
        re.search(r"\$\s?[\d,.]+\s*(million|thousand)?", s, re.I))]
    return hits[:2]


def parse_players(text):
    """Find complete filing sentences naming the offering's financial intermediary."""
    sents = re.split(r"(?<=[.;]) ", text)
    candidates = [s for s in sents if PLAYER_KEYS.search(s) and re.search(
        r"\b(acted as|served as (?:the )?(?:exclusive )?(?:placement|underwriting|selling|sales) agent|"
        r"appointed|engaged|retained|"
        r"underwriting agreement|placement agency agreement|sales agents?|"
        r"through [A-Z][\w&.-]*(?:\s+(?:and|,)\s+[A-Z][\w&.-]*)?)\b", s, re.I)]
    candidates.sort(key=lambda s: not re.search(
        r"\b(acted as|served as (?:the )?(?:exclusive )?(?:placement|underwriting|selling|sales) agent|"
        r"appointed|engaged|retained|"
        r"underwriting agreement|placement agency agreement)\b", s, re.I))
    return candidates[:2]


def cash_burn_metrics(t, info):
    """Use reported cash-flow data to project current cash and runway."""
    reported_cash = info.get("totalCash")
    cash_date = None
    try:
        balance = t.balance_sheet
        for label in ("Cash Cash Equivalents And Short Term Investments",
                      "Cash And Cash Equivalents"):
            if label in balance.index:
                values = balance.loc[label].dropna()
                if len(values):
                    reported_cash = float(values.iloc[0])
                    cash_date = pd.Timestamp(values.index[0]).tz_localize(None)
                    break
    except (AttributeError, KeyError, TypeError, ValueError):
        pass

    flows = []
    flow_months = 0
    try:
        quarterly = t.quarterly_cashflow
        for label in ("Operating Cash Flow", "Total Cash From Operating Activities",
                      "Total Cash From Operating Activities Continuing Operations"):
            if label in quarterly.index:
                values = quarterly.loc[label].dropna().head(4)
                if len(values):
                    flows = [float(value) for value in values]
                    flow_months = len(flows) * 3
                    break
    except (AttributeError, KeyError, TypeError, ValueError):
        pass
    if not flows:
        try:
            annual = t.cashflow
            for label in ("Operating Cash Flow", "Total Cash From Operating Activities",
                          "Total Cash From Operating Activities Continuing Operations"):
                if label in annual.index:
                    values = annual.loc[label].dropna().head(1)
                    if len(values):
                        flows = [float(values.iloc[0])]
                        flow_months = 12
                        break
        except (AttributeError, KeyError, TypeError, ValueError):
            pass

    total_flow = sum(flows) if flows else None
    monthly_burn = (abs(total_flow) / flow_months
                    if total_flow is not None and total_flow < 0 else None)
    current_cash = reported_cash
    elapsed_months = 0.0
    if reported_cash is not None and cash_date is not None:
        elapsed_days = max(0.0, (pd.Timestamp.now().tz_localize(None) - cash_date).days)
        elapsed_months = elapsed_days / 30.4375
        if monthly_burn is not None:
            current_cash = max(0.0, reported_cash - monthly_burn * elapsed_months)
    runway = current_cash / monthly_burn if current_cash is not None and monthly_burn else None
    period_label = "latest 4 reported quarters" if len(flows) > 1 else "latest reported annual period"
    return (reported_cash, current_cash, monthly_burn, runway, cash_date,
            elapsed_months, period_label)


def dilution_risk_section():
    """Score dilution risk using the 3.5/3.5/3.0 user-defined rubric."""
    state = risk_state()
    overhang = state.get("warrant_overhang")
    filings = state.get("recent_dilution_filings", 0)
    cash = state.get("current_cash")
    reported_cash = state.get("reported_cash")
    cash_date = state.get("cash_date")
    cash_projection_unverified = state.get("cash_projection_unverified", False)
    post_report_financing = state.get("post_report_financing", 0.0)
    monthly_burn = state.get("monthly_burn")
    burn_period = state.get("burn_period")
    runway = state.get("runway")
    fade_ratio = state.get("fade_ratio")
    avg_close = state.get("average_gap_close")
    gap_count = state.get("gap_count", 0)
    reverse_splits = state.get("reverse_splits", 0)
    mechanisms = len(state.get("dilution_mechanisms", set()))

    if runway is None:
        runway_points = 0.0
        runway_basis = ("positive operating cash flow; runway points 0.0"
                        if monthly_burn is not None
                        and monthly_burn <= 0
                        else "cash runway unavailable")
    elif runway < 3:
        runway_points = 3.5
        runway_basis = f"runway {runway:.1f} months"
    elif runway < 5:
        runway_points = 3.0
        runway_basis = f"runway {runway:.1f} months"
    elif runway < 8:
        runway_points = 2.2
        runway_basis = f"runway {runway:.1f} months"
    elif runway < 12:
        runway_points = 1.4
        runway_basis = f"runway {runway:.1f} months"
    elif runway < 24:
        runway_points = 0.6
        runway_basis = f"runway {runway:.1f} months"
    else:
        runway_points = 0.0
        runway_basis = f"runway {runway:.1f} months"
    if cash is not None:
        cash_label = ("estimated cash today" if cash_projection_unverified and post_report_financing
                      else "latest reported cash" if cash_projection_unverified else "current cash")
        runway_basis = f"{cash_label} ${cash / 1e6:.1f}M; " + runway_basis
        if reported_cash is not None and cash_date is not None:
            runway_basis += f" (reported ${reported_cash / 1e6:.1f}M on {cash_date:%d %b %y})"
        if burn_period:
            runway_basis += f"; based on {burn_period}"
        if cash_projection_unverified:
            runway_basis += "; current balance unverified"

    dilution_points = min(3.5,
                          min(1.5, filings * 0.5) +
                          (1.0 if overhang is not None and overhang >= 1 else
                           0.75 if overhang is not None and overhang >= 0.5 else
                           0.4 if overhang else 0.0) +
                          min(0.5, mechanisms * 0.25) +
                          min(0.5, reverse_splits * 0.25))

    if gap_count == 0 or fade_ratio is None or avg_close is None:
        gap_points = 0.0
        gap_basis = "large-gap history unavailable"
    else:
        fade_points = (2.0 if fade_ratio >= 0.8 else 1.5 if fade_ratio >= 0.6 else
                       1.0 if fade_ratio >= 0.4 else 0.5 if fade_ratio >= 0.2 else 0.0)
        close_points = (1.0 if avg_close <= -0.10 else 0.8 if avg_close < 0 else
                        0.3 if avg_close < 0.10 else 0.0)
        gap_points = min(3.0, fade_points + close_points)
        gap_basis = (f"{fade_ratio:.0%} faded; average close {avg_close:+.1%} vs open; "
                     f"{gap_count} qualifying gap-ups")

    total = min(10.0, runway_points + dilution_points + gap_points)
    grade, label = (("A", "Extreme") if total >= 8 else
                    ("B", "High") if total >= 6 else
                    ("C", "Moderate") if total >= 4 else
                    ("D", "Low") if total >= 2 else ("E", "Very low"))
    return ["", f"DILUTION RISK: {total:.2f}/10 — Grade {grade} ({label})",
            f"  Cash runway: {runway_points:.1f}/3.5 ({runway_basis})",
            f"  Historical dilution: {dilution_points:.1f}/3.5 ({filings} filings; "
            f"{mechanisms} offering mechanism(s); {reverse_splits} reverse split(s))",
            f"  Gap-and-fade history: {gap_points:.1f}/3.0 ({gap_basis})"]


def warrants(cik, since):
    """Current XBRL warrant facts reported within the requested lookback window."""
    out = []
    try:
        gaap = sec_json(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json")["facts"]["us-gaap"]
    except Exception:
        return ["  n/a (no XBRL facts)"]
    for tag, label in [("ClassOfWarrantOrRightOutstanding", "Warrants outstanding"),
                       ("ClassOfWarrantOrRightExercisePriceOfWarrantsOrRights1", "Exercise price")]:
        for unit, rows in gaap.get(tag, {}).get("units", {}).items():
            recent = [row for row in rows if row.get("end", "") >= since.strftime("%Y-%m-%d")]
            if not recent:
                continue
            r = sorted(recent, key=lambda x: x["end"])[-1]
            v = r["val"]
            txt = f"${v}" if "USD" in unit else f"{v:,.0f}"
            extra = ""
            shares_out = risk_state().get("shares_out")
            if "USD" not in unit and shares_out:
                extra = f"  ({v/shares_out:.2f}x O/S)"
            out.append(f"  {label}: {txt}{extra}  [as of {r['end']}, {r['form']}]")
    return out or ["  none reported in XBRL"]


BULLET = re.compile(r"([\d,]+) shares of (?:our )?common stock issuable upon (?:the )?exercise of "
                    r"([^;|]{0,160}?)exercise price of \$([\d.,]+)", re.I)
PROSPECTUS_FORMS = {"S-1", "S-1/A", "S-3", "S-3/A", "S-3ASR",
                    "F-1", "F-1/A", "F-3", "F-3/A", "F-10", "F-10/A",
                    "40-F", "40-F/A", "424B3", "424B4", "424B5", "424F"}
FPI_FORMS = {"6-K", "20-F", "20-F/A", "40-F", "40-F/A",
             "F-1", "F-1/A", "F-3", "F-3/A", "F-10", "F-10/A"}
DOMESTIC_PERIODIC_FORMS = {"10-K", "10-K/A", "10-Q", "10-Q/A"}
AMENDMENT_FORMS = {"6-K/A", "8-K/A", "10-K/A", "10-Q/A", "20-F/A",
                   "40-F/A", "F-1/A", "F-3/A", "F-10/A", "S-1/A", "S-3/A"}
RELEVANT_FORMS = (FPI_FORMS | DOMESTIC_PERIODIC_FORMS | AMENDMENT_FORMS |
                  PROSPECTUS_FORMS | {"8-K", "8-K/A", "EFFECT", "RW",
                  "DEF 14A", "PRE 14A", "DEFA14A", "3", "4", "5",
                  "13D", "13D/A", "13G", "13G/A", "25", "25-NSE",
                  "12b-25", "15"})
# Forms that can actually carry offering / financial text. Skips insider (3/4/5),
# ownership (13D/G), EFFECT, RW, 25, 15, proxies - big savings and fewer false positives.
DILUTION_FORMS = ({"8-K", "8-K/A", "6-K", "6-K/A"} | DOMESTIC_PERIODIC_FORMS |
                  {"20-F", "20-F/A"} | PROSPECTUS_FORMS | AMENDMENT_FORMS)
# Event filings only (an actual offering/issuance shows up here). Periodic reports
# (10-K/10-Q/20-F/40-F) are excluded so they don't inflate the frequency count.
DILUTION_EVENT_FORMS = ({"8-K", "8-K/A", "6-K", "6-K/A"} |
                        (PROSPECTUS_FORMS - {"40-F", "40-F/A"}))
SKIP_DOC = re.compile(r"^R\d+\.htm|FilingSummary|index", re.I)


def filing_url(cik, filing):
    return (f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/"
            f"{filing.accessionNumber.replace('-', '')}/{filing.primaryDocument}")


def complete_filing_index(cik):
    """Combine current and archived SEC submission indexes without a fixed cap."""
    if cik in _sec_index_cache:
        return _sec_index_cache[cik].copy()
    try:
        data = sec_json(f"https://data.sec.gov/submissions/CIK{cik}.json")
        rows = []
        recent = data.get("filings", {}).get("recent", {})
        keys = ("form", "filingDate", "reportDate", "items", "accessionNumber",
                "primaryDocument")
        for i, _ in enumerate(recent.get("form", [])):
            rows.append({key: recent.get(key, [""] * len(recent["form"]))[i]
                         for key in keys})
        names = [a.get("name") for a in data.get("filings", {}).get("files", []) if a.get("name")]
        if names:
            with ThreadPoolExecutor(4) as ex:
                archives = list(ex.map(
                    lambda n: sec_json(f"https://data.sec.gov/submissions/{n}"), names))
            for archived in archives:
                for i, _ in enumerate(archived.get("form", [])):
                    rows.append({key: archived.get(key, [""] * len(archived["form"]))[i]
                                 for key in keys})
        frame = normalized_filing_columns(pd.DataFrame(rows))
        if frame.empty:
            return frame
        frame["filingDate"] = pd.to_datetime(frame["filingDate"], errors="coerce")
        frame["reportDate"] = pd.to_datetime(frame["reportDate"], errors="coerce")
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=365 * SEC_LOOKBACK_YEARS)
        result = frame[
            frame["filingDate"].notna() & (frame["filingDate"] >= cutoff)
        ].drop_duplicates(["accessionNumber", "form"]).sort_values(
            "filingDate", ascending=False)
        _sec_index_cache[cik] = result
        return result.copy()
    except (KeyError, TypeError, ValueError, requests.RequestException):
        return pd.DataFrame()


def filing_documents(cik, filing):
    """Return the primary document and every HTML exhibit in its SEC directory."""
    accession = filing.accessionNumber.replace("-", "")
    base = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession}/"
    documents = [filing.primaryDocument]
    try:
        listing = sec_json(base + "index.json", ttl=86400)
        for item in listing.get("directory", {}).get("item", []):
            name = item.get("name", "")
            if name.lower().endswith((".htm", ".html")) and not SKIP_DOC.search(name):
                documents.append(name)
    except (KeyError, TypeError, ValueError, requests.RequestException):
        pass
    return list(dict.fromkeys(base + name for name in documents if name))


def _safe_fetch(url):
    try:
        return fetch_text(url)
    except requests.RequestException:
        return ""


def filing_text(cik, filing):
    """Read all discoverable filing documents (in parallel), cached in memory + disk."""
    cache_key = (cik, filing.accessionNumber)
    if cache_key in _sec_text_cache:
        return _sec_text_cache[cache_key]
    urls = filing_documents(cik, filing)
    with ThreadPoolExecutor(6) as ex:
        texts = [(u, t) for u, t in zip(urls, ex.map(_safe_fetch, urls)) if t]
    _sec_text_cache[cache_key] = texts
    return texts


def prefetch_filings(cik, rows):
    """Warm the filing-text cache for many filings at once."""
    if not rows:
        return
    with step(f"loading text of {len(rows)} filing(s) in parallel (cached ones are instant)", "sec"):
        with ThreadPoolExecutor(8) as ex:
            list(ex.map(lambda r: filing_text(cik, r), rows))


def normalized_filing_columns(frame):
    """Keep SEC filing metadata safe when optional fields are blank or missing."""
    frame = frame.copy()
    frame["form"] = frame["form"].fillna("").astype(str).str.upper().str.strip()
    frame["items"] = frame["items"].fillna("").astype(str)
    return frame


def filing_has_dilution(text):
    return bool(DIL_KEYS.search(text) and re.search(
        r"\b(shares?|warrants?|convertible|securities|offering|placement)\b", text, re.I))


def parse_raised_proceeds(text):
    """Extract actual raised/proceeds amounts, excluding unused offering capacity."""
    amounts = []
    for sentence in re.split(r"(?<=[.;]) ", text):
        if re.search(r"\b(gross proceeds|net proceeds|proceeds received|raised)\b",
                     sentence, re.I) and not re.search(
                         r"\b(may sell|offering capacity|remaining capacity|potential proceeds)\b",
                         sentence, re.I):
            matches = re.findall(
                r"\$\s?([\d,.]+)\s*(million|thousand|billion)?", sentence, re.I)
            for value, unit in matches:
                amount = float(value.replace(",", ""))
                multiplier = {"thousand": 1e3, "million": 1e6, "billion": 1e9}.get(
                    (unit or "").lower(), 1)
                amounts.append(amount * multiplier)
    return max(amounts) if amounts else 0.0


def sec_latest_cash(cik):
    """Get the latest reported cash balance from SEC XBRL facts."""
    tags = ("Cash",
            "CashAndCashEquivalentsAtCarryingValue",
            "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents")
    try:
        facts = sec_json(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json")["facts"]["us-gaap"]
        cutoff = (pd.Timestamp.now() -
                  pd.Timedelta(days=365 * SEC_LOOKBACK_YEARS)).strftime("%Y-%m-%d")
        candidates = []
        for tag in tags:
            for unit, rows in facts.get(tag, {}).get("units", {}).items():
                if unit != "USD":
                    continue
                for row in rows:
                    if (row.get("form") in RELEVANT_FORMS and
                            row.get("end", "") >= cutoff):
                        candidates.append(row)
        if not candidates:
            return None
        row = max(candidates, key=lambda item: (item.get("end", ""), item.get("filed", "")))
        return float(row["val"]), pd.Timestamp(row["end"]), row.get("form", "")
    except (KeyError, TypeError, ValueError, requests.RequestException):
        return None


def sec_latest_filing_financials(cik):
    """Extract dated cash-flow figures from the newest 6-K / 8-K exhibits that parse."""
    try:
        filings = complete_filing_index(cik)
        if filings.empty:
            return None
        financial_cutoff = pd.Timestamp.now() - pd.Timedelta(days=365 * SEC_LOOKBACK_YEARS)
        candidates = filings[
            (filings.filingDate >= financial_cutoff) &
            (
                filings.form.isin({"6-K", "6-K/A"}) |
                (filings.form.isin({"8-K", "8-K/A"}) &
                 filings["items"].str.contains(r"2\.02|8\.01|9\.01", regex=True))
            )
        ].sort_values("filingDate", ascending=False).head(12)

        best = None
        for filing in candidates.itertuples():
            filing_date = pd.Timestamp(str(filing.filingDate))
            for _, text in filing_text(cik, filing):
                date_match = re.search(
                    r"As of As of\s+([A-Za-z]+\s+\d+),\s+[A-Za-z]+\s+\d+,\s+(\d{4})",
                    text, re.I)
                cash_match = re.search(
                    r"\bCash\s+\$?\s*([\d,]+(?:\.\d+)?)\s+\$?\s*[\d,]+",
                    text, re.I)
                operating_match = re.search(
                    r"Net cash\s+\(used in\)\s+provided by operating activities"
                    r"\s+\(?\$?\s*([\d,]+(?:\.\d+)?)\s*\)?",
                    text, re.I)
                if not date_match or not cash_match or not operating_match:
                    continue
                statement_date = pd.Timestamp(
                    f"{date_match.group(1)} {date_match.group(2)}")
                cash = float(cash_match.group(1).replace(",", ""))
                operating_value = -float(operating_match.group(1).replace(",", ""))
                period_match = re.search(
                    r"for the six months ended\s+" + re.escape(date_match.group(1))
                    + r"\s*,?\s*" + re.escape(date_match.group(2)),
                    text, re.I)
                months = 6 if period_match else 12
                result = (cash, statement_date, "6-K", abs(operating_value) / months,
                          f"{statement_date:%d %b %Y} financial statements; "
                          f"Form 6-K filed {filing_date:%d %b %Y}")
                if best is None or statement_date > best[1]:
                    best = result
            if best:  # newest filing that parsed is enough - stop downloading
                break
        return best
    except (KeyError, TypeError, ValueError, AttributeError, requests.RequestException):
        return None


def sec_latest_shares(cik):
    """Get the latest SEC-reported common shares outstanding from XBRL or cover page."""
    cutoff = (pd.Timestamp.now() -
              pd.Timedelta(days=365 * SEC_LOOKBACK_YEARS)).strftime("%Y-%m-%d")
    try:
        facts = sec_json(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json")["facts"]["dei"]
        rows = facts.get("EntityCommonStockSharesOutstanding", {}).get("units", {}).get("shares", [])
        rows = [row for row in rows if row.get("form") in RELEVANT_FORMS
                and row.get("end", "") >= cutoff]
        if rows:
            row = max(rows, key=lambda item: (item.get("end", ""), item.get("filed", "")))
            return float(row["val"]), pd.Timestamp(row["end"]), row.get("form", "")
    except (KeyError, TypeError, ValueError, requests.RequestException):
        pass

    # Fallback: scan cover page of newest 10-K / 10-Q / 20-F
    try:
        f = complete_filing_index(cik)
        if not f.empty:
            periodics = f[f.form.isin({"10-K", "10-K/A", "10-Q", "10-Q/A", "20-F", "20-F/A"})].sort_values("filingDate", ascending=False).head(3)
            for r in periodics.itertuples():
                for _, text in filing_text(cik, r):
                    m = re.search(r'(?:as of|on)\s+([A-Za-z]+\s+\d+,\s+\d{4})[^\.\n]{0,80}?(?:had|were)\s+([\d,]+)\s+shares of (?:Class A\s+)?common stock', text, re.I)
                    if m:
                        cnt = float(m.group(2).replace(",", ""))
                        dt = pd.to_datetime(m.group(1))
                        return cnt, dt, r.form
    except Exception:
        pass
    return None


def sec_latest_operating_burn(cik):
    """Get monthly operating cash burn from the latest SEC-reported cash flow."""
    tags = ("NetCashProvidedByUsedInOperatingActivities",
            "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations")
    try:
        facts = sec_json(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json")["facts"]["us-gaap"]
        cutoff = (pd.Timestamp.now() -
                  pd.Timedelta(days=365 * SEC_LOOKBACK_YEARS)).strftime("%Y-%m-%d")
        candidates = []
        for tag in tags:
            for unit, rows in facts.get(tag, {}).get("units", {}).items():
                if unit != "USD":
                    continue
                for row in rows:
                    if row.get("form") not in {"10-Q", "10-Q/A", "10-K", "10-K/A", "20-F", "20-F/A"}:
                        continue
                    if row.get("end", "") < cutoff:
                        continue
                    if not row.get("start") or not row.get("end") or row.get("val", 0) >= 0:
                        continue
                    start = pd.Timestamp(row["start"])
                    end = pd.Timestamp(row["end"])
                    months = max(1, round((end - start).days / 30.4375))
                    candidates.append((row, months))
        if not candidates:
            return None
        row, months = max(candidates, key=lambda item: (item[0].get("end", ""), item[0].get("filed", "")))
        return abs(float(row["val"])) / months, row["start"], row["end"], row.get("form", "")
    except (KeyError, TypeError, ValueError, requests.RequestException):
        return None


def open_dilution(cik, f):
    """Keep ONLY potential new-share sources:
    1. ATM programs: remaining capacity in $, agent, and implied new shares (capacity / last price)
    2. Warrants: common, replacement, Series A/B, inducement, and pre-funded
    3. Other dilutive instruments: convertible notes, convertible preferred, equity lines/ELOC, options/RSUs (if material)
    """
    out = []
    so = risk_state().get("shares_out")
    last_price = risk_state().get("last_price")

    # 1. ATM Programs
    atm_prog = None
    KNOWN_AGENTS = [
        "Maxim Group", "B. Riley Securities", "B. Riley", "H.C. Wainwright", "Cantor Fitzgerald", 
        "Virtu Americas", "Virtu", "JonesTrading", "Alliance Global Partners", "A.G.P.", 
        "Roth Capital Partners", "Roth Capital", "ThinkEquity", "Craig-Hallum", "Chardan", 
        "Ladenburg Thalmann", "Canaccord Genuity", "Stifel", "BTIG", "EF Hutton", 
        "Dawson James", "Aegis Capital", "Cowen and Company", "TD Cowen", "Cowen", 
        "Oppenheimer", "Piper Sandler", "William Blair", "Barclays", "Citigroup", 
        "Goldman Sachs", "J.P. Morgan", "Morgan Stanley"
    ]
    atm_filings = f[f.form.isin({"424B5", "10-K", "10-K/A", "10-Q", "10-Q/A", "8-K", "6-K", "20-F"})].sort_values("filingDate", ascending=False).head(8)
    for r in atm_filings.itertuples():
        for _, text in filing_text(cik, r):
            if re.search(r'\b(?:at-the-market|at the market|equity distribution agreement|sales agreement|capital on demand)\b', text, re.I):
                m_cap = re.search(r'(?:aggregate (?:offering )?price of up to|capacity of up to|remaining capacity of|available for sale[^\$]{0,40}|offering of up to|aggregate sales price of up to|aggregate gross proceeds of up to)\s*\$\s?([\d\.,]+)\s*(million|billion|thousand)?', text, re.I)
                if not m_cap:
                    m_cap = re.search(r'(?:sales agreement|equity distribution agreement|at-the-market offering)[^\.]{0,100}?(?:up to|having an aggregate offering price of up to)\s*\$\s?([\d\.,]+)\s*(million|billion|thousand)?', text, re.I)
                
                # Identify sales agent
                agent_name = None
                for ag in KNOWN_AGENTS:
                    if re.search(rf'\b{re.escape(ag)}\b', text, re.I):
                        agent_name = ag
                        break
                if not agent_name:
                    m_agent = re.search(r'(?:with|through)\s+([A-Za-z0-9\.,\s&]+?)\s+(?:as\s+(?:sales\s+)?agent|as\s+sales\s+representative|as\s+placement\s+agent)', text, re.I)
                    if m_agent:
                        raw = m_agent.group(1).strip()
                        raw = re.sub(r'^(?:by\s+|from\s+|to\s+|or\s+|and\s+|the\s+)+', '', raw, flags=re.I)
                        raw = re.sub(r'(?:,\s*acting\s+as.*|,\s*as.*|\s+acting.*)$', '', raw, flags=re.I)
                        agent_name = raw.strip(",. ")
                if not agent_name:
                    agent_name = "sales agent"

                if m_cap:
                    val = float(m_cap.group(1).replace(",", ""))
                    mult = {"thousand": 1e3, "million": 1e6, "billion": 1e9}.get((m_cap.group(2) or "").lower(), 1e6 if val < 500 else 1.0)
                    cap_amt = val * mult
                    atm_prog = {"capacity": cap_amt, "agent": agent_name, "form": r.form, "date": r.filingDate}
                    break
        if atm_prog:
            break

    # 2. Warrants: common, replacement, Series A/B, inducement, and pre-funded
    warrants = []
    warrant_seen = set()

    # Check NEWEST prospectus FIRST (only the single newest; do not concatenate historical filings)
    p_filings = f[f.form.isin(PROSPECTUS_FORMS)].sort_values("filingDate", ascending=False)
    if not p_filings.empty:
        r_newest_p = p_filings.iloc[0]
        p_texts = [t for _, t in filing_text(cik, r_newest_p)]
        t_p = " ".join(p_texts)

        # Check headline registered / replacement / inducement warrants in this prospectus
        w_head = re.search(r'(?:plus an aggregate of\s+|purchase up to\s+|exercise of\s+)([\d,]+)\s+shares[^\.]{0,80}?exercise of (?:the\s+)?(?:New\s+|replacement\s+|inducement\s+)?Warrants', t_p, re.I)
        if not w_head:
            w_head = re.search(r'(?:purchase|offering of|exercise of)[^\.]{0,40}?(?:up to\s+)?([\d,]+)\s+(?:shares of common stock issuable upon exercise of\s+)?(?:the\s+)?(?:New\s+|replacement\s+|inducement\s+)?Warrants', t_p, re.I)

        px_head = re.search(r'exercise price of (?:the\s+)?(?:New\s+|replacement\s+|inducement\s+)?Warrants\s+(?:is|shall be|per share is)\s+\$([\d\.]+)', t_p, re.I)
        if not px_head:
            px_head = re.search(r'(?:the\s+)?Warrants\s+(?:have an|with an|at an)\s+exercise price of\s+\$([\d\.]+)', t_p, re.I)
        if not px_head:
            px_head = re.search(r'exercise price (?:per share\s+)?(?:is\s+|of\s+)\$([\d\.]+)', t_p, re.I)
        if not px_head:
            pr_m = re.search(r'aggregate gross proceeds of (?:approximately\s+)?\$([\d\.,]+)', t_p, re.I)
            if pr_m and w_head:
                cnt_temp = int(w_head.group(1).replace(",", ""))
                gross_temp = float(pr_m.group(1).replace(",", ""))
                px_val = f"{gross_temp / cnt_temp:.2f}"
                class TempMatch:
                    def group(self, n): return px_val
                px_head = TempMatch()

        if w_head:
            cnt = int(w_head.group(1).replace(",", ""))
            px = px_head.group(1) if px_head else "?"
            pend = " (unexercised; subject to stockholder approval)" if re.search(r"not exercisable until[^.]{0,40}Stockholder Approval", t_p, re.I) else " (unexercised)"
            label = "replacement / inducement warrants" if "replacement" in t_p.lower() or "inducement" in t_p.lower() else "common warrants"
            key = (cnt, str(px))
            if key not in warrant_seen and cnt > 100:
                warrant_seen.add(key)
                warrants.append({"count": cnt, "label": label, "price": px, "notes": pend, "type": "inducement"})

        # Check the 'excludes as of such date:' footnote in THIS newest prospectus
        exc_pos = -1
        for kw in ["excludes as of such date:", "excludes as of", "excludes the following:", "excludes:", "excluding:"]:
            idx = t_p.find(kw)
            if idx != -1:
                exc_pos = idx + len(kw)
                break

        if exc_pos != -1:
            exc_chunk = t_p[exc_pos:exc_pos + 2500]
            # 1. additional / common outstanding warrants
            m_add = re.search(r'([\d,]+)\s+shares[^\.;]{0,60}?(?:additional\s+)?(?:outstanding\s+)?warrants?[^\.;]{0,80}?(?:exercise price of|at an? exercise price of)\s*\$([\d\.]+)', exc_chunk, re.I)
            if m_add:
                cnt, px = int(m_add.group(1).replace(",", "")), m_add.group(2)
                key = (cnt, str(px))
                if key not in warrant_seen:
                    warrant_seen.add(key)
                    warrants.append({"count": cnt, "label": "common warrants", "price": px, "notes": "", "type": "common"})

            # 2. pre-funded warrants
            m_pref = re.search(r'([\d,]+)\s+shares[^\.;]{0,60}?pre-funded warrants?[^\.;]{0,80}?(?:exercise price of|at an? exercise price of)\s*\$([\d\.]+)', exc_chunk, re.I)
            if m_pref:
                cnt, px = int(m_pref.group(1).replace(",", "")), m_pref.group(2)
                key = (cnt, str(px))
                if key not in warrant_seen:
                    warrant_seen.add(key)
                    warrants.append({"count": cnt, "label": "pre-funded warrants", "price": px, "notes": "", "type": "prefunded"})

            # 3. Series A/B warrants
            m_ser = re.findall(r'([\d,]+)\s+shares[^\.;]{0,60}?(Series\s+[A-Za-z0-9]+(?:\s+and\s+Series\s+[A-Za-z0-9]+)?\s+Warrants?)[^\.;]{0,80}?(?:exercise price of|at an? exercise price of)\s*\$([\d\.]+)', exc_chunk, re.I)
            for cnt_s, s_name, px in m_ser:
                cnt = int(cnt_s.replace(",", ""))
                key = (cnt, str(px))
                if key not in warrant_seen:
                    warrant_seen.add(key)
                    warrants.append({"count": cnt, "label": s_name, "price": px, "notes": "", "type": "series_ab"})

    # If no warrants parsed from prospectus, check Closing 8-K / 8-A / 10-K (e.g. XLAB de-SPAC)
    if not warrants:
        w_pats = [
            re.compile(r"([\d,]+)\s+(?:PubCo\s+|public\s+|private\s+placement\s+)?warrants?\s+(?:issued and\s+)?outstanding[^\.]{0,140}?(?:entitling|exercisable|to purchase)[^\.]{0,140}?(?:at an? exercise price of|at a price of|exercise price per share[^\$]{0,30})\s*\$([\d\.]+)", re.I),
            re.compile(r"as of the Closing Date[^\.]{0,60}?(?:had|were)\s+([\d,]+)\s+(?:PubCo\s+)?warrants?\s+(?:issued and\s+)?outstanding[^\.]{0,140}?(?:entitling|exercisable|to purchase)[^\.]{0,140}?(?:at an? exercise price of|at a price of)\s*\$([\d\.]+)", re.I),
        ]
        closing_cands = f[f.form.isin({"8-K", "8-K/A", "10-K", "10-K/A", "8-A12B"})].sort_values("filingDate", ascending=False)
        for cand in closing_cands.itertuples():
            for _, c_text in filing_text(cik, cand):
                for pat in w_pats:
                    m = pat.search(c_text)
                    if m:
                        cnt = int(m.group(1).replace(",", ""))
                        px = m.group(2)
                        key = (cnt, str(px))
                        if key not in warrant_seen:
                            warrant_seen.add(key)
                            warrants.append({"count": cnt, "label": "public & private warrants", "price": px, "notes": " (unexercised)", "type": "common"})
                            break
                if warrants:
                    break
            if warrants:
                break

    # 3. Other Dilutive Instruments: convertible notes, convertible preferred, equity lines/ELOC, options/RSUs (if material)
    other = []
    other_seen = set()
    periodic = f[f.form.isin({"10-K", "10-K/A", "10-Q", "10-Q/A", "8-K", "6-K", "20-F"})].sort_values("filingDate", ascending=False).head(5)
    for r in periodic.itertuples():
        for _, text in filing_text(cik, r):
            # SAFEs
            safe_m = re.search(r'on\s+([A-Za-z]+\s+\d+,\s+\d{4})[^\.]{0,80}?(?:sold and issued|issued|provided)[^\.]{0,40}?SAFE[^\.]{0,40}?\$\s?([\d\.]+)\s*(million|thousand)?', text, re.I)
            if safe_m and "safe" not in other_seen:
                other_seen.add("safe")
                s_amt = safe_m.group(2)
                s_date = safe_m.group(1)
                other.append(f"${s_amt}M convertible SAFE issued {s_date} (convertible into common equity)")

            # Equity Line of Credit (ELOC) / SEPA
            eloc_m = re.search(r'(?:committed equity facility|standby equity purchase agreement|equity line of credit|SEPA)[^\.]{0,80}?(?:with\s+([A-Za-z0-9\.,\s]+?)[^\.]{0,40}?)?up to\s*\$\s?([\d\.,]+)\s*(million|billion)?', text, re.I)
            if eloc_m and "eloc" not in other_seen:
                other_seen.add("eloc")
                investor = eloc_m.group(1).strip() if eloc_m.group(1) else "committed investor"
                cap = eloc_m.group(2)
                unit = eloc_m.group(3) or "million"
                other.append(f"${cap}{unit[0].upper()} Equity Line of Credit (ELOC) with {investor}")

            # Convertible Notes / Debentures
            cn_m = re.search(r'(?:senior\s+)?convertible (?:promissory\s+)?(?:notes?|debentures?)[^\.]{0,80}?aggregate principal amount of\s*\$\s?([\d\.,]+)\s*(million|thousand)?', text, re.I)
            if cn_m and "convertible_note" not in other_seen:
                other_seen.add("convertible_note")
                amt = cn_m.group(1)
                unit = cn_m.group(2) or ""
                other.append(f"${amt}{unit} convertible promissory notes (convertible into common shares)")

            # Convertible Preferred Stock
            pref_m = re.search(r'(?:issued|outstanding|designated)[^\.]{0,60}?(Series\s+[A-Za-z0-9]+\s+Convertible\s+Preferred\s+Stock|convertible\s+preferred\s+stock)[^\.]{0,60}?(?:convertible into (?:up to )?([\d,]+)\s+shares)?', text, re.I)
            if pref_m and "convertible_preferred" not in other_seen:
                other_seen.add("convertible_preferred")
                p_name = pref_m.group(1).strip()
                p_sh = f" (convertible into ~{pref_m.group(2)} shares)" if pref_m.group(2) else ""
                other.append(f"{p_name}{p_sh}")

    # Material Options / RSUs (only if material, > 5% of shares outstanding)
    if so and not p_filings.empty:
        r_newest_p = p_filings.iloc[0]
        t_p = " ".join(t for _, t in filing_text(cik, r_newest_p))
        opt_m = re.search(r'([\d,]+)\s+shares[^\.;]{0,60}?exercise of stock options[^\.;]{0,80}?exercise price of \$([\d\.]+)', t_p, re.I)
        if opt_m:
            opt_cnt = int(opt_m.group(1).replace(",", ""))
            opt_px = opt_m.group(2)
            if opt_cnt / so > 0.05:  # Material only (>5% O/S)
                other.append(f"{opt_cnt:,} stock options @ ${opt_px} ({opt_cnt/so:.1%} of shares outstanding)")
        rsu_m = re.search(r'([\d,]+)\s+shares[^\.;]{0,60}?(?:unvested\s+)?(?:RSUs|restricted stock units)', t_p, re.I)
        if rsu_m:
            rsu_cnt = int(rsu_m.group(1).replace(",", ""))
            if rsu_cnt / so > 0.05:
                other.append(f"{rsu_cnt:,} restricted stock units (RSUs) ({rsu_cnt/so:.1%} of shares outstanding)")

    # BUILD FINAL CLEAN OUTPUT (Strictly keeping only potential new-share sources)
    # 1. ATM Program
    if atm_prog:
        cap_val = atm_prog['capacity']
        impl_str = ""
        if last_price and last_price > 0:
            impl_shares = int(cap_val / last_price)
            impl_str = f" (~{impl_shares:,} implied new shares @ ${last_price:.2f})"
        out.append(f"  ATM program: ${cap_val/1e6:.2f}M remaining capacity via {atm_prog['agent']}{impl_str}")
    else:
        out.append("  ATM program: none active or reported")

    # 2. Warrants
    if warrants:
        out.append("  Warrants:")
        tot_warrants = 0
        for w in warrants:
            cnt = w['count']
            tot_warrants += cnt
            px_str = f" @ ${w['price']}" if w['price'] != "?" else ""
            out.append(f"    - {cnt:,} {w['label']}{px_str}{w['notes']}")
        if so:
            risk_state()["warrant_overhang"] = tot_warrants / so
            out.append(f"    Total warrant overhang: {tot_warrants:,} shares ({tot_warrants/so:.2f}x O/S)")
        else:
            out.append(f"    Total warrant overhang: {tot_warrants:,} shares")
    else:
        risk_state()["warrant_overhang"] = 0.0
        out.append("  Warrants: none reported")

    # 3. Other Dilutive Instruments
    if other:
        out.append("  Other dilutive instruments:")
        for o in other:
            out.append(f"    - {o}")
    else:
        out.append("  Other dilutive instruments: none material reported")

    return out


def news_tags(sym):
    out = []
    try:
        items = yf.Ticker(sym).news or []
        for it in items[:8]:
            c = it.get("content", it)
            title = c.get("title", "")
            when = (c.get("pubDate") or "")[:10]
            tags = [k for k, rx in TAGS.items() if re.search(rx, title, re.I)]
            out.append(f"  {when} [{'/'.join(tags) or '-'}] {title[:90]}")
    except Exception:
        return ["  n/a"]
    return out or ["  no recent news"]


def prefetch_sec(sym):
    """Warm SEC caches (JSON + filing text) in a background thread.
    Must NOT touch risk_state() / flask.g - it only fills caches."""
    try:
        cik = get_cik(sym)
        if not cik:
            log(f"{sym}: not found on EDGAR (no SEC data)", "sec")
            return
        log(f"{sym}: CIK {cik} - downloading XBRL facts + filing index", "sec")
        with ThreadPoolExecutor(2) as ex:
            ex.submit(sec_json, f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json")
            fut = ex.submit(complete_filing_index, cik)
            f = fut.result()
        if f.empty:
            log(f"{sym}: SEC filing index unavailable", "sec")
            return
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=DILUTION_LOOKBACK_DAYS)
        rows = f[(f.filingDate > cutoff) & f.form.isin(DILUTION_EVENT_FORMS)]
        log(f"{sym}: {len(f)} filings in index, {len(rows)} relevant to scan", "sec")
        prefetch_filings(cik, list(rows.itertuples()))
        log(f"{sym}: SEC prefetch complete", "sec")
    except Exception as e:
        log(f"{sym}: SEC prefetch error: {e}", "sec")


def sec_section(sym):
    out = []
    risk_state()["recent_dilution_filings"] = 0
    risk_state()["dilution_mechanisms"] = set()
    cik = get_cik(sym)
    if not cik:
        return ["SEC: ticker not found on EDGAR"], None
    log(f"{sym} -> CIK {cik}", "sec")
    with step("reading XBRL cash / shares / burn", "sec"):
        latest_cash = sec_latest_cash(cik)
    with step("scanning newest 6-K/8-K financial statements", "sec"):
        filing_financials = sec_latest_filing_financials(cik)
    if filing_financials and (
            latest_cash is None or filing_financials[1] > latest_cash[1]):
        latest_cash = filing_financials[:3]
    latest_shares = sec_latest_shares(cik)
    if latest_shares:
        risk_state()["shares_out"] = latest_shares[0]
        risk_state()["shares_source"] = "SEC"
        risk_state()["shares_date"] = latest_shares[1]
    latest_burn = sec_latest_operating_burn(cik)
    if filing_financials and (
            latest_burn is None or filing_financials[1] >= pd.Timestamp(latest_burn[2])):
        latest_burn = (
            filing_financials[3],
            f"{filing_financials[1] - pd.DateOffset(months=6):%Y-%m-%d}",
            f"{filing_financials[1]:%Y-%m-%d}",
            filing_financials[4],
        )
    if latest_cash:
        cash, cash_date, _ = latest_cash
        risk_state()["reported_cash"] = cash
        risk_state()["current_cash"] = cash
        risk_state()["cash_date"] = cash_date
        risk_state()["cash_projection_unverified"] = True
        if risk_state().get("monthly_burn"):
            risk_state()["runway"] = cash / risk_state()["monthly_burn"]
    if latest_burn:
        risk_state()["monthly_burn"] = latest_burn[0]
        risk_state()["burn_period"] = f"{latest_burn[1]} to {latest_burn[2]} ({latest_burn[3]})"
        if latest_cash:
            risk_state()["runway"] = latest_cash[0] / latest_burn[0]
    else:
        risk_state()["cash_projection_unverified"] = True
    f = complete_filing_index(cik)
    if f.empty:
        return out + ["SEC filing index unavailable"], cik
    try:
        j = sec_json(f"https://data.sec.gov/submissions/CIK{cik}.json")
    except (KeyError, TypeError, ValueError, requests.RequestException):
        j = {}
    now = pd.Timestamp.now()
    y1 = now - pd.Timedelta(days=365 * SEC_LOOKBACK_YEARS)
    y3 = y1

    for n in j.get("formerNames", [])[:3]:
        out.append(f"Former name: {n['name']} (until {n['to'][:10]})")
    out.append("")

    is_fpi = bool(set(f.form.unique()) & FPI_FORMS)
    if is_fpi:
        out.append("Issuer type: foreign private issuer (6-K/20-F reporting)")
    else:
        out.append("Issuer type: domestic issuer (8-K/10-K/10-Q reporting)")

    # Exchange deficiency notices use 8-K Item 3.01 for domestic issuers;
    # foreign private issuers generally report comparable notices on 6-K.
    d = f[(f.filingDate > y1) & (
        ((f.form.isin({"8-K", "8-K/A"}) & f["items"].str.contains("3.01")) |
         (is_fpi & f.form.isin({"6-K", "6-K/A", "20-F", "20-F/A", "40-F", "40-F/A"}))))].sort_values(
            "filingDate", ascending=False)
    label = f"Exchange/issuer notices ({SEC_LOOKBACK_YEARS}y)"
    out.append(label + ": " +
               (", ".join(f"{r.form} {r.filingDate:%d %b %y}" for r in d.itertuples())
                if len(d) else "none found"))
    # Shelf / ATM indicators
    shelf = f[f.form.isin(PROSPECTUS_FORMS) & (f.filingDate > y3)]
    out.append(f"Shelf/registration ({SEC_LOOKBACK_YEARS}y): " +
               (", ".join(f"{r.form} {r.filingDate:%d %b %y}" for r in shelf.itertuples()) or "n/a"))
    atm = f[f.form.str.startswith("424B") & (f.filingDate > y1)]
    out.append(f"Prospectus supplements 424B ({SEC_LOOKBACK_YEARS}y): " +
               (", ".join(f"{r.form} {r.filingDate:%d %b %y}" for r in atm.itertuples()) or "n/a"))
    out.append("")
    if latest_cash:
        out.append(f"Latest confirmed SEC cash: ${latest_cash[0] / 1e6:.3f}M "
                   f"({latest_cash[1]:%d %b %Y}, Form {latest_cash[2]})")
        if risk_state().get("monthly_burn"):
            out.append(f"Reported operating cash burn: ${risk_state()['monthly_burn'] / 1e6:.3f}M/month")
    else:
        out.append("Latest confirmed SEC cash: unavailable")
    if latest_shares:
        out.append(f"Latest confirmed SEC shares: {latest_shares[0]:,.0f} "
                   f"({latest_shares[1]:%d %b %Y}, Form {latest_shares[2]})")
    else:
        out.append("Latest confirmed SEC shares: unavailable")
    out.append("")
    out.append("Open dilution:")
    with step("analyzing open dilution (ATM, warrants, dilutive instruments)", "sec"):
        out += open_dilution(cik, f)
    out.append("")
    out.append("Last 12 months - securities dilution history:")
    dil_cutoff = now - pd.Timedelta(days=DILUTION_LOOKBACK_DAYS)
    candidates = f[(f.filingDate >= dil_cutoff)].sort_values("filingDate", ascending=False)
    
    completed = []
    potential_table = []
    administrative = []
    seen = set()
    risk_state()["dilution_mechanisms"] = set()
    
    for r in candidates.itertuples():
        form = r.form.upper()
        items = [it.strip() for it in str(r.items).split(",") if it.strip()]
        
        # 1. Skip insider & ownership filings
        if form in {'3', '4', '5', 'SCHEDULE 13G', '13G', '13D', '13D/A', 'CERT', 'EFFECT', 'RW', 'DEF 14A', 'PRE 14A'}:
            continue
            
        # 2. Shelf / Prospectuses / Registrations
        if form in {'424B3', '424B4', '424B5', '424F', 'S-1', 'S-3', 'S-4', 'F-1', 'F-3', 'F-10', '8-A12B', '8-A12G'}:
            administrative.append({
                "date": r.filingDate,
                "form": form,
                "type": "registration",
                "label": f"- {r.filingDate:%d %b %Y}: {form} registration filing; registration does not itself mean shares were sold."
            })
            continue
            
        # 3. Amendments
        if "/A" in form:
            administrative.append({
                "date": r.filingDate,
                "form": form,
                "type": "amendment",
                "label": f"- {r.filingDate:%d %b %Y}: Form {form} amendment to prior filing."
            })
            continue

        # 4. Standard excluded operational & governance items
        if form == "8-K":
            if set(items).issubset({'5.02', '9.01', '7.01'}):
                administrative.append({"date": r.filingDate, "form": form, "type": "governance", "label": f"- {r.filingDate:%d %b %Y}: Form 8-K (Item 5.02) governance / director appointment - no securities issuance."})
                continue
            if set(items).issubset({'2.02', '9.01', '7.01'}):
                administrative.append({"date": r.filingDate, "form": form, "type": "earnings", "label": f"- {r.filingDate:%d %b %Y}: Form 8-K (Item 2.02) financial results release - operational update, no securities issuance."})
                continue
            if set(items).issubset({'3.01', '9.01', '7.01'}):
                administrative.append({"date": r.filingDate, "form": form, "type": "exchange_notice", "label": f"- {r.filingDate:%d %b %Y}: Form 8-K (Item 3.01) exchange notice - no securities issuance."})
                continue
            if set(items).issubset({'4.01', '9.01'}):
                administrative.append({"date": r.filingDate, "form": form, "type": "auditor", "label": f"- {r.filingDate:%d %b %Y}: Form 8-K (Item 4.01) auditor update - no securities issuance."})
                continue

        texts = filing_text(cik, r)
        combined = " ".join(t for _, t in texts)
        tx_date = r.reportDate if pd.notna(r.reportDate) and form == "8-K" else r.filingDate

        # Check commercial contract Item 1.01 (e.g. Sunon in XLAB)
        if form == "8-K" and set(items).issubset({'1.01', '9.01'}):
            if not re.search(r'\b(common stock|shares|warrants|convertible|equity financing|registered direct|private placement)\b', combined, re.I) or re.search(r'purchase and sale agreement\b|supply agreement\b|license agreement\b', combined, re.I):
                if not re.search(r'\b(?:issued|issuance of)\s+[\d,]+\s+shares\b', combined, re.I):
                    administrative.append({"date": r.filingDate, "form": form, "type": "commercial", "label": f"- {r.filingDate:%d %b %Y}: Commercial definitive agreement (Item 1.01); operational agreement, no equity issued."})
                    continue

        # 5. Check Business Combination Closing (Item 2.01)
        if form == "8-K" and ('2.01' in items or re.search(r'\bconsummat\w+\s+(?:the\s+)?business combination\b', combined, re.I)):
            m_shares = re.search(r'([\d,]+)\s+shares of (?:PubCo\s+)?Class A[^\.]{0,120}?and ([\d,]+)\s+shares of (?:PubCo\s+)?Class B', combined, re.I)
            m_val = re.search(r'aggregate merger consideration of \$([\d,\.]+)\s*(million|billion)?', combined, re.I)
            if 'business_combination' not in seen:
                seen.add('business_combination')
                a_sh = m_shares.group(1) if m_shares else "unknown"
                b_sh = m_shares.group(2) if m_shares else "unknown"
                val = (f"${m_val.group(1)} {m_val.group(2) or ''}".strip() + " deemed value") if m_val else "deemed value unverified"
                completed.append({
                    "date": tx_date,
                    "title": "Business combination closing",
                    "shares_str": f"+{a_sh} Class A + {b_sh} Class B shares",
                    "terms": f"{val} merger consideration shares issued",
                    "badge": "Non-cash share issuance",
                    "cash_amt": 0.0,
                    "short_type": "Business combination closing"
                })
                risk_state()["dilution_mechanisms"].add("merger issuance")
            continue

        # 6. Check Warrant Inducement / Exercise (Item 3.02 & 1.01)
        if form == "8-K" and "3.02" in items and re.search(r'warrant exercise agreement|warrant inducement', combined, re.I):
            ex_m = re.search(r'exercise (?:certain\s+)?outstanding warrants to purchase (?:an aggregate of\s+)?([\d,]+)\s+shares', combined, re.I)
            red_m = re.search(r'reduced from \$([\d\.]+)(?:\s+per share)? to \$([\d\.]+)(?:\s+per share)?', combined, re.I)
            nw_m = re.search(r'new (?:unregistered\s+)?warrants to purchase (?:up to an aggregate of\s+)?([\d,]+)\s+shares', combined, re.I)
            nw_price_m = re.search(r'exercise price of \$([\d\.]+)', combined, re.I)
            
            tx_key = f"warrant_inducement_{tx_date:%Y%m%d}"
            if tx_key not in seen and ex_m:
                seen.add(tx_key)
                ex_shares = int(ex_m.group(1).replace(",", ""))
                red_price = red_m.group(2) if red_m else "unknown"
                gross = ex_shares * float(red_price) if red_price != "unknown" else 0.0
                
                m_ag = re.search(r'\b(Maxim Group LLC|H\.C\. Wainwright|Cantor Fitzgerald|B\. Riley|Roth Capital|ThinkEquity|A\.G\.P\.)\b', combined, re.I)
                agent_str = f" — {m_ag.group(1)}" if m_ag else ""
                
                completed.append({
                    "date": tx_date,
                    "title": f"Warrant inducement{agent_str}",
                    "shares_str": f"+{ex_shares:,} shares",
                    "terms": f"${gross/1e6:.3f}M gross proceeds · ${red_price}/share",
                    "badge": "Cash received",
                    "cash_amt": gross,
                    "short_type": "Warrant inducement"
                })
                if nw_m:
                    nw_cnt = int(nw_m.group(1).replace(",", ""))
                    nw_p = nw_price_m.group(1) if nw_price_m else red_price
                    potential_table.append({
                        "instrument": f"{tx_date:%b %Y} replacement warrants",
                        "shares": nw_cnt,
                        "price": f"${nw_p}"
                    })
                risk_state()["dilution_mechanisms"].add("warrant inducement")
            continue

        # 7. Check Debt Exchange / Note conversion (Item 3.02)
        if form == "8-K" and "3.02" in items and re.search(r'Exchange Agreement|exchanged? each of the|promissory note', combined, re.I):
            de_m = re.search(r'reduced by \$([\d\.,]+)[^\.]{0,80}?exchange[^\.]{0,80}?for an aggregate of ([\d,]+)\s+shares[^\.]{0,50}?Common Stock', combined, re.I)
            lender_m = re.search(r'with\s+([A-Za-z0-9\.,\s]+?)\s*(?:\([“"\'\s]*Lender|to whom)', combined, re.I)
            lender = lender_m.group(1).strip() if lender_m else "Debt holder"
            tx_key = f"debt_exchange_{tx_date:%Y%m%d}"
            if tx_key not in seen and de_m:
                seen.add(tx_key)
                debt_amt = de_m.group(1).rstrip(',')
                de_sh = int(de_m.group(2).replace(",", ""))
                completed.append({
                    "date": tx_date,
                    "title": lender,
                    "shares_str": f"+{de_sh:,} shares",
                    "terms": f"${debt_amt} debt exchanged for shares",
                    "badge": "Non-cash conversion",
                    "cash_amt": 0.0,
                    "short_type": "Debt-to-share conversion"
                })
                risk_state()["dilution_mechanisms"].add("convertible")
            continue

        # 8. Check Vote Postponements / Meeting Adjournments (Item 5.07 or 8.01 without 3.02 / 2.01)
        if form == "8-K" and re.search(r'\badjourn\w*\s+(?:the\s+)?(?:Annual|Special)?\s*Meeting\b|\bmore time to (?:consider and )?vote\b|\b(?:not\s+sufficient|lacked\s+sufficient)\s+votes\b', combined, re.I):
            w_desc = "replacement warrants"
            administrative.append({
                "date": tx_date,
                "type": "vote_postponement",
                "w_desc": w_desc
            })
            continue

        # 9. Check Reverse Split / Charter Amendment without share issuance (Item 3.03 / 5.03)
        if form == "8-K" and ('3.03' in items or '5.03' in items) and re.search(r'\breverse stock split\b|\bCertificate of Incorporation\b', combined, re.I) and not ('3.02' in items):
            split_m = re.search(r'reverse stock split[^\.]{0,100}?ratio (?:at|of)\s+([\d\-for]+)', combined, re.I)
            ratio_str = split_m.group(1) if split_m else "unknown ratio"
            administrative.append({
                "date": tx_date,
                "type": "reverse_split",
                "ratio": ratio_str
            })
            continue

        # 10. General Item 3.02 completed sales of equity securities
        if form == "8-K" and "3.02" in items:
            sh_m = re.search(r'(?:sold|issued|issuance of)\s+(?:an aggregate of\s+)?([\d,]+)\s+shares', combined, re.I)
            pr_m = re.search(r'(?:gross proceeds of\s+|price of\s+|purchase price of\s+)\$([\d\.,]+)\s*(million|thousand)?', combined, re.I)
            tx_key = f"general_302_{tx_date:%Y%m%d}"
            if tx_key not in seen and sh_m:
                seen.add(tx_key)
                shares = int(sh_m.group(1).replace(",", ""))
                proc_str = f" for ${pr_m.group(1)}{pr_m.group(2) or ''}" if pr_m else ""
                gross_amt = 0.0
                if pr_m:
                    val = float(pr_m.group(1).replace(",", ""))
                    mult = {"thousand": 1e3, "million": 1e6, "billion": 1e9}.get((pr_m.group(2) or "").lower(), 1e6 if val < 500 else 1.0)
                    gross_amt = val * mult
                completed.append({
                    "date": tx_date,
                    "title": "Private placement / offering",
                    "shares_str": f"+{shares:,} shares",
                    "terms": proc_str or "equity offering",
                    "badge": "Cash received" if gross_amt > 0 else "Shares issued",
                    "cash_amt": gross_amt,
                    "short_type": "Private placement"
                })
                risk_state()["dilution_mechanisms"].add("private placement")
            continue

        # 11. Check 10-K/10-Q Subsequent Events (SAFE / Convertible)
        if form in {'10-K', '10-Q'}:
            safe_m = re.search(r'on\s+([A-Za-z]+\s+\d+,\s+\d{4})[^\.]{0,80}?(?:sold and issued|issued|provided)[^\.]{0,40}?SAFE[^\.]{0,40}?\$\s?([\d\.]+)\s*(million|thousand)?', combined, re.I)
            if safe_m and 'safe_issuance' not in seen:
                seen.add('safe_issuance')
                s_date = pd.to_datetime(safe_m.group(1))
                s_amt = safe_m.group(2)
                potential_table.append({
                    "instrument": f"${s_amt}M convertible SAFE ({s_date:%b %Y})",
                    "shares": 0,
                    "price": "convertible"
                })
                risk_state()["dilution_mechanisms"].add("convertible")
            continue

    # Sort completed events chronologically descending
    completed.sort(key=lambda x: pd.Timestamp(x["date"]), reverse=True)
    potential_table.sort(key=lambda x: x["shares"], reverse=False)
    risk_state()["recent_dilution_filings"] = len(completed)

    # Build Dilution Dashboard
    out.append(f"{sym} — Dilution Dashboard")
    out.append("Last 12 months · Based on your supplied filing notes")
    out.append("")

    distinct_dates = sorted(set(pd.Timestamp(ev["date"]).normalize() for ev in completed))
    tot_cash = sum(ev.get("cash_amt", 0) for ev in completed)
    tot_pot = sum(p["shares"] for p in potential_table)
    cash_events = [ev for ev in completed if ev.get("cash_amt", 0) > 0]
    words = {1: "One", 2: "Two", 3: "Three", 4: "Four"}.get(len(cash_events), str(len(cash_events)))
    cash_sub = f"{words} warrant inducement{'s' if len(cash_events) != 1 else ''}" if cash_events else "Non-cash events only"

    out.append("Completed events")
    out.append(str(len(completed)))
    out.append(f"{len(distinct_dates)} distinct date{'s' if len(distinct_dates) != 1 else ''}")
    out.append("")
    out.append("")

    out.append("Gross cash raised")
    out.append(f"${tot_cash/1e6:.2f}M")
    out.append(cash_sub)
    out.append("")
    out.append("")

    out.append("Replacement warrants")
    out.append(f"{tot_pot/1e6:.2f}M" if tot_pot > 0 else "0.00M")
    out.append("Potential future shares")
    out.append("")
    out.append("")

    out.append("Latest event")
    if completed:
        out.append(f"{completed[0]['date']:%d %b %y}")
        out.append(completed[0]['short_type'])
    else:
        out.append("none")
        out.append("No completed dilution")
    out.append("")
    out.append("")
    out.append("")
    out.append("")

    # Section 1: Completed dilution
    out.append("1. Completed dilution — actual shares issued")
    if not completed:
        out.append("  None confirmed in the last 12 months")
        out.append("")
    else:
        for ev in completed:
            out.append(f"{ev['date']:%d %b %y}")
            out.append("")
            out.append(ev['title'])
            out.append(ev['shares_str'])
            out.append(ev['terms'])
            out.append(ev['badge'])
            out.append("")
            out.append("")
            out.append("")
            out.append("")

    out.append("")
    # Section 2: Remaining potential dilution
    out.append("2. Remaining potential dilution")
    warrant_items = [p for p in potential_table if p.get("shares", 0) > 0]
    if warrant_items:
        out.append(f"{'Instrument':<34} {'Shares':<12} {'Exercise price'}")
        for p in warrant_items:
            out.append(f"{p['instrument']:<34} {p['shares']:<12,} {p['price']}")
        out.append(f"{'Total':<34} {tot_pot:<12,} —")
        out.append("")
        tot_str = f"{tot_pot/1e6:.2f}M" if tot_pot >= 1e6 else f"{tot_pot:,}"
        out.append(f"These warrants are not new shares yet. Verify the current outstanding balance, expiration dates, and any exercise or redemption activity before treating all {tot_str} as live.")
    else:
        out.append("None reported from recent financing transactions.")
    out.append("")

    # Section 3: Administrative / watchlist events
    out.append("3. Administrative / watchlist events")
    vote_seen = 0
    vote_lines, split_lines, reg_lines, comm_lines = [], [], [], []
    administrative.sort(key=lambda x: pd.Timestamp(x["date"]), reverse=True)
    for a in administrative:
        if a.get("type") == "vote_postponement":
            vote_seen += 1
            if vote_seen == 1:
                vote_lines.append(f"- {a['date']:%d %b %Y}: Stockholder vote postponed/adjourned for the {a.get('w_desc', 'replacement warrants')}.")
            else:
                vote_lines.append(f"- {a['date']:%d %b %Y}: Earlier postponement of the same proposal.")
        elif a.get("type") == "reverse_split":
            split_lines.append(f"- {a['date']:%d %b %Y}: {a.get('ratio', 'unknown ratio')} reverse split; capital structure adjustment, not a new issuance.")
        elif a.get("type") == "registration":
            reg_lines.append(f"- {a['date']:%d %b %Y}: {a['form']} registration filing; registration does not itself mean shares were sold.")
        elif a.get("type") == "commercial":
            comm_lines.append(f"- {a['date']:%d %b %Y}: Commercial definitive agreement (Item 1.01); operational agreement, no equity issued.")


    admin_lines = vote_lines + split_lines + reg_lines + comm_lines
    seen_al = set()
    for al in admin_lines:
        if al not in seen_al:
            seen_al.add(al)
            out.append(al)

    # Post-cutoff financing reconciliation using financing.py
    risk_state()["post_report_financing"] = 0.0
    risk_state()["post_report_gross_financing"] = 0.0
    if latest_cash:
        cutoff_date = latest_cash[1]
        fin_events = []
        fin_filings = f[(f.filingDate >= cutoff_date) & f.form.isin({"8-K", "8-K/A", "6-K", "6-K/A", "10-K", "10-Q"})]
        for r_fin in fin_filings.itertuples():
            for _, t_fin in filing_text(cik, r_fin):
                fin_events.extend(fin.extract_events(t_fin, r_fin.form, r_fin.filingDate, label=f"{r_fin.form} {r_fin.filingDate:%d %b %y}"))
        merged_fin = fin.merge_events(fin_events)
        rec_fin = fin.reconcile(merged_fin, cutoff_date)
        risk_state()["post_report_gross_financing"] = rec_fin["gross_verified"]
        risk_state()["post_report_financing"] = rec_fin["net_verified"]

        burn = risk_state().get("monthly_burn")
        elapsed_months = max(0.0, (pd.Timestamp.now() - latest_cash[1]).days / 30.4375)
        financing = risk_state()["post_report_financing"]
        gross_financing = risk_state()["post_report_gross_financing"]
        burned = burn * elapsed_months if burn is not None else None
        monthly_burn = burn
        if burned is not None and monthly_burn is not None:
            current_cash = max(0.0, latest_cash[0] + financing - burned)
            risk_state()["current_cash"] = current_cash
            risk_state()["runway"] = current_cash / monthly_burn if monthly_burn else None
            out.append("")
            out.append(f"Estimated cash today: ${current_cash / 1e6:.3f}M "
                       f"(latest cash ${latest_cash[0] / 1e6:.3f}M"
                       f" + verified net financing ${financing / 1e6:.3f}M"
                       f" - estimated burn since report ${burned / 1e6:.3f}M)")
            out.append(f"  Verified gross post-report proceeds: ${gross_financing / 1e6:.3f}M; "
                       f"net verified financing ${financing / 1e6:.3f}M")
            out.append(f"  Estimated post-report burn: ${burned / 1e6:.3f}M "
                       f"({elapsed_months:.1f} months × reported "
                       f"${monthly_burn / 1e6:.3f}M/month)")
            out.append("Post-report financing reconciled against filing dates, closing dates, and non-cash exclusions.")
            out.append(f"Estimated cash runway: {risk_state()['runway']:.1f} months"
                       if risk_state()["runway"] is not None
                       else "Estimated cash runway: unavailable")
    log("scoring dilution risk", "sec")
    out += dilution_risk_section()
    return out, cik


# --------------------------------------------------------------------------
# Intraday bars: Polygon/Massive (one call per day, cached) -> Yahoo fallback
# --------------------------------------------------------------------------
_poly_limit = RateLimiter(1.0 / POLYGON_MIN_INTERVAL) if POLYGON_MIN_INTERVAL > 0 else None


def _polygon_full_day(sym, day):
    """Premarket + regular session minute bars (04:00-15:59 ET) in ONE API call."""
    if not MASSIVE_KEY:
        return None
    os.makedirs(CACHE, exist_ok=True)
    fp = os.path.join(CACHE, f"{sym}_{day}_full.pkl")
    if os.path.exists(fp):
        try:
            log(f"{sym} {day}: bars from disk cache", "polygon")
            return pd.read_pickle(fp)
        except Exception as e:
            log(f"{sym} {day}: incompatible or corrupt cache ({e}), re-fetching", "polygon")
            try:
                os.remove(fp)
            except OSError:
                pass
    r = None
    try:
        for attempt in range(3):
            if _poly_limit:
                _poly_limit.wait()
            log(f"{sym} {day}: calling API (attempt {attempt + 1}/3)", "polygon")
            r = requests.get(
                f"https://api.polygon.io/v2/aggs/ticker/{sym}/range/1/minute/{day}/{day}",
                params={"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": MASSIVE_KEY},
                timeout=30)
            if r.status_code != 429:
                break
            log(f"{sym} {day}: rate limited (429), retrying", "polygon")
    except requests.RequestException as e:
        log(f"{sym} {day}: request failed: {e}", "polygon")
        return None
    if r is not None and r.status_code == 200 and r.json().get("results"):
        d = pd.DataFrame(r.json()["results"])
        d.index = pd.DatetimeIndex(pd.to_datetime(d["t"], unit="ms", utc=True)).tz_convert("America/New_York")
        d = d.rename(columns={
            "h": "High", "l": "Low", "c": "Close", "v": "Volume"
        }).between_time("04:00", "15:59")
        if len(d):
            d.to_pickle(fp)  # past days never change, so cache forever
            log(f"{sym} {day}: got {len(d)} minute bars (cached to disk)", "polygon")
            return d
    log(f"{sym} {day}: no data (HTTP {r.status_code if r is not None else 'n/a'}) - using Yahoo fallback", "polygon")
    return None


def get_day(sym, day, yahoo_intra, start="09:30", end="15:59"):
    """Minute bars for one date between start/end (ET).
    Massive/Polygon first (consolidated data, ~2y history), Yahoo 5m as fallback."""
    full = _polygon_full_day(sym, day)
    if full is not None:
        return full.between_time(start, end)
    if not yahoo_intra.empty:
        return yahoo_intra[yahoo_intra.index.date == day].between_time(start, end)
    return pd.DataFrame()


def price_section(sym):
    t = yf.Ticker(sym)
    out = []
    risk_state().clear()

    # All Yahoo calls run in parallel (they were sequential before).
    log("fetching info, 1y daily, 60d 5m, splits, news in parallel", "yahoo")
    t_yahoo = time.time()
    with ThreadPoolExecutor(6) as ex:
        f_info = ex.submit(lambda: t.info or {})
        f_hist = ex.submit(t.history, period="1y", interval="1d", auto_adjust=False)
        f_intra = ex.submit(t.history, period="60d", interval="5m", auto_adjust=False)
        f_splits = ex.submit(lambda: t.splits)
        f_news = ex.submit(news_tags, sym)
        try:
            info = f_info.result()
        except Exception:
            info = {}
        f_cash = ex.submit(cash_burn_metrics, t, info)
        sp = f_splits.result()
        df = f_hist.result()
        try:
            intra = f_intra.result()
            intra.index = pd.DatetimeIndex(intra.index).tz_convert("America/New_York")
        except Exception:
            intra = pd.DataFrame()
        news_lines = f_news.result()
        cash_metrics = f_cash.result()

    log(f"all Yahoo data received ({time.time() - t_yahoo:.1f}s)", "yahoo")
    fl, so = info.get("floatShares"), info.get("sharesOutstanding")
    (reported_cash, current_cash, monthly_burn, runway, _,
     _, burn_period) = cash_metrics
    risk_state()["reported_cash"] = reported_cash
    risk_state()["current_cash"] = current_cash
    risk_state()["monthly_burn"] = monthly_burn
    risk_state()["runway"] = runway
    risk_state()["burn_period"] = burn_period
    risk_state()["shares_out"] = so
    risk_state()["shares_source"] = "Yahoo"
    curr_px = info.get("currentPrice") or info.get("regularMarketPrice")
    if not curr_px and not df.empty and "Close" in df:
        curr_px = float(df["Close"].iloc[-1])
    risk_state()["last_price"] = curr_px
    out.append(f"Float {fl/1e6:.1f}M" if fl else "Float n/a")
    if so:
        out.append(f"Shares out {so/1e6:.1f}M")
    out.append("")

    out.append("Reverse splits:")
    rs = [(d, r) for d, r in sp.items() if r < 1]
    risk_state()["reverse_splits"] = len(rs)
    for d, r in reversed(rs):
        out.append(f"R/S {round(1/r)}:1    {d:%d %b %y}")
    if not rs:
        out.append("  none")
    out.append("")

    if df.empty:
        return out + ["No price data"]
    df["prev"] = df["Close"].shift(1)
    df["gap"] = df["Open"] / df["prev"] - 1
    df["hod"] = df["High"] / df["Open"] - 1
    df["cls"] = df["Close"] / df["Open"] - 1
    split_dates = [pd.Timestamp(str(d)).date() for d, _ in rs]
    split_affected = df.index.to_series().apply(
        lambda value: any(abs((pd.Timestamp(str(value)).date() - split_date).days) <= 2
                          for split_date in split_dates))

    gaps = df[(df.gap >= GAP_MIN) & ~split_affected].tail(5).iloc[::-1]
    excluded_splits = int((split_affected & (df.gap >= GAP_MIN)).sum())

    log(f"{len(df)} daily bars; {len(gaps)} gap-up day(s) >= {GAP_MIN:.0%} to analyze", "yahoo")
    if MASSIVE_KEY and len(gaps):
        with step(f"loading minute bars for {len(gaps)} gap day(s)", "polygon"):
            with ThreadPoolExecutor(5) as ex:
                list(ex.map(lambda d: _polygon_full_day(sym, pd.Timestamp(str(d)).date()),
                            gaps.index))
    elif len(gaps):
        log("no Polygon key set - using Yahoo 5m bars (only last 60 days available)", "polygon")

    out.append(f"Gap History (last 5 gap ups >= {GAP_MIN*100:.0f}%; "
               "regular-session HOD)")
    if excluded_splits:
        out.append(f"  Excluded {excluded_splits} gap(s) within 2 sessions of a reverse split")
    times, faded = [], 0
    for d, r in gaps.iterrows():
        tm, vw = "unavailable", "unavailable"
        day = get_day(sym, pd.Timestamp(str(d)).date(), intra)
        if len(day):
            tm_ts = pd.Timestamp(str(day["High"].idxmax()))
            ist_timestamp = tm_ts.tz_convert(DISPLAY_TIMEZONE)
            tm = ist_timestamp.strftime("%H:%M")
            times.append(ist_timestamp.hour * 60 + ist_timestamp.minute)
            vwap = (day["Close"] * day["Volume"]).sum() / max(day["Volume"].sum(), 1)
            vw = "above VWAP" if day["Close"].iloc[-1] > vwap else "below VWAP"
        faded += r.cls < 0
        out.append(f"{d:%d %b %y} {pct(r.gap):>6} HOD {pct(r.hod)} @ {tm:<11} "
                   f"close {pct(r.cls)} vs open  {vw}")
    out += ["", "Premarket spike and fade (times shown in IST; informational only)",
            f"  Qualified only when premarket dollar turnover is at least "
            f"${PREMARKET_MIN_TURNOVER / 1e6:.1f}M"]
    premarket_found = 0
    for d, r in gaps.iterrows():
        premarket = get_day(sym, pd.Timestamp(str(d)).date(), intra, "04:00", "09:29")
        if premarket.empty or "Volume" not in premarket or "Close" not in premarket:
            continue
        turnover = float((premarket["Close"] * premarket["Volume"]).sum())
        if turnover < PREMARKET_MIN_TURNOVER:
            continue
        pm_high = float(premarket["High"].max())
        pm_low = float(premarket["Low"].min())
        open_price = float(r["Open"])
        move = pm_high - pm_low
        fade = max(0.0, min(1.0, (pm_high - open_price) / move)) if move > 0 else 0.0
        high_time = ist_time(premarket["High"].idxmax())
        premarket_found += 1
        out.append(f"  {d:%d %b %y} high {pm_high:.4g} at {high_time} IST; low {pm_low:.4g}; "
                   f"session open price {open_price:.4g}; high-to-open fade {fade:.0%}; "
                   f"turnover ${turnover / 1e6:.2f}M")
    if not premarket_found:
        out.append("  no qualifying premarket session met the $2.0M turnover threshold")
    out.append("")
    if len(gaps):
        risk_state()["fade_ratio"] = faded / len(gaps)
        risk_state()["gap_count"] = len(gaps)
        risk_state()["average_gap_close"] = float(gaps.cls.mean())
        hods = gaps.hod.tolist()
        line = f"Faded {faded} of {len(gaps)} - HOD median {pct(st.median(hods))} (avg {pct(st.mean(hods))})"
        if times:
            m = int(st.median(times))
            line += f" - median time {m//60:02d}:{m%60:02d} IST"
        out.append(line)
    else:
        risk_state()["fade_ratio"] = None
        risk_state()["gap_count"] = 0
        risk_state()["average_gap_close"] = None
        out.append("  no gap ups in last 12 months")
    out.append("")

    out.append("Open -> HOD (daily bars)")

    def row(label, s):
        if len(s):
            out.append(f"{label:<22} median {pct(s.median())}  avg {pct(s.mean())}  ({len(s)} days)")
    row("Last 30 sessions", df.hod.tail(30))
    row("Last 90 sessions", df.hod.tail(90))
    row("Smaller gap days >=5%", df[df.gap >= 0.05].hod)
    row("High-volume days", df[df.Volume >= df.Volume.quantile(0.8)].hod)

    out += ["", "Recent news (auto-tagged catalyst):"] + news_lines

    # Plain-English summary placed at the top
    read = ["== BEHAVIOR READ =="]
    if len(gaps):
        avg_close = gaps.cls.mean()
        read.append(f"- Gaps up {len(df[(df.gap >= GAP_MIN) & ~split_affected])}x in 12m; "
                    f"faded below open on {faded} of last {len(gaps)}.")
        read.append(f"- Typical spike: +{gaps.hod.median()*100:.0f}% above open, avg close {pct(avg_close)} vs open.")
        if times:
            m = int(st.median(times))
            read.append(f"- HOD usually in by {m//60:02d}:{m%60:02d} IST -> "
                        + ("fades after the morning spike." if avg_close < 0 else "tends to hold gains."))
    else:
        read.append("- No big gap-ups in 12m; normal-day open->HOD median "
                    f"{pct(df.hod.tail(90).median())}.")
    if rs:
        read.append(f"- {len(rs)} reverse split(s) on record, latest {rs[-1][0]:%b %Y}: history of dilute-then-R/S.")
    out = read + [""] + out
    return out


def build_sources(cik, f, max_sources=25):
    """Package recent dilution-relevant filing text as numbered sources for Gemini."""
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=DILUTION_LOOKBACK_DAYS)
    rows = f[(f.filingDate >= cutoff) & f.form.isin(DILUTION_FORMS)].sort_values(
        "filingDate", ascending=False)
    sources, i = {}, 1
    for r in rows.itertuples():
        for url, text in filing_text(cik, r):
            w = gn.windows(text, DIL_KEYS)
            if len(w) > 200 and i <= max_sources:
                sources[f"S{i}"] = {"form": r.form, "date": f"{r.filingDate:%Y-%m-%d}",
                                    "url": url, "text": w}
                i += 1
    return sources


def ai_rewrite(sym, text):
    """Gemini-written version of the notes; falls back to the original text on any error."""
    try:
        cik = get_cik(sym)
        if not cik:
            return text + "\n\n[AI notes skipped: ticker not on EDGAR]"
        f = complete_filing_index(cik)
        if f.empty:
            return text + "\n\n[AI notes skipped: SEC filing index unavailable]"
        with step(f"{sym}: Gemini analyst + auditor + writer", "gemini"):
            return gn.ai_notes(sym, build_sources(cik, f), py_facts=text, template=text)
    except Exception as e:
        traceback.print_exc()
        return text + f"\n\n[AI notes failed: {e}]"


@app.route("/api/notes")
def notes():
    sym = request.args.get("symbol", "").strip().upper()
    if not sym.isalnum():
        return jsonify(error="Bad symbol"), 400
    ai = bool(request.args.get("ai"))
    ckey = f"{sym}:ai" if ai else sym
    hit = _result_cache.get(ckey)
    if hit and time.time() - hit[0] < RESULT_TTL and not request.args.get("refresh"):
        log(f"{sym}: served from {RESULT_TTL}s result cache (press Refresh to recompute)", "app")
        return jsonify(text=hit[1])
    t0 = time.time()
    log(f"===== {sym}: new request =====", "app")
    try:
        # SEC downloads start in the background while Yahoo/Polygon run in the foreground.
        with ThreadPoolExecutor(1) as bg:
            log(f"{sym}: starting SEC prefetch in background", "app")
            warm = bg.submit(prefetch_sec, sym)
            with step(f"{sym}: price / gap section (Yahoo + Polygon)", "app"):
                price = price_section(sym)
            log(f"{sym}: waiting for SEC prefetch to finish", "app")
            warm.result()
        with step(f"{sym}: SEC section", "app"):
            sec, _ = sec_section(sym)  # now mostly cache hits
        text = "\n".join([f"Notes - {sym}", ""] + price + [""] + sec)
        _result_cache[sym] = (time.time(), text)
        if ai:
            text = ai_rewrite(sym, text)
            _result_cache[ckey] = (time.time(), text)
        log(f"===== {sym}: finished in {time.time() - t0:.1f}s =====", "app")
        return jsonify(text=text)
    except Exception as e:
        log(f"{sym}: ERROR after {time.time() - t0:.1f}s: {e}", "app")
        traceback.print_exc()
        return jsonify(error=str(e)), 500


PAGE = """<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Dilution Notes – SEC Filings, Cash & Dilution Analysis</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    :root {
      --bg: #000;
      --card-bg: #08080a;
      --toolbar-bg: #0d0d10;
      --border: #262626;
      --purple: #a855f7;
      --purple-glow: rgba(168, 85, 247, 0.15);
      --purple-hover: #c084fc;
      --cyan: #29a8ff;
      --green: #26d974;
      --gold: #ffd23f;
      --red: #ff4d4d;
      --text: #e8e8e8;
      --muted: #8a8a8a;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font: 14px/1.5 Consolas, "SF Mono", Menlo, monospace;
      min-height: 100vh;
      display: flex;
      flex-direction: column;
    }
    .bar {
      display: flex;
      gap: 10px;
      padding: 16px 30px;
      flex-wrap: wrap;
      align-items: center;
      background: var(--toolbar-bg);
      border-bottom: 1px solid var(--border);
      position: sticky;
      top: 0;
      z-index: 100;
    }
    .brand {
      font-size: 15px;
      font-weight: bold;
      color: var(--purple-hover);
      display: flex;
      align-items: center;
      gap: 6px;
      margin-right: 8px;
    }
    input[type=text] {
      background: #000;
      border: 1px solid var(--border);
      color: #fff;
      font: inherit;
      font-size: 18px;
      font-weight: bold;
      padding: 0 14px;
      height: 42px;
      width: 140px;
      text-transform: uppercase;
      border-radius: 3px;
      outline: none;
      transition: border-color 0.15s, box-shadow 0.15s;
    }
    input[type=text]:focus {
      border-color: var(--purple);
      box-shadow: 0 0 0 2px var(--purple-glow);
    }
    button {
      background: #141416;
      border: 1px solid var(--border);
      color: #fff;
      font: inherit;
      font-size: 14px;
      padding: 0 16px;
      height: 42px;
      cursor: pointer;
      border-radius: 3px;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      font-weight: 500;
      transition: all 0.15s;
    }
    button:hover {
      background: #222228;
      border-color: #555;
    }
    .btn-primary {
      background: #240038;
      border-color: var(--purple);
      color: var(--purple-hover);
      font-weight: bold;
    }
    .btn-primary:hover {
      background: #360054;
      border-color: #d8b4fe;
      color: #fff;
    }
    .btn-ai {
      background: #001a33;
      border-color: var(--cyan);
      color: var(--cyan);
      font-weight: bold;
    }
    .btn-ai:hover {
      background: #002b55;
      border-color: #6cc4ff;
      color: #fff;
    }
    .status {
      color: var(--muted);
      font-size: 13px;
      margin-left: 6px;
    }
    .back-btn {
      margin-left: auto;
      border-color: var(--cyan);
      color: var(--cyan);
      font-size: 13px;
      height: 40px;
      padding: 0 16px;
    }
    .back-btn:hover {
      background: #001a33;
    }
    .main-wrap {
      padding: 24px 30px 40px;
      max-width: 1200px;
      width: 100%;
      margin: 0 auto;
      flex: 1;
    }
    #o {
      border: 1px solid var(--border);
      padding: 24px 28px;
      background: var(--card-bg);
      border-radius: 4px;
      white-space: pre-wrap;
      word-break: break-word;
      line-height: 1.6;
      font-size: 13.5px;
      color: #ddd;
      min-height: 280px;
      overflow-x: auto;
    }
    .empty-state {
      color: var(--muted);
      font-size: 14px;
      line-height: 1.8;
    }
    .empty-state b {
      color: var(--purple-hover);
    }
    .loading-spin {
      display: inline-block;
      width: 14px;
      height: 14px;
      border: 2px solid rgba(255,255,255,0.2);
      border-top-color: var(--purple-hover);
      border-radius: 50%;
      animation: spin 0.8s linear infinite;
      margin-right: 8px;
      vertical-align: middle;
    }
    @keyframes spin {
      to { transform: rotate(360deg); }
    }
  </style>
</head>
<body>
  <div class="bar">
    <div class="brand">📑 Dilution Notes</div>
    <input id="s" type="text" placeholder="TICKER" autofocus autocomplete="off" spellcheck="false">
    <button class="btn-primary" id="go_btn" onclick="go()">Get notes</button>
    <button id="rf_btn" onclick="go(true)">Refresh</button>
    <button class="btn-ai" id="ai_btn" onclick="go('ai')">✨ AI notes</button>
    <button id="copy" onclick="copyNotes()">Copy all</button>
    <span id="status" class="status"></span>
    <button class="back-btn" id="backbtn">← Back to Chart Stats</button>
  </div>
  <div class="main-wrap">
    <pre id="o"><div class="empty-state">Enter a stock ticker above (e.g. <b>BKYI</b>, <b>AAPL</b>, <b>NVDA</b>) and click <b>Get notes</b>.<br><br>• Real-time SEC EDGAR Dilution & Offering History<br>• Cash Balance, Burn Rate & Estimated Runway<br>• Warrant Overhang & Exercise Prices<br>• 10-K, 10-Q, 8-K, S-1, S-3 & 424B Prospectus Breakdown<br>• Reverse Splits & Intraday Gap Stats</div></pre>
  </div>
  <script>
    const s = document.getElementById('s');
    const o = document.getElementById('o');
    const status = document.getElementById('status');
    const copyBtn = document.getElementById('copy');

    s.addEventListener('keydown', e => { if (e.key === 'Enter') go(); });

    document.getElementById('backbtn').onclick = () => {
      if (window.opener && !window.opener.closed) {
        window.close();
      } else if (document.referrer && document.referrer !== window.location.href) {
        window.location.href = document.referrer;
      } else {
        window.location.href = '/';
      }
    };

    // Auto-load symbol if passed in query string (?symbol=XYZ or ?ticker=XYZ)
    const urlParams = new URLSearchParams(window.location.search);
    const initialSym = (urlParams.get('symbol') || urlParams.get('ticker') || '').trim().toUpperCase();
    if (initialSym) {
      s.value = initialSym;
      setTimeout(() => go(), 50);
    }

    async function go(refresh) {
      const sym = (s.value || '').trim().toUpperCase();
      if (!sym) {
        o.innerHTML = '<span style="color:#ff5a5a">Please enter a valid stock symbol.</span>';
        return;
      }
      const modeLabel = refresh === 'ai' ? 'Running Gemini AI dilution analysis' : (refresh ? 'Refreshing SEC & Market stats' : 'Gathering SEC filings & dilution data');
      o.innerHTML = `<span style="color:#8a8a8a"><span class="loading-spin"></span>${modeLabel} for <b style="color:#fff">${sym}</b>…</span>`;
      status.textContent = '';
      const t0 = Date.now();
      try {
        const url = '/api/notes?symbol=' + encodeURIComponent(sym) +
                    (refresh === true ? '&refresh=1' : '') +
                    (refresh === 'ai' ? '&ai=1' : '');
        const r = await fetch(url);
        const j = await r.json();
        const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
        if (j.error) {
          o.innerHTML = `<span style="color:#ff4d4d">Error: ${escapeHtml(j.error)}</span>`;
          status.textContent = `Failed after ${elapsed}s`;
        } else {
          o.textContent = j.text || 'No data returned.';
          status.textContent = `Completed in ${elapsed}s`;
        }
      } catch (err) {
        o.innerHTML = `<span style="color:#ff4d4d">Network or server error: ${escapeHtml(err.message || String(err))}</span>`;
        status.textContent = 'Error';
      }
    }

    function escapeHtml(str) {
      return (str || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }

    async function copyNotes() {
      const text = o.textContent;
      if (!text || text.includes('Enter a stock ticker above') || text.includes('Gathering SEC filings')) {
        status.textContent = 'Nothing to copy';
        return;
      }
      try {
        await navigator.clipboard.writeText(text);
        copyBtn.textContent = 'Copied ✓';
        status.textContent = 'Report copied to clipboard!';
        setTimeout(() => { copyBtn.textContent = 'Copy all'; }, 1500);
      } catch (e) {
        const area = document.createElement('textarea');
        area.value = text;
        area.style.position = 'fixed';
        area.style.opacity = '0';
        document.body.appendChild(area);
        area.select();
        const ok = document.execCommand('copy');
        area.remove();
        copyBtn.textContent = ok ? 'Copied ✓' : 'Copy failed';
        setTimeout(() => { copyBtn.textContent = 'Copy all'; }, 1500);
      }
    }
  </script>
</body>
</html>"""


@app.route("/")
@app.route("/dilution")
@app.route("/notes")
def index():
    return PAGE


def main():
    log("Dilution Notes starting -> http://localhost:5000", "app")
    log(f"Polygon/Massive key: {'set' if MASSIVE_KEY else 'NOT set (Yahoo 5m fallback)'}; "
        f"min interval {POLYGON_MIN_INTERVAL}s; cache dir {CACHE}", "app")
    log(f"SEC lookback {SEC_LOOKBACK_YEARS}y; result cache {RESULT_TTL}s", "app")
    app.run(debug=os.environ.get("FLASK_DEBUG") == "1", threaded=True)


if __name__ == "__main__":
    main()