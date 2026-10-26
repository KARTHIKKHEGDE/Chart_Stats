"""finviz_dilution.py - Finviz Dilution Page Scraper & Dilution Risk Analyzer

Fetches and extracts structured dilution data from Finviz's dedicated dilution tab:
    https://finviz.com/stock?b=2&p=m&t={TICKER}&ta=1&ty=di

Extracts:
  - Overall Risk Dashboard (Finviz risk score, band, drivers: offering ability, cash need, overhang, dilution history)
  - Financial Position (Reported cash, cash vs investments, operating cash burn, runway, shares, baby shelf status)
  - Open Dilution Instruments (Shelf capacity & raisable amount, ATM programs, ELOC lines, Warrants, Convertible Notes, Preferreds)
  - Today's Selling Risk Synthesis (Confirmed active vs historical/inactive mechanisms, baby-shelf caps, implied shares)
  - Clear provenance and status tags: [VERIFIED], [FINVIZ REPORTED], [ESTIMATED], [UNVERIFIED], [NOT FOUND]
"""
import json
import os
import re
import time
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

try:
    from curl_cffi import requests as _curl_requests
except ImportError:
    _curl_requests = None

CACHE_DIR = Path(__file__).resolve().parent / ".cache" / "finviz"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_TTL = 3600  # 1 hour cache for Finviz pages (bypassable via refresh=True)

_session = requests.Session()
_session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://finviz.com/",
    "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
})


def _fetch_html(url: str) -> Tuple[int, str]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://finviz.com/",
        "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    }
    # 1. Try curl_cffi first with Chrome TLS fingerprint impersonation (bypasses Cloudflare JA3 block on Render/AWS)
    if _curl_requests is not None:
        try:
            r = _curl_requests.get(url, headers=headers, impersonate="chrome124", timeout=20)
            return r.status_code, r.text
        except Exception:
            pass

    # 2. Fallback to requests Session
    r = _session.get(url, headers=headers, timeout=20)
    return r.status_code, r.text


def _fetch_rendered_html(url: str) -> Tuple[int, str]:
    """Fetch a rendered copy when Finviz blocks server-side requests."""
    proxy_url = "https://r.jina.ai/" + url
    headers = {"Accept": "text/html", "X-Return-Format": "html"}
    if _curl_requests is not None:
        try:
            r = _curl_requests.get(proxy_url, headers=headers, impersonate="chrome124", timeout=30)
            return r.status_code, r.text
        except Exception:
            pass
    r = requests.get(proxy_url, headers=headers, timeout=30)
    return r.status_code, r.text


class _VisibleText(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts: List[str] = []
        self.hidden = 0
        self.tables: List[Dict[str, Any]] = []
        self.current_heading = ""
        self._heading_parts: Optional[List[str]] = None
        self._table: Optional[Dict[str, Any]] = None
        self._row: Optional[List[str]] = None
        self._cell: Optional[List[str]] = None

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style", "noscript", "svg"):
            self.hidden += 1
        elif tag in ("h1", "h2", "h3"):
            self._heading_parts = []
        elif tag == "table":
            self._table = {"heading": self.current_heading, "rows": []}
        elif tag == "tr" and self._table is not None:
            self._row = []
        elif tag in ("td", "th") and self._row is not None:
            self._cell = []

    def handle_endtag(self, tag):
        if tag in ("script", "style", "noscript", "svg") and self.hidden:
            self.hidden -= 1
        elif tag in ("td", "th") and self._cell is not None:
            self._row.append(" ".join(self._cell).strip())
            self._cell = None
        elif tag == "tr" and self._row is not None:
            if any(self._row):
                self._table["rows"].append(self._row)
            self._row = None
        elif tag == "table" and self._table is not None:
            if self._table["rows"]:
                self.tables.append(self._table)
            self._table = None
        elif tag in ("h1", "h2", "h3") and self._heading_parts is not None:
            self.current_heading = " ".join(self._heading_parts).strip()
            self._heading_parts = None

    def handle_data(self, data):
        if not self.hidden and data.strip():
            value = data.strip()
            self.parts.append(value)
            if self._cell is not None:
                self._cell.append(value)
            if self._heading_parts is not None:
                self._heading_parts.append(value)


def _rendered_page_fallback(ticker: str, html: str, url: str) -> Dict[str, Any]:
    """Build a cautious summary from Finviz's server-rendered dilution page."""
    parser = _VisibleText()
    parser.feed(html)
    text = " ".join(parser.parts)
    # Jina can return Markdown when its HTML mode is unavailable.
    if not text:
        text = re.sub(r"[`*_#]+", " ", html)
        text = re.sub(r"\s+", " ", text).strip()
    if not re.search(rf"\b{re.escape(ticker)}\b", text, re.I) or not re.search(r"\bDilution\b", text, re.I):
        raise ValueError("Rendered Finviz response did not contain a recognizable dilution page")
    return {
        "_rendered_fallback": True,
        "_rendered_text": text,
        "_rendered_tables": parser.tables,
        "_source_url": url,
        "_fetched_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
    }


def _save_cache(cache_file: Path, data: Dict[str, Any]) -> None:
    try:
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception:
        pass


def _fmt_money(val: Optional[float], decimals: int = 2) -> str:
    if val is None:
        return "—"
    a = abs(val)
    sign = "-" if val < 0 else ""
    if a >= 1e9:
        return f"{sign}${a / 1e9:.{decimals}f}B"
    if a >= 1e6:
        return f"{sign}${a / 1e6:.{decimals}f}M"
    if a >= 1e3:
        return f"{sign}${a / 1e3:.{decimals}f}K"
    return f"{sign}${a:,.{decimals}f}"


def _fmt_shares(val: Optional[float], decimals: int = 2) -> str:
    if val is None:
        return "—"
    a = abs(val)
    sign = "-" if val < 0 else ""
    if a >= 1e9:
        return f"{sign}{a / 1e9:.{decimals}f}B"
    if a >= 1e6:
        return f"{sign}{a / 1e6:.{decimals}f}M"
    if a >= 1e3:
        return f"{sign}{a / 1e3:.1f}K"
    return f"{sign}{a:,.0f}"


def fetch_finviz_dilution_data(ticker: str, refresh: bool = False) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Fetch and parse the embedded JSON dilution model from Finviz.
    Returns (data_dict, error_message).
    """
    t = ticker.strip().upper()
    cache_file = CACHE_DIR / f"{t}_dilution.json"

    if not refresh and cache_file.exists():
        try:
            mtime = cache_file.stat().st_mtime
            if time.time() - mtime < CACHE_TTL:
                with open(cache_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    return data, None
        except Exception:
            pass

    url = f"https://finviz.com/stock?b=2&p=m&t={t}&ta=1&ty=di"
    try:
        status_code, html = _fetch_html(url)
        # Finviz commonly returns 403 to hosted/server requests, even for valid
        # tickers. Try its rendered public page through the reader before giving up.
        if status_code != 200:
            try:
                proxy_status, proxy_html = _fetch_rendered_html(url)
                if proxy_status == 200:
                    try:
                        m = re.search(r'(\{"schema_version":\s*1.*?</script>)', proxy_html, re.DOTALL)
                        if m:
                            raw_json = m.group(1).split("</script>", 1)[0]
                            data = json.loads(raw_json)
                            data["_source_url"] = url
                            data["_fetched_at"] = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
                            return data, None
                    except (ValueError, json.JSONDecodeError):
                        pass
                    data = _rendered_page_fallback(t, proxy_html, url)
                    data["ticker"] = t
                    _save_cache(cache_file, data)
                    return data, None
            except Exception:
                pass
            if status_code == 404:
                return None, f"Ticker {t} not found on Finviz (HTTP 404)"
            if status_code == 429:
                return None, "Finviz rate limit reached (HTTP 429). Please try again shortly."
            return None, f"Finviz returned HTTP {status_code}; rendered-page fallback unavailable"
        # Search for schema_version JSON payload
        m = re.search(r'(\{"schema_version":\s*1.*?\})</script>', html, re.DOTALL)
        if not m:
            # Fallback regex if closing script tag has whitespace or attributes
            m = re.search(r'(\{"schema_version":\s*1.*?"cards":\s*\{.*?\}.*?\})', html, re.DOTALL)

        if not m:
            try:
                data = _rendered_page_fallback(t, html, url)
                data["ticker"] = t
                _save_cache(cache_file, data)
                return data, None
            except ValueError:
                return None, f"Finviz page loaded for {t}, but no structured dilution data was present"

        raw_json = m.group(1)
        data = json.loads(raw_json)
        data["_fetched_at"] = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
        data["_source_url"] = url

        # Save to cache
        _save_cache(cache_file, data)

        return data, None

    except requests.Timeout:
        return None, "Finviz connection timed out"
    except requests.RequestException as e:
        return None, f"Finviz connection error: {e}"
    except Exception as e:
        return None, f"Finviz parsing error: {e}"


def build_finviz_dilution_report(data: Dict[str, Any], current_price: Optional[float] = None) -> List[str]:
    """
    Format the Finviz dilution model into the clean, compact report structure requested.
    """
    if data.get("_rendered_fallback"):
        text = data.get("_rendered_text", "")
        def value(label: str) -> Optional[str]:
            match = re.search(rf"\b{label}\b\s+([A-Za-z]+)(?:\s+(\d{{1,3}})\s*/\s*100)?", text, re.I)
            if not match:
                return None
            return f"{match.group(1)} ({match.group(2)}/100)" if match.group(2) else match.group(1)
        lines = ["DILUTION", f"Updated {data.get('_fetched_at', 'date unavailable')}", "", "DILUTION RISK"]
        risk = value("Dilution Risk")
        lines.append(f"- Dilution Risk: {risk}" if risk else "- Dilution Risk: unavailable")
        for label in ("Offering", "Cash Need", "Overhang", "History"):
            result = value(label)
            if result:
                lines.append(f"- {label}: {result}")
        lines.append("")
        lines.append("KEY METRICS")
        for label, pattern in (("Runway", r"Runway\s+([\d.]+mo)"), ("Cash", r"Cash\s+(\$?[\d.]+[KMB]?)"), ("Burn/Q", r"Burn/Q\s+(\$?[\d.]+[KMB]?)"), ("Shares O/S", r"Shares O/S\s+([\d.]+[KMB]?)"), ("Baby Shelf", r"Baby Shelf\s+(Yes|No)")):
            match = re.search(pattern, text, re.I)
            if match:
                lines.append(f"- {label}: {match.group(1)}")
        summary = re.search(r"The huge .+?(?:pressure\.|pressure$)", text, re.I)
        if summary:
            lines.extend(["", "SUMMARY", summary.group(0)])
        tables = data.get("_rendered_tables") or []
        if tables:
            lines.extend(["", "INSTRUMENTS"])
            for table in tables:
                lines.append(table.get("heading") or "Finviz table")
                for row in table.get("rows") or []:
                    lines.append(" | ".join(cell.strip() for cell in row if cell.strip()))
        else:
            lines.extend(["", "INSTRUMENTS", "Finviz's rendered response did not include parseable instrument tables."])
        lines.append(f"Source: {data.get('_source_url', 'https://finviz.com')} (updated {data.get('_fetched_at', 'date unavailable')})")
        return lines

    out: List[str] = []
    def cell(value: Any) -> str:
        if value is None or value == "":
            return "—"
        if isinstance(value, dict):
            return str(value.get("form") or value.get("accession_number") or "—")
        if isinstance(value, bool):
            return "Yes" if value else "No"
        if isinstance(value, float) and value.is_integer():
            return f"{int(value):,}"
        return str(value)

    def row(title: str, fields: List[Any]) -> str:
        return f"- {title}\t" + "\t".join(cell(value) for value in fields)

    def money_ui(value: Optional[float]) -> str:
        if value is None:
            return "—"
        return re.sub(r"\.0(?=[KMB])", "", _fmt_money(value, 1))

    def shares_ui(value: Optional[float]) -> str:
        return _fmt_shares(value, 1) if value is not None else "—"
    badges = data.get("badges") or {}
    overall = badges.get("overall") or {}
    drivers = {d.get("key"): d for d in (badges.get("drivers") or []) if isinstance(d, dict)}
    company = data.get("company") or {}
    cash = company.get("cash") or {}
    cards = data.get("cards") or {}
    brief = data.get("brief") or {}

    as_of = data.get("as_of") or "unknown"
    gen_at = (data.get("generated_at") or "")[:10]
    update_date = gen_at or as_of
    source_url = data.get("_source_url", "https://finviz.com")

    # --------------------------------------------------------------------------
    # 1. DILUTION RISK DASHBOARD
    # --------------------------------------------------------------------------
    out.extend(["DILUTION", f"Updated {update_date}", "", "DILUTION RISK"])
    score = overall.get("score")
    label = overall.get("label") or "Unrated"
    if score is not None:
        out.append(f"- Dilution Risk: {label} ({score}/100)")
    else:
        out.append(f"- Dilution Risk: {label}")

    oa = drivers.get("offering_ability")
    if oa:
        out.append(f"- Offering: {oa.get('band_text', '—')}")
    else:
        out.append("- Offering: Unavailable")

    cn = drivers.get("cash_need")
    if cn:
        out.append(f"- Cash Need: {cn.get('band_text', '—')}")
    else:
        out.append("- Cash Need: Unavailable")

    oh = drivers.get("overhang")
    if oh:
        out.append(f"- Overhang: {oh.get('band_text', '—')}")
    else:
        out.append("- Overhang: Unavailable")

    dh = drivers.get("dilution_history")
    if dh:
        out.append(f"- History: {dh.get('band_text', '—')}")

    out.append("")
    out.append("KEY METRICS")
    metrics = []
    runway_months = cash.get("months_of_cash")
    if runway_months is not None:
        metrics.append(f"Runway: {runway_months:.1f}mo")
    key_cash = cash.get("current_cash_est_usd") or cash.get("latest_reported_cash_usd")
    if key_cash is not None:
        metrics.append(f"Cash: {money_ui(key_cash)}")
    if cash.get("op_cf_quarterly_usd") is not None:
        metrics.append(f"Burn/Q: {money_ui(abs(cash['op_cf_quarterly_usd']))}")
    if company.get("shares_outstanding") is not None:
        metrics.append(f"Shares O/S: {_fmt_shares(company['shares_outstanding'])}")
    if company.get("is_baby_shelf_restricted") is not None:
        metrics.append(f"Baby Shelf: {'Yes' if company['is_baby_shelf_restricted'] else 'No'}")
    out.extend(f"- {metric}" for metric in metrics)

    summary_text = brief.get("summary")
    if summary_text:
        # Wrap summary cleanly
        clean_sum = summary_text.strip().replace("\n", " ")
        out.append(clean_sum)
    out.append("")

    # --------------------------------------------------------------------------
    # 2. FINANCIAL POSITION
    # --------------------------------------------------------------------------
    # Historical shares and fully diluted instrument mix, matching Finviz's chart.
    os_chart = company.get("os_chart") or {}
    os_bars = os_chart.get("bars") or []
    if os_bars:
        out.extend(["", "HISTORICAL O/S & POTENTIAL DILUTION"])
        price_basis = os_chart.get("price_basis") or current_price
        if price_basis:
            out.append(f"ATM, equity-line and S-1 capacity shown in shares at the latest chart price of ${price_basis:.2f}. Unused shelf capacity is not included.")
        out.append("Shares outstanding (chart series, in millions):")
        chart_points = []
        for bar in os_bars:
            date = bar.get("quarter_end") or "Period"
            shares = bar.get("shares")
            if shares is not None:
                # Older pre-split bars are stored in millions; recent bars are
                # absolute share counts. Normalize to millions for the chart.
                millions = shares if abs(shares) < 10_000 else shares / 1_000_000
                value_text = f"{millions / 1_000:.3g}K" if abs(millions) >= 1_000 else f"{millions:.3g}"
                chart_points.append(f"{date}: {value_text}")
        out.append("- " + " | ".join(chart_points))
        components = []
        warrant_shares = sum((w.get("remaining_outstanding") or 0) for w in (cards.get("warrant") or []))
        convert_shares = sum((c.get("remaining_shares_issuable") or 0) for c in (cards.get("convertible") or []))
        preferred_shares = sum((p.get("remaining_shares_issuable") or 0) for p in (cards.get("preferred") or []))
        line_capacity = sum((e.get("remaining_capacity") or 0) for e in (cards.get("equity_line") or []) if e.get("terminated") is not True)
        if warrant_shares:
            components.append(f"Warrants {warrant_shares:.1f}")
        if convert_shares:
            components.append(f"Convertible Notes {shares_ui(convert_shares)}")
        if preferred_shares:
            components.append(f"Convertible Preferred {shares_ui(preferred_shares)}")
        if line_capacity and price_basis:
            components.append(f"Equity Line {shares_ui(line_capacity / price_basis)}")
        if components:
            out.append("Fully Diluted: " + " | ".join(components))
        out.append("Chart scale: 0 | 800 | 1.6K | 2.4K | 3.2K million shares")

    out.extend(["", "CASH POSITION"])
    rep_cash = cash.get("latest_reported_cash_usd")
    cash_only = cash.get("latest_reported_cash_only_usd")
    investments = cash.get("latest_reported_investments_usd")
    cash_period = cash.get("latest_period_end") or "latest period"
    op_cf_q = cash.get("op_cf_quarterly_usd")

    if rep_cash is not None:
        parts = []
        if cash_only is not None:
            parts.append(f"{_fmt_money(cash_only)} cash")
        if investments and investments > 0:
            parts.append(f"{_fmt_money(investments)} short-term investments")
        breakdown = f" ({' + '.join(parts)})" if parts else ""
        current_cash = cash.get("current_cash_est_usd")
        if runway_months is not None and current_cash is not None and op_cf_q is not None:
            out.append(f"About {runway_months:.1f} months of runway: estimated cash of {_fmt_money(current_cash)} against operating burn of {_fmt_money(abs(op_cf_q))} per quarter.")
        out.append(f"Reported balance as of {cash_period}: {_fmt_money(rep_cash)}{breakdown}.")
    else:
        out.append("- Reported cash: [UNVERIFIED]")

    # Cash Burn rate
    if op_cf_q is not None:
        monthly_burn = abs(op_cf_q) / 3.0
        out.append(f"- Monthly burn: ~{_fmt_money(monthly_burn)}/month (quarterly operating cash flow: {_fmt_money(op_cf_q)})")
    else:
        out.append("- Monthly burn: [UNVERIFIED]")

    # Runway
    if runway_months is not None:
        out.append(f"- Estimated runway: {runway_months:.1f} months")
    else:
        out.append("- Estimated runway: [UNVERIFIED]")

    # Shares Outstanding & Float
    so = company.get("shares_outstanding")
    fl = company.get("float_shares")
    os_latest = (company.get("os_chart") or {}).get("latest") or {}
    so_src = os_latest.get("source") or "Finviz company data"

    if so is not None:
        fl_str = f" · Free Float: {_fmt_shares(fl)}" if fl is not None else ""
        out.append(f"- Shares outstanding: {_fmt_shares(so)}{fl_str} ({so_src})")
    else:
        out.append("- Shares outstanding: [UNVERIFIED]")

    # Baby Shelf restrictions
    is_baby = company.get("is_baby_shelf_restricted")
    if is_baby is True:
        hi_close = company.get("highest_60_day_close")
        p_exceed = company.get("price_to_exceed_baby_shelf")
        p_str = f" · stock needs ${p_exceed:.2f} (60d high ${hi_close:.2f}) to exit cap" if p_exceed else ""
        out.append(f"- Baby shelf restriction: YES (capped under SEC Rule 415/I.B.6 to 1/3 of public float){p_str} [VERIFIED]")
    elif is_baby is False:
        out.append("- Baby shelf restriction: NO (public float exceeds $75M threshold) [VERIFIED]")
    else:
        out.append("- Baby shelf restriction: [UNVERIFIED]")

    cap_raised = cash.get("capital_raised_since_usd")
    if cap_raised is not None and cap_raised > 0:
        out.append(f"- Capital raised since {cash_period}: {_fmt_money(cap_raised)} [FINVIZ REPORTED]")

    cash_bars = (cash.get("chart") or {}).get("bars") or []
    if cash_bars:
        out.append("Cash history (millions USD):")
        points = []
        for bar in cash_bars:
            date = "Current Est." if bar.get("kind") == "estimate" else (bar.get("period_end") or "Period")
            value = bar.get("cash_usd")
            if value is not None:
                points.append(f"{date}: {_fmt_money(value, 1)}")
        out.append("- " + " | ".join(points))

    out.append("")

    # --------------------------------------------------------------------------
    # 3. OPEN DILUTION (Instruments & Facilities)
    # --------------------------------------------------------------------------
    out.append("DILUTION INSTRUMENTS")

    # ATM Programs
    atms = cards.get("atm") or []
    if atms:
        for atm in atms:
            title = atm.get("title") or "At-The-Market Program"
            reg = "Registered" if atm.get("registered") == "Registered" else "Unregistered"
            tot = atm.get("total_capacity")
            rem = atm.get("remaining_capacity")
            sold = atm.get("sales_total_usd")
            agent = atm.get("placement_agent") or "Agent not stated"
            updated = atm.get("last_update_date") or ""

            # Check if active
            is_active = (rem is not None and rem > 0) or (rem is None and tot is not None)
            status_tag = "[VERIFIED ACTIVE]" if (rem is not None and rem > 0) else ("[FULLY UTILIZED/EXHAUSTED]" if rem == 0.0 else "[FINVIZ REPORTED]")

            if atm == atms[0]:
                out.append(f"ATM — {len(atms)} programs · {_fmt_money(sum((a.get('remaining_capacity') or 0) for a in atms))} remaining capacity")
                out.append("Instrument | Filed | Base Shelf | Remaining Capacity | Total Capacity | Remaining (ex. I.B.6) | Baby Shelf | Sold to Date | Used % | Sales Agent | Start Date | End Date | Bank Tier | Investor Class | Updated")
            out.append(row(title, [atm.get("registered"), atm.get("parent_shelf"), _fmt_money(rem) if rem is not None else None, _fmt_money(tot) if tot is not None else None, _fmt_money(atm.get("remaining_without_baby_shelf")) if atm.get("remaining_without_baby_shelf") is not None else None, atm.get("limited_by_baby_shelf"), _fmt_money(sold) if sold is not None else None, f"{atm['used_pct']:.0f}%" if atm.get("used_pct") is not None else None, agent, atm.get("agreement_start_date"), atm.get("agreement_end_date"), atm.get("bank_tier"), atm.get("investor_class"), updated]))
    else:
        out.append("- ATM: none reported on Finviz [NOT FOUND]")

    # Equity Line of Credit (ELOC)
    elocs = cards.get("equity_line") or []
    if elocs:
        for el in elocs:
            title = el.get("title") or "Equity Line of Credit"
            reg = "Registered" if el.get("registered") == "Registered" else "Unregistered"
            tot = el.get("total_capacity")
            rem = el.get("remaining_capacity")
            sold = el.get("sales_total_usd")
            term = el.get("terminated")
            end_date = el.get("agreement_end_date") or ""

            if term is True:
                out.append(f"- ELOC: {title} — TERMINATED [VERIFIED INACTIVE]")
                continue

            status_tag = "[VERIFIED ACTIVE]" if (rem is not None and rem > 0) else "[FINVIZ REPORTED]"
            details = []
            if rem is not None:
                details.append(f"remaining drawable: {_fmt_money(rem)}")
            if tot is not None:
                details.append(f"headline facility: {_fmt_money(tot)}")
            if sold is not None and sold > 0:
                details.append(f"drawn to date: {_fmt_money(sold)}")
            if end_date:
                details.append(f"expires {end_date}")

            implied_sh = ""
            if current_price and current_price > 0 and rem is not None and rem > 0:
                ish = rem / current_price
                implied_sh = f" · ~{_fmt_shares(ish)} implied shares @ ${current_price:.2f}"

            if el == elocs[0]:
                out.extend(["", f"EQUITY LINES — {len(elocs)} line(s) · {_fmt_money(sum((e.get('remaining_capacity') or 0) for e in elocs))} remaining capacity"])
                out.append("Instrument | Filed | Base Shelf | Remaining Capacity | Total Capacity | Sold to Date | Used % | Counterparty | Terminated | Start Date | End Date | Bank Tier | Investor Class | Updated")
            out.append(row(title, [el.get("registered"), el.get("parent_shelf"), _fmt_money(rem) if rem is not None else None, _fmt_money(tot) if tot is not None else None, _fmt_money(sold) if sold is not None else None, f"{el['used_pct']:.0f}%" if el.get("used_pct") is not None else None, el.get("counterparty"), term, el.get("agreement_start_date"), end_date, el.get("bank_tier"), el.get("investor_class"), el.get("last_update_date")]))
    else:
        out.append("- ELOC: none reported on Finviz [NOT FOUND]")

    # Shelf Registrations
    shelves = cards.get("shelf") or []
    if shelves:
        for sh in shelves:
            title = sh.get("title") or "Shelf Registration"
            status = sh.get("shelf_status") or "active"
            tot = sh.get("total_shelf_capacity")
            raisable = sh.get("current_raisable_amount")
            is_baby = sh.get("is_baby_shelf_restricted")
            raised = sh.get("total_amount_raised")
            exp = sh.get("expiration_date") or ""
            banker = sh.get("last_banker") or ""

            details = []
            if raisable is not None:
                details.append(f"currently raisable: {_fmt_money(raisable)}")
            if tot is not None:
                details.append(f"total shelf size: {_fmt_money(tot)}")
            if raised is not None and raised > 0:
                details.append(f"raised to date: {_fmt_money(raised)}")
            if is_baby:
                details.append("baby-shelf limited (1/3 float)")
            if exp:
                details.append(f"expires {exp}")
            if banker:
                details.append(f"banker: {banker}")

            status_tag = "[VERIFIED ACTIVE]" if status == "active" else f"[{status.upper()}]"
            if sh == shelves[0]:
                out.extend(["", f"SHELF — {len(shelves)} shelf(s) · {_fmt_money(sum((s.get('current_raisable_amount') or 0) for s in shelves))} raisable"])
                out.append("Instrument | Filed | Status | Remaining Capacity | Total Capacity | Unlimited | Baby Shelf | Total Raised | Raised (12mo, I.B.6) | Outstanding Shares | Float | I.B.6 Float | 60d High | Price to Exit Cap | Underwriter | Effective Date | Expiration | Updated")
            out.append(row(title, [sh.get("registered"), status, _fmt_money(raisable) if raisable is not None else None, _fmt_money(tot) if tot is not None else None, sh.get("unlimited"), is_baby, _fmt_money(raised) if raised is not None else None, _fmt_money(sh.get("raised_last_12mo_under_ib6")) if sh.get("raised_last_12mo_under_ib6") is not None else None, _fmt_shares(sh.get("outstanding_shares")) if sh.get("outstanding_shares") is not None else None, _fmt_shares(sh.get("float_shares")) if sh.get("float_shares") is not None else None, _fmt_money(sh.get("ib6_float_value")) if sh.get("ib6_float_value") is not None else None, f"${sh['highest_60_day_close']:.2f}" if sh.get("highest_60_day_close") is not None else None, f"${sh['price_to_exceed_baby_shelf']:.2f}" if sh.get("price_to_exceed_baby_shelf") is not None else None, banker, sh.get("effect_date"), exp, sh.get("last_update_date")]))
    else:
        out.append("- Shelf: no active shelf registration reported on Finviz [NOT FOUND]")

    # Warrants
    warrants = cards.get("warrant") or []
    if warrants:
        w_lines = []
        for w in warrants:
            title = w.get("title") or "Warrant"
            rem_sh = w.get("remaining_outstanding")
            tot_sh = w.get("total_issued")
            ep = w.get("exercise_price")
            exp = w.get("expiration_date") or ""
            underwriter = w.get("underwriter") or ""
            reg = "Registered" if w.get("registered") == "Registered" else "Unregistered"

            ep_str = _fmt_money(ep, 2) if ep is not None else "—"

            # Check if in-the-money
            itm_str = ""
            if current_price and ep is not None:
                if current_price >= ep:
                    itm_str = " [IN THE MONEY - DILUTION RISK HIGH]"
                else:
                    itm_str = " [OUT OF THE MONEY]"

            w_lines.append(row(title, [w.get("registered"), w.get("parent_shelf"), w.get("resale_registration"), _fmt_shares(tot_sh) if tot_sh is not None else None, _fmt_shares(rem_sh) if rem_sh is not None else None, ep_str, ", ".join(w.get("known_owners") or []) or None, underwriter, w.get("issue_date"), w.get("exercisable_date"), exp, w.get("bank_tier"), w.get("investor_class"), w.get("last_update_date")]))

        out.append(f"WARRANTS — {len(warrants)} series · {sum((w.get('remaining_outstanding') or 0) for w in warrants):.1f} shares outstanding")
        out.append("Instrument | Filed | Base Shelf | Resale Registration | Shares Issued | Outstanding | Exercise Price | Known Owners | Underwriter | Issued | Exercisable | Expiration | Bank Tier | Investor Class | Updated")
        out.extend(w_lines)
    else:
        out.append("- Warrants: none reported on Finviz [NOT FOUND]")

    # Convertible Notes & Preferreds
    converts = cards.get("convertible") or []
    prefs = cards.get("preferred") or []
    if converts or prefs:
        if converts:
            principal_total = sum((c.get("principal_remaining") or 0) for c in converts)
            shares_total = sum((c.get("remaining_shares_issuable") or 0) for c in converts)
            out.append(f"CONVERTIBLE NOTES — {len(converts)} notes · {money_ui(principal_total)} principal · {shares_ui(shares_total)} shares")
            out.append("Instrument | Filed | Resale Registration | Principal Issued | Principal Remaining | Conversion Price | Shares Issuable | Remaining Issuable | Known Owners | Underwriter | Issued | Convertible | Maturity | Bank Tier | Investor Class | Updated")
        for c in converts:
            title = c.get("title") or "Convertible Note"
            out.append(row(title, [c.get("registered"), c.get("resale_registration"), _fmt_money(c.get("principal_total")) if c.get("principal_total") is not None else None, _fmt_money(c.get("principal_remaining")) if c.get("principal_remaining") is not None else None, f"${c['conversion_price']:.2f}" if c.get("conversion_price") is not None else None, _fmt_shares(c.get("total_shares_issuable")) if c.get("total_shares_issuable") is not None else None, _fmt_shares(c.get("remaining_shares_issuable")) if c.get("remaining_shares_issuable") is not None else None, ", ".join(c.get("known_owners") or []) or None, c.get("underwriter"), c.get("issue_date"), c.get("convertible_date"), c.get("maturity_date"), c.get("bank_tier"), c.get("investor_class"), c.get("last_update_date")]))

        if prefs:
            principal_total = sum((p.get("principal_remaining") or p.get("principal_total") or 0) for p in prefs)
            shares_total = sum((p.get("remaining_shares_issuable") or p.get("total_shares_issuable") or 0) for p in prefs)
            out.append(f"CONVERTIBLE PREFERRED — {len(prefs)} issues · {money_ui(principal_total)} principal · {shares_ui(shares_total)} shares")
            out.append("Instrument | Filed | Resale Registration | Principal Issued | Principal Remaining | Conversion Price | Shares Issuable | Remaining Issuable | Known Owners | Underwriter | Issued | Convertible | Maturity | Bank Tier | Investor Class | Updated")
            for p in prefs:
                title = p.get("title") or "Convertible Preferred"
                out.append(row(title, [p.get("registered"), p.get("resale_registration"), _fmt_money(p.get("principal_total")) if p.get("principal_total") is not None else None, _fmt_money(p.get("principal_remaining")) if p.get("principal_remaining") is not None else None, f"${p['conversion_price']:.2f}" if p.get("conversion_price") is not None else None, _fmt_shares(p.get("total_shares_issuable")) if p.get("total_shares_issuable") is not None else None, _fmt_shares(p.get("remaining_shares_issuable")) if p.get("remaining_shares_issuable") is not None else None, ", ".join(p.get("known_owners") or []) or None, p.get("underwriter"), p.get("issue_date"), p.get("convertible_date"), p.get("maturity_date"), p.get("bank_tier"), p.get("investor_class"), p.get("last_update_date")]))
    else:
        out.append("- Convertible securities: none reported on Finviz [NOT FOUND]")

    # Other active mechanisms (e.g., S-1 unpriced offerings)
    s1s = cards.get("s1_offering") or []
    if s1s:
        out.append(f"- Active S-1 Offerings: {len(s1s)} pending registration [UNVERIFIED PRICING]")
    else:
        out.append("- Other active financing mechanisms: none pending")

    out.append("")

    # --------------------------------------------------------------------------
    # 4. TODAY'S SELLING RISK SYNTHESIS
    # --------------------------------------------------------------------------
    out.append("TODAY'S SELLING RISK")

    active_mechanisms = []
    potential_mechanisms = []
    total_potential_shares: float = 0.0

    # 1. ATM evaluation
    for atm in atms:
        rem = atm.get("remaining_capacity")
        if rem is not None and rem > 0:
            active_mechanisms.append(f"ATM ({atm.get('placement_agent', 'Agent')}) with {_fmt_money(rem)} capacity")
            if current_price and current_price > 0:
                total_potential_shares += rem / current_price

    # 2. ELOC evaluation
    for el in elocs:
        if el.get("terminated") is not True:
            rem = el.get("remaining_capacity")
            if rem is not None and rem > 0:
                potential_mechanisms.append(f"ELOC with {_fmt_money(rem)} reported facility (requires registration & trading volume)")
                if current_price and current_price > 0:
                    total_potential_shares += rem / current_price

    # 3. Shelf restriction check
    shelf_raisable_cap = None
    for sh in shelves:
        if sh.get("shelf_status") == "active":
            r_amt = sh.get("current_raisable_amount")
            if r_amt is not None and r_amt > 0:
                shelf_raisable_cap = r_amt
                active_mechanisms.append(f"Active shelf capacity {_fmt_money(r_amt)} raisable now")

    # 4. In-the-money warrants
    itm_warrant_shares = 0.0
    for w in warrants:
        rem_sh = w.get("remaining_outstanding") or w.get("total_issued")
        ep = w.get("exercise_price")
        if rem_sh and ep and current_price and current_price >= ep:
            itm_warrant_shares += rem_sh
            active_mechanisms.append(f"In-the-money warrants ({_fmt_shares(rem_sh)} sh @ ${ep:.2f})")
            total_potential_shares += rem_sh

    # 5. Convertibles
    for c in converts:
        rem_iss = c.get("remaining_shares_issuable")
        if rem_iss and rem_iss > 0:
            potential_mechanisms.append(f"Convertible note overhang: {_fmt_shares(rem_iss)} issuable shares")
            total_potential_shares += rem_iss

    if active_mechanisms:
        out.append(f"- Confirmed available issuance mechanisms: {'; '.join(active_mechanisms)} [VERIFIED ACTIVE]")
    else:
        out.append("- Confirmed available issuance mechanisms: None immediately active [VERIFIED]")

    if potential_mechanisms:
        out.append(f"- Potential but unverified mechanisms: {'; '.join(potential_mechanisms)} [FINVIZ REPORTED]")
    else:
        out.append("- Potential but unverified mechanisms: None")

    if current_price and current_price > 0:
        out.append(f"- Estimated potential shares at current price (${current_price:.2f}): ~{_fmt_shares(total_potential_shares)} shares [ESTIMATED]")
    else:
        out.append(f"- Estimated potential shares: ~{_fmt_shares(total_potential_shares)} shares [ESTIMATED]")

    # Important conditions and unknowns
    conditions = []
    if is_baby:
        conditions.append("Baby-shelf cap (SEC Rule 415/I.B.6) strictly caps total primary offerings to 1/3 of public float")
    if atms and any(a.get("remaining_capacity") is None for a in atms):
        conditions.append("ATM program capacity draws against primary shelf; exact unallocated capacity depends on recent prospectuses")
    if not conditions:
        conditions.append("Sales subject to broker pacing and market liquidity; headline facility size does not equal instantaneous selling")

    out.append(f"- Important conditions and unknowns: {'; '.join(conditions)}")
    # Match the Finviz page order and keep only its displayed sections.
    if "TODAY'S SELLING RISK" in out:
        out = out[:out.index("TODAY'S SELLING RISK")]
    out = [line for line in out if not line.startswith("- Other active financing mechanisms:")]

    # The page's cash section is a short runway note followed by its history chart.
    if "CASH POSITION" in out and "DILUTION INSTRUMENTS" in out:
        cash_start = out.index("CASH POSITION")
        instruments_start = out.index("DILUTION INSTRUMENTS")
        cash_block = out[cash_start:instruments_start]
        keep_cash = ["CASH POSITION"]
        keep_cash.extend(line for line in cash_block if line.startswith("About "))
        for index, line in enumerate(cash_block):
            if line.startswith("Cash history "):
                keep_cash.append(line)
                if index + 1 < len(cash_block):
                    keep_cash.append(cash_block[index + 1])
                break
        out = out[:cash_start] + keep_cash + out[instruments_start:]

    section_markers = {
        "ATM": ("ATM —", "- ATM:"),
        "EQUITY LINES": ("EQUITY LINES —", "- ELOC:"),
        "SHELF": ("SHELF —", "- Shelf:"),
        "WARRANTS": ("WARRANTS —", "- Warrants:"),
        "CONVERTIBLE NOTES": ("CONVERTIBLE NOTES —",),
        "CONVERTIBLE PREFERRED": ("CONVERTIBLE PREFERRED —", "- Convertible securities:"),
    }
    starts = {}
    for name, prefixes in section_markers.items():
        for index, line in enumerate(out):
            if line.startswith(prefixes):
                starts[name] = index
                break
    ordered_indices = sorted(starts.values())
    blocks = {}
    for name, start in starts.items():
        end = next((index for index in ordered_indices if index > start), len(out))
        blocks[name] = out[start:end]
    if "DILUTION INSTRUMENTS" in out:
        header_index = out.index("DILUTION INSTRUMENTS")
        prefix = out[:header_index + 1]
        suffix = out[max(ordered_indices) + 1:] if ordered_indices else out[header_index + 1:]
        ordered_names = ("WARRANTS", "CONVERTIBLE NOTES", "CONVERTIBLE PREFERRED", "EQUITY LINES", "SHELF", "ATM")
        out = prefix + [line for name in ordered_names for line in blocks.get(name, [])] + suffix

    out.append(f"Source: {source_url} (updated {update_date})")
    # The primary-source heading identifies Finviz once; per-field provenance
    # tags obscure the UI-style summary and instrument tables.
    return [re.sub(r"\s+\[[^\]]+\]", "", line).replace(" [FINVIZ REPORTED]", "") for line in out]
