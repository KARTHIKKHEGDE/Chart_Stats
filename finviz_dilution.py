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


def _fmt_shares(val: Optional[float]) -> str:
    if val is None:
        return "—"
    a = abs(val)
    sign = "-" if val < 0 else ""
    if a >= 1e9:
        return f"{sign}{a / 1e9:.2f}B"
    if a >= 1e6:
        return f"{sign}{a / 1e6:.2f}M"
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
        if status_code == 404:
            return None, f"Ticker {t} not found on Finviz (HTTP 404)"
        if status_code == 429:
            return None, "Finviz rate limit reached (HTTP 429). Please try again shortly."
        if status_code != 200:
            return None, f"Finviz returned HTTP {status_code}"
        # Search for schema_version JSON payload
        m = re.search(r'(\{"schema_version":\s*1.*?\})</script>', html, re.DOTALL)
        if not m:
            # Fallback regex if closing script tag has whitespace or attributes
            m = re.search(r'(\{"schema_version":\s*1.*?"cards":\s*\{.*?\}.*?\})', html, re.DOTALL)

        if not m:
            return None, f"No structured dilution tab data found on Finviz for {t} (ticker may lack active dilution tracking)"

        raw_json = m.group(1)
        data = json.loads(raw_json)
        data["_fetched_at"] = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
        data["_source_url"] = url

        # Save to cache
        try:
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except Exception:
            pass

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
    out: List[str] = []
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
    out.append("DILUTION RISK")
    score = overall.get("score")
    label = overall.get("label") or "Unrated"
    if score is not None:
        out.append(f"- Finviz risk score: {score}/100 ({label}) [FINVIZ REPORTED]")
    else:
        out.append(f"- Finviz risk score: {label} [FINVIZ REPORTED]")

    oa = drivers.get("offering_ability")
    if oa:
        out.append(f"- Offering risk: {oa.get('band_text', '—')} ({oa.get('score', '—')}/100) [FINVIZ REPORTED]")
    else:
        out.append("- Offering risk: [UNVERIFIED]")

    cn = drivers.get("cash_need")
    if cn:
        out.append(f"- Cash need: {cn.get('band_text', '—')} ({cn.get('score', '—')}/100) [FINVIZ REPORTED]")
    else:
        out.append("- Cash need: [UNVERIFIED]")

    oh = drivers.get("overhang")
    if oh:
        out.append(f"- Overhang: {oh.get('band_text', '—')} ({oh.get('score', '—')}/100) [FINVIZ REPORTED]")
    else:
        out.append("- Overhang: [UNVERIFIED]")

    dh = drivers.get("dilution_history")
    if dh:
        out.append(f"- Dilution history: {dh.get('band_text', '—')} ({dh.get('score', '—')}/100) [FINVIZ REPORTED]")

    out.append(f"- Page updated: {update_date} [FINVIZ REPORTED]")

    summary_text = brief.get("summary")
    if summary_text:
        # Wrap summary cleanly
        clean_sum = summary_text.strip().replace("\n", " ")
        out.append(f"- Summary: {clean_sum}")
    out.append("")

    # --------------------------------------------------------------------------
    # 2. FINANCIAL POSITION
    # --------------------------------------------------------------------------
    out.append("FINANCIAL POSITION")
    rep_cash = cash.get("latest_reported_cash_usd")
    cash_only = cash.get("latest_reported_cash_only_usd")
    investments = cash.get("latest_reported_investments_usd")
    cash_period = cash.get("latest_period_end") or "latest period"

    if rep_cash is not None:
        parts = []
        if cash_only is not None:
            parts.append(f"{_fmt_money(cash_only)} cash")
        if investments and investments > 0:
            parts.append(f"{_fmt_money(investments)} short-term investments")
        breakdown = f" ({' + '.join(parts)})" if parts else ""
        out.append(f"- Reported cash: {_fmt_money(rep_cash)} as of {cash_period}{breakdown} [FINVIZ REPORTED]")
    else:
        out.append("- Reported cash: [UNVERIFIED]")

    # Cash Burn rate
    op_cf_q = cash.get("op_cf_quarterly_usd")
    if op_cf_q is not None:
        monthly_burn = abs(op_cf_q) / 3.0
        out.append(f"- Monthly burn: ~{_fmt_money(monthly_burn)}/month (quarterly operating cash flow: {_fmt_money(op_cf_q)}) [ESTIMATED]")
    else:
        out.append("- Monthly burn: [UNVERIFIED]")

    # Runway
    runway_months = cash.get("months_of_cash")
    if runway_months is not None:
        out.append(f"- Estimated runway: {runway_months:.1f} months [FINVIZ REPORTED]")
    else:
        out.append("- Estimated runway: [UNVERIFIED]")

    # Shares Outstanding & Float
    so = company.get("shares_outstanding")
    fl = company.get("float_shares")
    os_latest = (company.get("os_chart") or {}).get("latest") or {}
    so_src = os_latest.get("source") or "Finviz company data"

    if so is not None:
        fl_str = f" · Free Float: {_fmt_shares(fl)}" if fl is not None else ""
        out.append(f"- Shares outstanding: {_fmt_shares(so)}{fl_str} ({so_src}) [FINVIZ REPORTED]")
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

    out.append("")

    # --------------------------------------------------------------------------
    # 3. OPEN DILUTION (Instruments & Facilities)
    # --------------------------------------------------------------------------
    out.append("OPEN DILUTION")

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

            details = []
            if rem is not None:
                details.append(f"remaining capacity: {_fmt_money(rem)}")
            if tot is not None:
                details.append(f"authorized: {_fmt_money(tot)}")
            if sold is not None and sold > 0:
                details.append(f"sold to date: {_fmt_money(sold)}")
            details.append(f"agent: {agent}")
            if updated:
                details.append(f"as of {updated}")

            implied_sh = ""
            if current_price and current_price > 0 and rem is not None and rem > 0:
                ish = rem / current_price
                implied_sh = f" · ~{_fmt_shares(ish)} implied shares @ ${current_price:.2f}"

            out.append(f"- ATM: {title} ({reg}) — {', '.join(details)}{implied_sh} {status_tag}")
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

            out.append(f"- ELOC: {title} ({reg}) — {', '.join(details)}{implied_sh} {status_tag}")
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
            out.append(f"- Shelf: {title} ({status}) — {', '.join(details)} {status_tag}")
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

            ep_str = f"exercise px ${ep:,.4g}" if ep is not None else "exercise px unstated"
            sh_str = f"{_fmt_shares(rem_sh)} warrants" if rem_sh is not None else f"{_fmt_shares(tot_sh)} issued"

            # Check if in-the-money
            itm_str = ""
            if current_price and ep is not None:
                if current_price >= ep:
                    itm_str = " [IN THE MONEY - DILUTION RISK HIGH]"
                else:
                    itm_str = " [OUT OF THE MONEY]"

            meta = []
            meta.append(reg)
            if exp:
                meta.append(f"exp {exp}")
            if underwriter:
                meta.append(underwriter)

            w_lines.append(f"  * {title}: {sh_str} @ {ep_str} ({', '.join(meta)}){itm_str}")

        out.append(f"- Warrants ({len(warrants)} cards reported) [FINVIZ REPORTED]:")
        out.extend(w_lines)
    else:
        out.append("- Warrants: none reported on Finviz [NOT FOUND]")

    # Convertible Notes & Preferreds
    converts = cards.get("convertible") or []
    prefs = cards.get("preferred") or []
    if converts or prefs:
        out.append("- Convertible Securities [FINVIZ REPORTED]:")
        for c in converts:
            title = c.get("title") or "Convertible Note"
            princ_rem = c.get("principal_remaining")
            conv_p = c.get("conversion_price")
            rem_issuable = c.get("remaining_shares_issuable")
            mat = c.get("maturity_date") or ""
            owners = ", ".join(c.get("known_owners") or [])

            details = []
            if princ_rem is not None:
                details.append(f"principal remaining: {_fmt_money(princ_rem)}")
            if conv_p is not None:
                details.append(f"conversion px: ${conv_p:.4g}")
            if rem_issuable is not None:
                details.append(f"issuable: {_fmt_shares(rem_issuable)} shares")
            if owners:
                details.append(f"holder: {owners}")
            if mat:
                details.append(f"maturity: {mat}")

            out.append(f"  * Convertible Note: {title} — {', '.join(details)}")

        for p in prefs:
            title = p.get("title") or "Convertible Preferred"
            princ = p.get("principal_remaining") or p.get("principal_total")
            conv_p = p.get("conversion_price")
            sh = p.get("remaining_shares_issuable") or p.get("total_shares_issuable")
            details = []
            if princ:
                details.append(f"amount: {_fmt_money(princ)}")
            if conv_p:
                details.append(f"conversion px: ${conv_p:.4g}")
            if sh:
                details.append(f"issuable: {_fmt_shares(sh)} shares")
            details_str = f" — {', '.join(details)}" if details else ""
            out.append(f"  * Preferred Stock: {title}{details_str}")
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
    out.append(f"- Source: Finviz Dilution ({source_url}) [Data a/o {update_date}]")

    return out
