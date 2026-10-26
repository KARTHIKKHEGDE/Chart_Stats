"""Strict post-cutoff financing logic for the cash-position estimate.

Pure functions (pandas + re only; no network, no Flask) so they can be unit-tested.

Rule: cash counts ONLY if the issuer actually received it AFTER the latest reported
cash-balance date (the cutoff).  A filing date is never treated as a receipt date.

    events = extract_events(text, form, filed_date)      # per filing
    result = reconcile(merge_events(all_events), cutoff)  # dedupe + classify
    est    = estimate_cash(reported, cutoff, result, monthly_burn, now)
"""
from __future__ import annotations

import copy
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import pandas as pd

# --------------------------------------------------------------------------
# Low-level parsing helpers
# --------------------------------------------------------------------------
_MON = (r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|June?|July?|"
        r"Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)")
DATE_RE = re.compile(r"\b(" + _MON + r")\.?\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})\b", re.I)
_MONEY = r"\$\s?([\d,]+(?:\.\d+)?)(?:\s*(billion|million|thousand)\b|\s?([MBK])\b)?"
MONEY_RE = re.compile(_MONEY, re.I)
_UNITS = {"billion": 1e9, "b": 1e9, "million": 1e6, "m": 1e6, "thousand": 1e3, "k": 1e3}


def _amt(m) -> float:
    unit = (m.group(2) or m.group(3) or "").lower()
    return float(m.group(1).replace(",", "")) * _UNITS.get(unit, 1.0)


def find_dates(s: str):
    out = []
    for m in DATE_RE.finditer(s):
        try:
            ts = pd.Timestamp(f"{m.group(1)[:3].title()} {int(m.group(2))} {m.group(3)}")
        except Exception:
            continue
        out.append((m.start(), ts))
    return out


def _date_near(dates, pos):
    return min(dates, key=lambda d: abs(d[0] - pos))[1] if dates else None


def split_sentences(text: str) -> List[str]:
    return [x.strip() for x in re.split(r"(?<=[.;])\s+(?=[A-Z(\"'\u201c])", text) if x.strip()]


def _money_after(s: str, kw: re.Pattern, window: int = 90, skip_fee: bool = False,
                 allow_before: bool = True) -> Optional[float]:
    """First $ amount after the keyword (else just before it)."""
    m = kw.search(s)
    if not m:
        return None
    for mm in MONEY_RE.finditer(s, m.end(), min(len(s), m.end() + window)):
        between = s[m.end():mm.start()]
        if skip_fee and re.search(r"fee|expense|commission|discount|deduct", between, re.I):
            continue
        return _amt(mm)
    if allow_before:
        last = None
        for mm in MONEY_RE.finditer(s, max(0, m.start() - 45), m.start()):
            last = mm
        if last:
            return _amt(last)
    return None


# --------------------------------------------------------------------------
# Patterns
# --------------------------------------------------------------------------
GROSS_KW = re.compile(
    r"gross (?:cash )?proceeds|aggregate proceeds|aggregate exercise price(?: of)?|"
    r"(?<!net )(?<!net cash )proceeds (?:of|totaling|totalling)|\braised\b|\braising\b", re.I)
NET_KW = re.compile(r"net (?:cash )?proceeds", re.I)
FEE_A = re.compile(r"(?:fees?|commissions?|discounts?|expenses|costs)(?:(?!proceeds)[^.$]){0,60}?" + _MONEY, re.I)
FEE_B = re.compile(_MONEY + r"\s+(?:in|of|for)\s+(?:[\w\-]+\s+){0,3}(?:fees?|commissions?|expenses|costs)", re.I)
FEE_PCT = re.compile(r"(\d+(?:\.\d+)?)\s?%\s+(?:cash\s+)?(?:placement\s+|underwriting\s+|agent\s+)?"
                     r"(?:fee|commission|discount)", re.I)
OFFER_AMT = re.compile(_MONEY + r"\s+(?:[\w\-]+\s+){0,3}(?:registered direct|private placement|\bPIPE\b|"
                       r"public offering|underwritten offering|offering|financing|note|notes|debenture|\bSAFE\b)", re.I)
CAP_RE = re.compile(r"\bup to\b|aggregate offering price of up to|\bmay (?:offer|sell)\b|from time to time|"
                    r"remaining (?:capacity|availability|amount)|available (?:capacity|for (?:future )?sale)|"
                    r"\bcapacity\b", re.I)
CLOSED_RE = re.compile(r"\b(?:closed|consummat(?:ed|ion)|completed|funded|settled|borrowed|drew|sold|provided|disbursed)\b|"
                       r"\breceived\b[^.]{0,30}proceeds|\bhas received\b|\bobtained access to\b|"
                       r"announc\w+ the (?:closing|completion) of", re.I)
FORWARD_I = re.compile(r"\b(?:expected|anticipated|scheduled|intends?|subject to|to close|upon|"
                       r"to be completed|conditioned|proposed|not|terminated|failed|unable)\b", re.I)
FORWARD_CS = re.compile(r"\b(?:may|could|would|will|if)\b")  # case-sensitive: "May 5" is a date
SIGNED_RE = re.compile(r"\b(?:entered into|agreed to|executed|signed|priced|announced|engaged)\b", re.I)
NONCASH_RE = re.compile(r"cashless|net exercise|conversion of (?:the )?(?:principal|notes?|debt|preferred|safes?)|"
                        r"converted into|exchanged? .{0,40}\bfor\b|in exchange for (?!\$|cash|an aggregate|"
                        r"aggregate|gross|a purchase price|the purchase price)|debt (?:conversion|settlement|exchange)|"
                        r"in satisfaction of|in lieu of cash|non-?cash|merger consideration\b|deemed value\b|"
                        r"shares of (?:PubCo\s+)?(?:Class [AB]\s+)?common stock and [\d,]+\s+shares", re.I)
SELLING_RE = re.compile(r"selling (?:stock|share)holders?|selling securityholders?|selling holders?|"
                        r"secondary offering", re.I)
NO_COMPANY_PROCEEDS = re.compile(r"(?:we|the company|company) (?:will not|did not|does not|do not) receive any "
                                 r"(?:of the )?(?:net )?proceeds", re.I)
COMPANY_RECEIPT = re.compile(r"(?:we|the company|company) (?:received|will receive|obtained access)", re.I)
IPO_RE = re.compile(r"initial public offering|\bIPO\b", re.I)
ATM_RE = re.compile(r"at-the-market|\bATM\b|sales agreement|equity distribution agreement|sales agent", re.I)
DEBT_RE = re.compile(r"convertible (?:promissory )?notes?|promissory note|\bloan\b|credit (?:agreement|facility)|"
                     r"debenture|term loan|senior secured", re.I)
SAFE_RE = re.compile(r"\bSAFE\b|simple agreement for future equity", re.I)
FALSE_OFFERING_RE = re.compile(r"\b(?:pipeline|customer pipeline|authorized to issue|authority to issue|backlog|tam|total addressable market)\b", re.I)
MERGER_CLOSE_RE = re.compile(r"consummat\w+\s+(?:the\s+)?(?:[\w\-]+\s+){0,3}(?:Business Combination|Merger)", re.I)
_STATUS_RANK = {"proposed": 0, "signed": 1, "closed": 2}


@dataclass
class Event:
    kind: str = "offering"          # offering|atm|warrant_exercise|debt|merger|ipo
    gross: Optional[float] = None
    net: Optional[float] = None
    fees: Optional[float] = None
    fee_pct: Optional[float] = None
    cap_amount: Optional[float] = None
    signed: Optional[pd.Timestamp] = None
    closed: Optional[pd.Timestamp] = None
    start: Optional[pd.Timestamp] = None   # ATM sale-period start
    status: str = "proposed"               # proposed|signed|closed
    issuer_cash: bool = True
    noncash: bool = False
    capacity_only: bool = False
    idx: int = 0
    sources: List[str] = field(default_factory=list)
    excerpt: str = ""


def _closing_pos(s: str) -> Optional[int]:
    """Position of a closing verb that is NOT forward-looking/negated, else None."""
    for m in CLOSED_RE.finditer(s):
        prefix = s[max(0, m.start() - 60):m.start()]
        if FORWARD_I.search(prefix) or FORWARD_CS.search(prefix):
            continue
        return m.start()
    return None


def _trust_amount(s: str) -> Optional[float]:
    """Cash released/remaining from a SPAC trust or retained from a business combination after redemptions."""
    if ((re.search(r"trust account|retained from the Business Combination|access to cash proceeds", s, re.I) and
         re.search(r"released|remain(?:ed|ing)|available|redemption|retained|obtained access", s, re.I))
            and not re.search(r"placed|deposited|initial public offering|\bIPO\b", s, re.I)):
        m = MONEY_RE.search(s)
        return _amt(m) if m else None
    return None


def _kind(ctx: str) -> str:
    if re.search(r"warrants?", ctx, re.I) and re.search(r"exercis", ctx, re.I):
        return "warrant_exercise"
    if ATM_RE.search(ctx):
        return "atm"
    if SAFE_RE.search(ctx):
        return "safe"
    if DEBT_RE.search(ctx):
        return "debt"
    return "offering"


def _merger_close_date(sents: List[str]) -> Optional[pd.Timestamp]:
    for i, s in enumerate(sents):
        if MERGER_CLOSE_RE.search(s) and _closing_pos(s) is not None:
            d = find_dates(s) or (find_dates(sents[i - 1]) if i else [])
            if d:
                return d[0][1]
    return None


# --------------------------------------------------------------------------
# Extraction: one filing's text -> candidate financing events
# --------------------------------------------------------------------------
def extract_events(text: str, form: str, filed, label: Optional[str] = None) -> List[Event]:
    src = label or f"{form} filed {pd.Timestamp(filed):%d %b %y}"
    sents = split_sentences(text)
    doc_no_proceeds = bool(NO_COMPANY_PROCEEDS.search(text))
    merger_close = _merger_close_date(sents)
    events: List[Event] = []
    satellites = []  # (idx, dict) closing / net / fee facts without their own gross amount

    for i, s in enumerate(sents):
        prev = sents[i - 1] if i else ""
        ctx = prev + " " + s
        own_dates = find_dates(s)
        dates = own_dates or find_dates(prev)
        cpos = _closing_pos(s)
        cdate = None
        if cpos is not None and dates:
            cdate = _date_near(dates, cpos) if own_dates else dates[0][1]
        sm = SIGNED_RE.search(s)
        sdate = _date_near(own_dates, sm.start()) if (sm and own_dates) else None

        ipo = bool(IPO_RE.search(s))
        trust_amt = _trust_amount(s)
        net = _money_after(s, NET_KW, skip_fee=True, allow_before=False)
        fees = None
        for pat in (FEE_A, FEE_B):
            fm = pat.search(s)
            if fm:
                fees = _amt(fm) if pat is FEE_B else float(fm.group(1).replace(",", "")) * _UNITS.get(
                    (fm.group(2) or fm.group(3) or "").lower(), 1.0)
                break
        pm = FEE_PCT.search(s)
        fee_pct = float(pm.group(1)) / 100 if pm else None

        gross = None
        kind = "offering"
        if ipo:
            kind = "ipo"
            gross = _money_after(s, GROSS_KW, skip_fee=True)
        elif trust_amt is not None:
            kind, gross = "merger", trust_amt
        else:
            kind = _kind(ctx)
            if SAFE_RE.search(s) and not NONCASH_RE.search(s):
                kind = "safe"
                mm = MONEY_RE.search(s)
                if mm and (re.search(r"provided|issued|invested|raised|amount of", s, re.I) or GROSS_KW.search(s)):
                    gross = _amt(mm)
                    if cpos is None and (re.search(r"\bprovided\b|\bdisbursed\b|\breceived\b", s, re.I) or own_dates):
                        cpos = 0
            else:
                gross = _money_after(s, GROSS_KW, skip_fee=True)
                if gross is None and not CAP_RE.search(s) and not FALSE_OFFERING_RE.search(s):
                    om = OFFER_AMT.search(s)
                    gross = _amt(om) if om else None

        capacity = False
        cap_amount = None
        if CAP_RE.search(s) and cpos is None and not ipo and trust_amt is None:
            mm = MONEY_RE.search(s)
            if mm and (gross is not None or _kind(ctx) == "atm" or "$" in s):
                capacity, cap_amount, gross = True, (gross if gross is not None else _amt(mm)), None

        if gross is None and not capacity:
            # satellite sentence: closing/net/fee info that belongs to a neighbouring event
            if cdate is not None and "$" not in s and re.search(
                    r"offering|placement|financing|transaction|closing|exercise|issuance|business combination|"
                    r"merger|sale", s, re.I):
                satellites.append((i, {"closed": cdate}))
            elif net is not None or fees is not None or fee_pct is not None:
                satellites.append((i, {"net": net, "fees": fees, "fee_pct": fee_pct}))
            continue

        ev = Event(kind=kind, gross=gross, net=net, fees=fees, fee_pct=fee_pct, cap_amount=cap_amount,
                   capacity_only=capacity, idx=i, sources=[src], excerpt=s[:200])
        ev.noncash = bool(NONCASH_RE.search(s))
        if SELLING_RE.search(s) and not COMPANY_RECEIPT.search(s):
            ev.issuer_cash = False
        elif SELLING_RE.search(ctx) and doc_no_proceeds:
            ev.issuer_cash = False
        if sdate is not None:
            ev.signed = sdate
        if cpos is not None:
            ev.status = "closed"
            ev.closed = cdate
        elif sm:
            ev.status = "signed"
        if kind == "merger" and ev.closed is None and merger_close is not None:
            ev.status, ev.closed = "closed", merger_close
        if kind == "atm" and not capacity and ev.status == "closed":
            ds = [d for _, d in own_dates]
            if len(ds) >= 2:
                ev.start, ev.closed = min(ds), max(ds)
            elif len(ds) == 1:
                before = s[max(0, own_dates[0][0] - 25):own_dates[0][0]]
                if re.search(r"\b(?:since|from|commencing)\b", before, re.I):
                    ev.start, ev.closed = ds[0], None   # open-ended period: end unknown
                else:
                    ev.start, ev.closed = ds[0], ds[0]
        events.append(ev)

    # attach satellites to the nearest event (prefer the preceding one) within 3 sentences
    for i, info in satellites:
        cands = [e for e in events if abs(e.idx - i) <= 3]
        if not cands:
            continue
        e = min(cands, key=lambda x: (abs(x.idx - i), 0 if x.idx <= i else 1))
        if info.get("closed") is not None and e.closed is None and e.kind != "atm":
            e.closed, e.status = info["closed"], "closed"
        for k in ("net", "fees", "fee_pct"):
            if info.get(k) is not None and getattr(e, k) is None:
                setattr(e, k, info[k])
    return events


# --------------------------------------------------------------------------
# De-duplication across filings (8-K + press release + prospectus + 10-Q ...)
# --------------------------------------------------------------------------
def _same(a: Event, b: Event) -> bool:
    if a.gross is None or b.gross is None:
        return (a.capacity_only and b.capacity_only and a.cap_amount == b.cap_amount)
    if abs(a.gross - b.gross) > max(1000.0, 0.005 * max(a.gross, b.gross)):
        return False
    da, db = (a.closed or a.signed), (b.closed or b.signed)
    max_days = 120 if (a.kind == "safe" or b.kind == "safe") else 90
    return not (da is not None and db is not None and abs((da - db).days) > max_days)



def _absorb(m: Event, b: Event) -> None:
    m.sources += [x for x in b.sources if x not in m.sources]
    for k in ("net", "fees", "fee_pct"):
        if getattr(m, k) is None:
            setattr(m, k, getattr(b, k))
    for k in ("closed", "signed", "start"):   # earliest date = conservative
        vb, vm = getattr(b, k), getattr(m, k)
        if vb is not None:
            setattr(m, k, vb if vm is None else min(vm, vb))
    if _STATUS_RANK[b.status] > _STATUS_RANK[m.status]:
        m.status = b.status
    m.noncash = m.noncash or b.noncash
    m.issuer_cash = m.issuer_cash and b.issuer_cash
    if m.kind == "offering" and b.kind != "offering":
        m.kind = b.kind


def merge_events(events: List[Event]) -> List[Event]:
    merged: List[Event] = []
    for e in events:
        for m in merged:
            if _same(m, e):
                _absorb(m, e)
                break
        else:
            merged.append(copy.deepcopy(e))
    return merged


# --------------------------------------------------------------------------
# Reconciliation against the strict cutoff
# --------------------------------------------------------------------------
def _net(e: Event):
    """(net, fees, fees_documented). Never assumes a generic fee rate."""
    if e.net is not None:
        return e.net, (e.gross - e.net if e.gross is not None and e.net <= e.gross else e.fees), True
    if e.fees is not None and e.gross is not None:
        return e.gross - e.fees, e.fees, True
    if e.fee_pct is not None and e.gross is not None:
        return e.gross * (1 - e.fee_pct), e.gross * e.fee_pct, True
    return e.gross, None, False


def _item(e: Event, reason: str) -> Dict:
    net, fees, documented = _net(e)
    return {"kind": e.kind, "gross": e.gross, "cap": e.cap_amount, "fees": fees, "net": net,
            "fees_documented": documented, "closed": e.closed, "signed": e.signed,
            "start": e.start, "sources": list(e.sources), "reason": reason, "excerpt": e.excerpt}


def reconcile(events: List[Event], cutoff) -> Dict:
    cutoff = pd.Timestamp(cutoff).normalize()
    inc, pot, exc = [], [], []
    fd = lambda d: f"{d:%d %b %Y}"
    for e in events:
        if e.noncash:
            exc.append(_item(e, "noncash (cashless exercise / conversion / exchange)"))
        elif not e.issuer_cash:
            exc.append(_item(e, "proceeds go to selling shareholders, not the company"))
        elif e.capacity_only:
            exc.append(_item(e, "unsold ATM / shelf capacity - no sales shown"))
        elif e.kind == "ipo" and (e.closed is None or e.closed <= cutoff):
            when = f"closed {fd(e.closed)}" if e.closed is not None else "date not post-cutoff"
            exc.append(_item(e, f"historical IPO / SPAC IPO proceeds ({when}) - already received before cutoff"))
        elif e.kind == "atm":
            if e.status != "closed":
                pot.append(_item(e, "ATM sales not confirmed"))
            elif e.start is not None and e.start > cutoff:
                (inc if e.gross else pot).append(_item(e, f"ATM sales {fd(e.start)} onward (after cutoff)"))
            elif e.closed is not None and e.closed <= cutoff:
                exc.append(_item(e, f"ATM sales ended {fd(e.closed)} - on/before cutoff, already in reported cash"))
            else:
                pot.append(_item(e, "ATM sale period overlaps/undated vs cutoff - cannot split"))
        elif e.closed is None:
            if e.status == "closed":
                pot.append(_item(e, "closed, but receipt/closing date not disclosed"))
            elif e.signed is not None and e.signed > cutoff:
                pot.append(_item(e, f"{e.status} {fd(e.signed)}; closing not confirmed"))
            elif e.signed is not None:
                exc.append(_item(e, f"signed {fd(e.signed)} (on/before cutoff); no post-cutoff closing shown"))
            else:
                pot.append(_item(e, "no dates disclosed; closing not confirmed"))
        elif e.closed <= cutoff:
            exc.append(_item(e, f"closed {fd(e.closed)} - on/before cutoff, already in reported cash"))
        elif e.gross is None:
            pot.append(_item(e, "closed after cutoff but amount not disclosed"))
        else:
            inc.append(_item(e, f"closed {fd(e.closed)} (after cutoff)"
                             if e.kind != "merger" else f"merger closed {fd(e.closed)}: trust cash released"))
    return {"included": inc, "potential": pot, "excluded": exc, "cutoff": cutoff,
            "gross_verified": sum(i["gross"] or 0.0 for i in inc),
            "net_verified": sum(i["net"] or 0.0 for i in inc),
            "fees_unknown_n": sum(1 for i in inc if not i["fees_documented"])}


# --------------------------------------------------------------------------
# Estimate + auditable report
# --------------------------------------------------------------------------
def _m(x) -> str:
    return "n/a" if x is None else f"${x / 1e6:.3f}M"


def estimate_cash(reported_cash, cash_date, result: Dict, monthly_burn, now=None,
                  cash_form: str = "", burn_label: str = "") -> Dict:
    now = pd.Timestamp(now or pd.Timestamp.now()).normalize()
    cd = pd.Timestamp(cash_date).normalize()
    months = max(0.0, (now - cd).days / 30.4375)
    burned = monthly_burn * months if monthly_burn is not None else None
    fin = result["net_verified"]
    est = max(0.0, reported_cash + fin - (burned or 0.0))

    reasons = []
    if monthly_burn is None:
        reasons.append("operating burn unavailable - estimate is incomplete")
    if result["potential"]:
        reasons.append(f"{len(result['potential'])} potential financing event(s) unconfirmed (not counted)")
    if result["fees_unknown_n"]:
        reasons.append(f"fees undisclosed on {result['fees_unknown_n']} included deal(s); net may be overstated")
    age = (now - cd).days
    if age > 270:
        reasons.append(f"reported cash is {age} days old")
    level = ("Low" if (monthly_burn is None or age > 270) else "Medium" if reasons else "High")

    L = [f"CASH POSITION - strict cutoff {cd:%d %b %Y}: only financing received AFTER this date counts",
         f"  REPORTED cash: {_m(reported_cash)} as of {cd:%d %b %Y}" + (f" (Form {cash_form})" if cash_form else ""),
         f"  Verified post-report net financing: {_m(fin)}"
         + ("" if result["included"] else "  (none verified)")]
    for it in result["included"]:
        L.append(f"    + {it['kind']}: gross {_m(it['gross'])} - fees "
                 f"{_m(it['fees']) if it['fees'] is not None else 'undisclosed'} = net {_m(it['net'])}"
                 f" | {it['reason']} | src: {'; '.join(it['sources'])}")
    L.append("  Potential financing awaiting confirmation (NOT counted):"
             + (" none" if not result["potential"] else ""))
    for it in result["potential"]:
        L.append(f"    ? {it['kind']} {_m(it['gross'] if it['gross'] is not None else it['cap'])}"
                 f" | {it['reason']} | src: {'; '.join(it['sources'])}")
    L.append("  Excluded financing (NOT counted):" + (" none" if not result["excluded"] else ""))
    for it in result["excluded"][:12]:
        L.append(f"    x {it['kind']} {_m(it['gross'] if it['gross'] is not None else it['cap'])}"
                 f" | {it['reason']} | src: {'; '.join(it['sources'])}")
    if len(result["excluded"]) > 12:
        L.append(f"    ... and {len(result['excluded']) - 12} more excluded")
    if burned is not None:
        L.append(f"  Estimated burn since report: {_m(burned)} = {months:.1f} months "
                 f"({cd:%d %b %Y} -> {now:%d %b %Y}) x {_m(monthly_burn)}/month"
                 + (f" [{burn_label}]" if burn_label else ""))
    else:
        L.append("  Estimated burn since report: unavailable")
    L.append(f"  ESTIMATED cash today: {_m(est)} = reported {_m(reported_cash)} + verified net financing "
             f"{_m(fin)} - estimated burn {_m(burned if burned is not None else 0.0)}"
             + ("  [INCOMPLETE]" if monthly_burn is None else "") + "  <- ESTIMATE, not a reported figure")
    L.append(f"  Confidence: {level}" + (" - " + "; ".join(reasons) if reasons else
                                       " - every included deal has a closing date and documented fees"))
    L.append("  Not modeled: other post-report cash outflows/inflows unless disclosed.")
    return {"estimate": est, "burned": burned, "months": months, "net_financing": fin,
            "confidence": level, "reasons": reasons, "lines": L}