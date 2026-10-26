"""gemini_notes.py - Gemini-written notes with a 4-stage verification pipeline.

Stages:
  1. Analyst (FAST model)   : claims, each with source id + verbatim quote
  2. Python check           : quote must literally exist in the cited source, numbers too
  3. Auditor (STRONG model) : judges each surviving claim vs its source, lists missed events
  4. Writer (STRONG model)  : rewrites your existing notes in the SAME layout using verified claims only
  5. Number guard (Python)  : flags any number in the final text not traceable to a claim / Python fact

Setup:  pip install google-genai pydantic
.env :  GEMINI_API_KEY=...   (optional: GEMINI_MODEL, GEMINI_VERIFY_MODEL)
"""
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Optional

from google import genai
from pydantic import BaseModel

def _load_env():
    env_path = Path(__file__).with_name(".env")
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip("\"'"))
    if "GEMINI_API_KEY" in os.environ:
        os.environ["GOOGLE_API_KEY"] = os.environ["GEMINI_API_KEY"]


_load_env()

FAST = os.environ.get("GEMINI_MODEL", "gemini-3.5-flash")
STRONG = os.environ.get("GEMINI_VERIFY_MODEL", "gemini-3.5-flash")
CACHE = Path(__file__).with_name(".cache") / "gemini"
CACHE.mkdir(parents=True, exist_ok=True)
_client = None


def client():
    global _client
    if _client is None:
        _client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
    return _client


def call(model, prompt, schema=None):
    """One Gemini call, cached on disk by (model + prompt)."""
    ck = CACHE / (hashlib.md5((model + prompt).encode()).hexdigest() + ".txt")
    if ck.exists():
        return ck.read_text(encoding="utf-8")
    cfg = {"temperature": 0}
    if schema:
        cfg.update(response_mime_type="application/json", response_schema=schema)

    models_to_try = [model]
    for fallback in ["gemini-3.5-flash", "gemini-3.5-flash-lite"]:
        if fallback not in models_to_try:
            models_to_try.append(fallback)

    last_err = None
    for m in models_to_try:
        try:
            txt = client().models.generate_content(model=m, contents=prompt, config=cfg).text or ""
            if txt.strip():
                ck.write_text(txt, encoding="utf-8")
            return txt
        except Exception as e:
            last_err = e
            err_msg = str(e)
            if "503" in err_msg or "404" in err_msg or "UNAVAILABLE" in err_msg:
                print(f"  [gemini] {m} returned {err_msg[:60]}... trying fallback")
                continue
            raise
    raise last_err


# ---------------------------------------------------------------- schemas
class Claim(BaseModel):
    id: str                       # c1, c2 ...
    section: str                  # warrants | convertibles | atm | completed_dilution | cash | reverse_split | nasdaq | other
    statement: str
    source_id: str                # S1, S2 ... or "PY" for Python-computed facts
    quote: str                    # verbatim from that source
    value: Optional[float] = None


class Claims(BaseModel):
    claims: list[Claim]


class Verdict(BaseModel):
    id: str
    verdict: str                  # supported | contradicted | unsupported
    correction: Optional[str] = None


class Audit(BaseModel):
    verdicts: list[Verdict]
    missed_events: list[str]


# ---------------------------------------------------------------- helpers
def norm(s):
    return re.sub(r"\s+", " ", s or "").strip().lower()


def windows(text, rx, width=2500, limit=25000):
    """Keep only text around regex hits so long filings stay cheap."""
    spans = [(max(0, m.start() - width), m.end() + width) for m in rx.finditer(text)]
    merged = []
    for s, e in spans:
        if merged and s <= merged[-1][1]:
            merged[-1] = (merged[-1][0], e)
        else:
            merged.append((s, e))
    return " ... ".join(text[s:e] for s, e in merged)[:limit]


def render_sources(sources):
    return "\n\n".join(
        f"=== {sid} | {m['form']} | filed {m['date']} | {m['url']} ===\n{m['text']}"
        for sid, m in sources.items())


def py_check(claim, sources, py_facts):
    """Quote must exist verbatim in the cited source; numeric value must appear too."""
    src = py_facts if claim.source_id == "PY" else sources.get(claim.source_id, {}).get("text", "")
    if not src or not norm(claim.quote) or norm(claim.quote) not in norm(src):
        return False
    if claim.value is not None:
        digits = src.replace(",", "")
        v = int(claim.value) if float(claim.value).is_integer() else claim.value
        if str(v) not in digits and f"{claim.value:g}" not in digits:
            return False
    return True


NUM = re.compile(r"\d[\d,]*\.?\d*")


def number_guard(text, allowed_blob):
    """Flag lines containing numbers that appear in no verified claim / Python fact."""
    allowed = {n.rstrip(".,") for n in NUM.findall(allowed_blob.replace("$", " "))}
    out, bad = [], 0
    for line in text.splitlines():
        nums = [n.rstrip(".,") for n in NUM.findall(line)]
        if any(n not in allowed and len(n.replace(",", "")) > 1 for n in nums):
            line += "  [WARNING: number not traced to a verified source]"
            bad += 1
        out.append(line)
    return "\n".join(out), bad


# ---------------------------------------------------------------- prompts
ANALYST = """You are a small-cap dilution analyst. Using ONLY the sources below, list every fact about:
warrants (count, strike, expiry, holder, amendments, exercises), convertible notes (conversion formula,
discount, floor, maturity), ATM/shelf capacity and agent, completed share issuances (shares, gross proceeds,
date, counterparty, cash vs non-cash), reverse splits, Nasdaq deficiencies, cash and post-report financing.
Rules: never infer or estimate; no fact without a verbatim quote copied from its source; registered or
authorized is NOT issued; use source_id "PY" only for facts from the Python block. Claim ids: c1, c2, ...

PYTHON-COMPUTED FACTS (source PY):
{py}

SOURCES:
{src}"""

AUDITOR = """You are a strict auditor. For each claim, check it against its cited source text only.
verdict: supported | contradicted | unsupported. If a number, date or party differs, it is contradicted
and you give the correction. Then scan ALL sources for dilution events, warrants, convertibles or
financings that no claim covers and list them in missed_events with source id and a short verbatim quote.

CLAIMS:
{claims}

PYTHON-COMPUTED FACTS (source PY):
{py}

SOURCES:
{src}"""

WRITER = """Rewrite the notes for {sym} so they match the LAYOUT TEMPLATE exactly: same section order,
same headings, same line formats (BEHAVIOR READ, Float/Shares, Reverse splits, Gap History, Premarket,
Open -> HOD, Recent news, SEC section, Open dilution, Dilution Dashboard, Completed dilution,
Remaining potential dilution, Administrative, Estimated cash, DILUTION RISK block).
Rules:
- Copy the template's style, spacing, abbreviations and units (IST times, +NN%, $X.XXXM).
- Every number, date and name must come from VERIFIED CLAIMS or the PYTHON FACTS. Never from memory.
- Where the template shows a value that no verified claim or Python fact supports, write "unverified".
- If verified claims show events/warrants/convertibles the template lacks, add them in the right section.
- If a verified claim contradicts the template, use the claim and append " (differs from automated parse)".
- Do not change the DILUTION RISK score or grade; copy that block from the template unchanged.
- Plain text only, no markdown.

LAYOUT TEMPLATE (current output):
{template}

PYTHON FACTS:
{py}

VERIFIED CLAIMS:
{claims}"""


# ---------------------------------------------------------------- main
def ai_notes(sym, sources, py_facts, template):
    """sources: {"S1": {"form","date","url","text"}}; py_facts/template: your current notes text."""
    src = render_sources(sources)

    print(f"  [gemini] Stage 1/4: Analyst ({FAST}) analyzing {len(sources)} sources...")
    claims = Claims(**json.loads(call(FAST, ANALYST.format(py=py_facts, src=src), Claims))).claims
    print(f"  [gemini] Analyst extracted {len(claims)} claims.")

    passed = [x for x in claims if py_check(x, sources, py_facts)]
    dropped = [x for x in claims if x not in passed]
    print(f"  [gemini] Stage 2/4: Python quote check passed {len(passed)}/{len(claims)} claims.")

    print(f"  [gemini] Stage 3/4: Auditor ({STRONG}) auditing claims & missed events...")
    audit = Audit(**json.loads(call(
        STRONG,
        AUDITOR.format(claims=json.dumps([x.model_dump() for x in passed]), py=py_facts, src=src),
        Audit)))
    verdict = {v.id: v for v in audit.verdicts}

    good = []
    unverified = [f"{x.statement} (quote not found in {x.source_id})" for x in dropped]
    for x in passed:
        v = verdict.get(x.id)
        if v and v.verdict == "supported":
            good.append(x)
        elif v and v.verdict == "contradicted":
            unverified.append(f"{x.statement} -> auditor says: {v.correction}")
        else:
            unverified.append(f"{x.statement} (auditor could not confirm)")

    print(f"  [gemini] Auditor confirmed {len(good)}/{len(passed)} claims.")
    good_json = json.dumps([x.model_dump() for x in good])

    print(f"  [gemini] Stage 4/4: Writer ({STRONG}) drafting verified notes...")
    body = call(STRONG, WRITER.format(sym=sym, template=template, py=py_facts, claims=good_json))
    body, bad = number_guard(body, py_facts + good_json)
    print(f"  [gemini] Pipeline complete ({bad} line(s) flagged).")

    tail = [f"\n--- VERIFICATION: {len(good)}/{len(claims)} claims verified; {bad} line(s) flagged ---"]
    if unverified:
        tail += ["NOT VERIFIED (do not rely on):"] + [f"  - {u}" for u in unverified]
    if audit.missed_events:
        tail += ["POSSIBLY MISSED (check manually):"] + [f"  - {m}" for m in audit.missed_events]
    return body.strip() + "\n" + "\n".join(tail)
