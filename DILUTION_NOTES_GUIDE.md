# Ticker Notes Project Guide

## Purpose

Ticker Notes is a small Flask web app for entering a stock ticker and receiving a plain-text research summary. It combines market data, SEC filing data, dilution analysis, cash/runway analysis, and recent-news tagging.

Run it with:

```text
python app.py
```

Then open:

```text
http://127.0.0.1:5000/
```

The page has:

- A ticker input
- A **Get notes** button
- A **Copy all** button that copies the complete report

## Main request flow

1. The browser requests `/api/notes?symbol=BKYI`.
2. The ticker is trimmed, uppercased, and checked to contain only letters and numbers.
3. `price_section()` gathers market, split, cash, runway, gap, HOD, VWAP, and news information.
4. `sec_section()` identifies the issuer, reads SEC filing metadata, analyzes dilution, and appends the risk score.
5. The API returns one JSON object:

```json
{
  "text": "complete plain-text report"
}
```

Errors are returned as an HTTP 400 or 500 JSON response.

## Data sources

### Yahoo Finance through `yfinance`

Used for:

- Float shares
- Shares outstanding fallback
- Historical daily OHLCV prices
- Reverse splits
- Recent news
- Fallback balance-sheet and cash-flow data
- Recent 5-minute intraday bars

Yahoo intraday data is limited for older dates, so older HOD/VWAP requests use Polygon when configured.

### SEC EDGAR

Used for:

- SEC ticker-to-CIK mapping
- Filing history and filing dates
- Primary filing documents
- Company facts/XBRL
- Cash balances
- Operating cash flow
- Warrants and exercise prices
- S-1/F-1 and related prospectus analysis
- Recent dilution filings

SEC discovery combines the complete submission index, including archived submission files,
instead of relying only on `filings.recent`. Relevant filing directories are also inspected
so the primary document and discoverable HTML, XML, and TXT exhibits are searched together.
Amendments such as `6-K/A`, `8-K/A`, `10-Q/A`, `10-K/A`, `20-F/A`, and registration-statement
amendments remain separate evidence records.

The SEC requests use the `SEC_HEADERS` User-Agent. A descriptive contact address should be used in production.

### Polygon/Massive-compatible aggregates API

Used for historical one-minute bars when `POLYGON_API_KEY` or `MASSIVE_API_KEY` is configured.

The request is:

```text
https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{day}/{day}
```

The app:

- Restricts bars to regular market hours, 09:30–15:59 Eastern Time. HOD is therefore explicitly a regular-session HOD; pre-market highs are not included.
- Converts timestamps to `America/New_York`
- Caches completed daily bars under `.cache/`
- Uses `POLYGON_MIN_INTERVAL` between requests
- Falls back to Yahoo 5-minute data when Polygon has no usable result

## Local configuration

The project-local `.env` contains API settings and is excluded by `.gitignore`.

Supported keys include:

```text
ALPACA_API_KEY
ALPACA_API_SECRET
DATA_PROVIDER
ALPACA_FEED
POLYGON_API_KEY
MASSIVE_API_KEY
POLYGON_MIN_INTERVAL
```

The current app uses Yahoo, SEC, and Polygon/Massive-compatible data paths. The Alpaca values are stored for future provider integration but are not currently used by the request calculations.

## Price and gap analysis

The app downloads one year of daily prices and calculates:

```text
gap = today's open / previous close - 1
HOD return = day's high / day's open - 1
close return = day's close / day's open - 1
```

The primary gap-up threshold is **30% or more**:

```text
GAP_MIN = 0.30
```

The report displays the latest five qualifying gap-ups, including:

- Date
- Gap percentage
- HOD percentage above the open
- Time of HOD when intraday data is available
- Close versus open
- Close relative to intraday VWAP

A gap is considered faded when its close is below its open.

The report also includes secondary statistics for:

- Last 30 sessions
- Last 90 sessions
- Smaller gap days of at least 5%
- High-volume days

For older dates where no intraday bars are available, the report says `unavailable` rather than presenting a false HOD time or VWAP result.

## SEC issuer handling

The app detects the reporting style from filing forms.

### Domestic issuers

Domestic paths include:

- 8-K and 8-K/A
- 10-K and 10-K/A
- 10-Q and 10-Q/A
- S-1, S-3, and amendments
- 424B prospectus supplements

Recent securities issuance checks use 8-K Item 3.02 when available.

### Foreign private issuers

Foreign issuer paths include:

- 6-K
- 20-F and 20-F/A
- 40-F and 40-F/A for Canadian foreign issuers
- F-1 and F-1/A
- F-3 and F-3/A
- F-10 and F-10/A
- 424F

Recent securities checks use relevant 6-K, 20-F, 20-F/A, 40-F, and 40-F/A filings instead of assuming that Form 8-K exists.

The report explicitly identifies the issuer as either:

```text
Issuer type: domestic issuer
```

or:

```text
Issuer type: foreign private issuer
```

## Open dilution analysis

The app first selects the newest available prospectus or registration statement within its supported forms.

It extracts:

- Shares outstanding before the offering
- Warrants
- Pre-funded warrants
- Exercise prices
- Stockholder approval requirements
- Warrant overhang
- Estimated post-exercise shares
- Dilution as a percentage of the post-exercise total

For example:

```text
Total potential warrants = all unique warrant/share quantities parsed
Warrant overhang = total potential warrant shares / current shares outstanding
Post-exercise shares = current shares outstanding + total potential warrant shares
Post-exercise dilution = total potential warrant shares / post-exercise shares
```

The parser prevents a headline warrant quantity from being counted again when the same quantity appears in the detailed prospectus table.

All SEC filing, exhibit, amendment, prospectus, and XBRL warrant scans use a three-year
lookback. This keeps each request bounded and makes the coverage window consistent across
cash, shares, dilution, financing, and exchange-status evidence.

Pre-funded warrants are treated as base shares rather than ordinary future dilution. Ordinary warrant overhang is calculated against the current base shares, including pre-funded shares. Warrant deduplication uses quantity, exercise price, and series/description rather than quantity alone.

The denominator prefers the latest SEC-reported shares outstanding when available, instead of relying only on Yahoo or an old prospectus cover.

If no current overhang can be parsed, the report says:

```text
No current warrant/share overhang was parsed from the latest prospectus.
```

## Financial players

Recent dilution filings are scanned for offering participants, including:

- Underwriters
- Placement agents
- Selling agents
- Sales agents
- Investment banks
- Bookrunners
- Financial advisors

The parser prioritizes sentences that explicitly identify a firm as acting as, serving as, or being appointed/engaged/retained in that role.

Examples include:

```text
Maxim Group LLC acted as the exclusive placement agent
```

or:

```text
Maxim Group LLC and Rodman & Renshaw LLC acted as sales agents
```

Generic mentions such as someone having previously worked in investment banking are filtered out where possible.

## Last 12 months dilution

The report searches recent issuer-appropriate filings and only keeps filings whose text contains dilution-related evidence such as:

- Shares
- Warrants
- Convertible securities
- Offerings
- Placements
- Registered direct transactions

For each selected filing it displays:

- Filing date
- Form
- Item or issuer-report classification
- Named financial players
- Complete extracted evidence sentences

Sentence character truncation was removed so evidence is not cut in the middle.

The report limits the number of displayed filings to keep responses manageable.

## Cash position and runway

### Latest confirmed cash

SEC XBRL is authoritative when available. The app searches recent balance-sheet facts such as:

- `CashAndCashEquivalentsAtCarryingValue`
- `CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents`

It selects the most recent qualifying 10-Q, 10-K, 20-F, or amendment fact.

For BKYI, this produces:

```text
Latest confirmed SEC cash: $1.379M
30 Jun 2026, Form 10-Q
```

If a foreign private issuer reports newer financial statements inside a 6-K exhibit
that is not represented in companyfacts XBRL, the app parses that exhibit and prefers
its dated cash balance over older XBRL or Yahoo data. Yahoo balance-sheet data is only
a fallback when no newer SEC source is available.

Cash and financial evidence is reconciled by the statement/reporting date, while financing
and other event evidence is reconciled by the event or closing date. A filing date alone
does not make an offering capacity, an unclosed transaction, or a stale balance-sheet
figure current.

### Reported operating cash burn

The app reads SEC XBRL operating cash flow facts such as:

- `NetCashProvidedByUsedInOperatingActivities`
- `NetCashProvidedByUsedInOperatingActivitiesContinuingOperations`

For foreign private issuers, the same metric is also extracted from a newer dated
6-K financial-statement exhibit when companyfacts does not contain the filing.

Negative operating cash flow is converted to a monthly rate using the actual reported period:

```text
monthly burn = absolute operating cash outflow / reported period months
```

For a six-month filing:

```text
$1.204M / 6 = approximately $0.201M/month
```

The report identifies the source period, for example:

```text
Reported operating cash burn: $0.201M/month
(2026-01-01 to 2026-06-30, 10-Q)
```

### Financing after the latest cash report

The app scans post-report event filings for actual proceeds, including:

- Gross proceeds
- Net proceeds
- Proceeds received
- Amounts raised

It excludes language describing unused capacity, such as:

- May
- Up to
- Potential
- Capacity

Only event filings are added as post-report financing:

- 8-K
- 6-K

Periodic reports are not added as new financing because they can repeat financing already included in the later balance sheet or cash-flow statement.

### Estimated cash today

When the required data exists, the app calculates:

```text
estimated cash today =
latest reported cash
+ parsed post-report gross proceeds less an 8% fee/cost allowance
- estimated post-report burn
```

Example:

```text
$1.379M
+ estimated net post-report proceeds
- estimated post-report burn
= $3.131M estimated cash today
```

The filing does **not** directly report the burn between the latest cash
report date and today. The app estimates that missing interval using:

```text
estimated post-report burn =
reported monthly operating burn
× months elapsed since the cash report date
```

For example:

```text
$0.201M/month × 3.78 months = $0.760M estimated post-report burn
```

The monthly burn itself comes from a public SEC cash-flow period. Only the
post-report extrapolation is estimated. It assumes the latest reported burn
rate continued unchanged; actual burn may differ.

The app applies an 8% planning allowance for fees and offering costs when converting parsed gross proceeds to estimated net proceeds. This is a planning assumption, not a filing-confirmed expense figure, and is not a substitute for the issuer's final cash-flow statement.

The report warns that proceeds are not the same as cash still held because it may not know:

- Offering fees
- Legal and underwriting expenses
- Debt repayments
- Other investing or financing cash uses
- Cash raised outside the scanned filing set

Therefore the current cash result is labeled estimated, not confirmed.

### Runway

```text
estimated runway =
estimated cash today / reported monthly operating burn
```

If post-report financing cannot be parsed, the report uses the latest confirmed cash and clearly marks current runway as unverified. If operating cash flow is positive, runway is undefined and receives 0.0/3.5 cash-runway points rather than being treated as infinite.

## Dilution-risk score

The report ends with a score out of 10 using three categories:

| Category | Maximum |
|---|---:|
| Cash runway | 3.5 |
| Historical dilution | 3.5 |
| Gap-and-fade history | 3.0 |
| **Total** | **10.0** |

### Cash runway points

| Runway | Points |
|---|---:|
| Under 3 months | 3.5 |
| 3 to under 5 months | 3.0 |
| 5 to under 8 months | 2.2 |
| 8 to under 12 months | 1.4 |
| 12 to under 24 months | 0.6 |
| 24 months or more | 0 |

### Historical dilution points

The score considers:

- Number of recent dilution filings
- Current warrant/share overhang
- Unique offering mechanisms
- Reverse splits

Mechanisms are deduplicated by type, including:

- ATM
- Convertible
- Warrant inducement
- Registered direct
- Private placement
- Underwritten offering

### Gap-and-fade points

The score considers:

- Fraction of 30%+ gap-ups that faded below the open
- Average close-versus-open return

The report applies the normal gap-and-fade calculation to every available qualifying gap-up, including a single observation. It does not reduce points or add a limited-confidence label based on sample size. The report still shows the number of qualifying gap-ups used so the result remains transparent.

### Grades

| Score | Grade | Risk |
|---:|:---:|---|
| `[8, 10]` | A | Extreme |
| `[6, 8)` | B | High |
| `[4, 6)` | C | Moderate |
| `[2, 4)` | D | Low |
| `[0, 2)` | E | Very low |

The upper endpoint is included only for Grade A; every lower grade uses a
half-open interval, so scores such as 7.95 have an unambiguous grade.

## Caching and request state

- Polygon daily bars are cached under `.cache/`.
- `.cache/`, `.env`, `__pycache__/`, and `.pyc` files are ignored by `.gitignore`.
- Risk and shares-outstanding state is request-local through Flask `g`, preventing concurrent ticker requests from mixing values.

## Known limitations

- SEC filings may be unavailable temporarily because of rate limits, network errors, or filing changes.
- Text parsing is heuristic and cannot replace reading the complete filing.
- Gross financing proceeds do not prove that the proceeds remain in cash.
- Current cash after the latest report can be estimated but is not confirmed unless a newer balance sheet is available.
- Cash burn can be positive in a reporting period; in that case no burn-based runway is calculated.
- Yahoo metadata may be stale, so SEC XBRL is preferred for cash and operating cash flow.
- Polygon historical intraday access depends on API entitlement and the configured key.
- `ALPACA_API_KEY` and `ALPACA_API_SECRET` are currently stored for configuration but are not used by the active data path.
- This tool is research software and is not investment advice.
