# Chart and Analysis Behavior Requirements

This note records the requested behavior for the Gap-Up and Deepdive charts and the related AI analysis. Screenshots shared in chat illustrate the reported issues; this file records the user's written requests.

## Shared chart experience

- Use one shared chart renderer and shared interaction/readout code for Gap-Up charts and Deepdive. A chart improvement should apply to both views without duplicating the implementation.
- Keep chart controls and behavior consistent between views, including interval selection, zoom, pan, reset, indicators, anchored VWAP, range measurement, navigator, hover information, and available event markers.
- Keep the charts readable at normal screen size. Put hover details in a legible strip beneath the chart alongside the date and time, in IST. Show candle open, high, low, close, and volume (OHLCV) there; do not use a small floating OHLCV popup.
- Do not add unwanted Previous Spike / Next Spike navigation or a manual Sessions selector.

## Event windows and historical comparisons

- When a gap-up or premarket-spike event is selected, load the event session and up to two trading sessions before and after it by default. Keep the selected event centered in the range. Use the same default in Gap-Up and Deepdive.
- Begin loading the initial five-session context as soon as the chart enters its lazy-load window; opening the expanded chart should reuse the completed or in-flight load.
- If fewer than five sessions exist in the available data, show the sessions that are available and make the shortfall clear.
- Load additional sessions automatically as the user zooms out or pans to the edge of the loaded range. Keep the current event and viewport stable while extending the data. Apply the same lazy-loading behavior to both chart views.
- Gap-Up chart candles and daily history must come from Polygon only. Deepdive may use bundled intraday JSON only for the selected event and its two trading sessions on either side; if a seed session is missing, fetch that session from Polygon. All sessions beyond that five-session window come from Polygon.
- Compare the current session with historical premarket-spike sessions for that ticker. Use the historical spike sessions as the comparison set; do not limit the comparison to the event day or to regular-hours-only candles.
- Keep the historical comparison ticker-specific. Data for one ticker or event must never appear in another ticker's chart.

## Premarket VWAP statistics

- Treat the spike as beginning in premarket. Calculate premarket high versus cumulative VWAP using premarket trading beginning at 04:00 ET, including the VWAP value at the time of the premarket high.
- Report historical results as a distribution, including the median and middle 50% range (25th–75th percentile), with per-session values when available. Use the distribution to help the user judge whether a current premium such as +21% or +23% is typical for the ticker's historical spike sessions.
- Keep premarket and regular-hours statistics distinguishable so a regular-hours-only VWAP measure is not presented as the premarket spike statistic.

## Ticker analysis, background work, and reuse

- Start preparing the ticker's AI summary and dilution analysis as soon as the ticker is entered/selected, so results are ready when the user opens them. Run independent data collection in parallel and show completed sections as soon as they are ready.
- Closing an analysis popup must not cancel its background work. Cache completed results and show them when the user reopens the popup.
- The All-in-One Summary and Dilution History use the same ticker's dilution/SEC-filing data. Fetch and normalize it once, then reuse it in both views rather than doing duplicate work.
- Include relevant SEC filings already available to the app in the AI analysis context. Do not report filings as unavailable when they have been collected; if no relevant filing data is actually available, state that accurately.
- Cache keys must include the ticker and the relevant event date/as-of date and timeframe. Reuse cached or in-flight work for the same ticker and context; never reuse another ticker's result.

## Readability

- Make analysis popups larger and easier to read, with larger text, adequate spacing, and scrolling for long results.
- Keep chart hover data in the bottom date/time strip so it remains visible and does not obscure candles.

## Completion checks

- Gap-Up and Deepdive use the same chart rendering and hover behavior.
- Both show timestamp and OHLCV beneath the chart.
- Both load the event plus up to two neighboring trading sessions by default, with further context loaded on zoom-out/pan and no sessions dropdown.
- Current charts can be compared with that ticker's historical premarket-spike sessions.
- Premarket high-versus-VWAP statistics use the premarket window and provide a historical median and 25th–75th percentile range.
- Ticker analysis starts in the background, survives popup close, reuses dilution/filing data, and keeps each ticker's cache isolated.
