# Chart Grade

Flask tools for gap-up fade analysis, SEC keyword event comparison, market scanning, and runner / first-red-day statistics.

## Run locally

Double-click `Start Chart Grade - Windows.bat` on Windows or `Start Chart Grade - Mac.command` on macOS. The first run creates a `.env` from `env.example`, installs dependencies, and starts the local server; add your own Polygon keys and SEC contact string to `.env` when prompted. Local runs use `LOCAL_MAX=1` and Waitress. The launchers watch Python files, templates, and static assets; saving one restarts the server and refreshes open app tabs automatically. To run manually without auto-reload, use `python chart_stats.py 8080`. Deployment keeps using `gunicorn chart_stats:app`.

To check the Polygon keys in `.env`, run `python check_keys.py`. The checker reports each key by number without displaying its value; it sends one request per unique key.

## Structure

- `chart_stats.py` — backwards-compatible WSGI and local-run entry point.
- `chart_app/application.py` — shared Flask app, configuration, and app-level endpoints.
- `chart_app/market_data.py` — Polygon/Yahoo data access, intraday bars, and VWAP/TWAP calculations.
- `chart_app/gap_analysis.py` — gap-up event analysis, grading, statistics, and report generation.
- `chart_app/auth.py` — authentication, account, and admin routes.
- `chart_app/scanner_settings.py` — scanner settings and UI visibility.
- `chart_app/charts.py` — session charts and chart-context endpoints.
- `chart_app/keyword_compare.py` — SEC keyword search, historical event matching, chart comparison, and forecasts.
- `chart_app/scanner.py` — scanner data collection and scan endpoint.
- `chart_app/runner.py` — runner and first-red-day analysis engine.
- `runner_stats.py` — standalone runner web entry point; it shares the analysis engine above.
- `templates/` — the gap-up, keyword-search, and runner pages.

Feature modules register routes on the shared application. Root-level cache and settings files remain in the project root.
