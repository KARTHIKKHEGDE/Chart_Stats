#!/bin/bash
set -e
cd "$(dirname "$0")"
export LOCAL_MAX=1
export PORT=8080
export FETCH_WORKERS_CAP=100
export ANALYZE_WORKERS=32
export REPORT_WORKERS=8
export SPIKE_WORKERS=50
export WARMUP_WORKERS=8
export SERVER_THREADS=50
export MAX_EVENTS=50
export OPTIONAL_WAIT=1.25
export POLYGON_RPM=5
export YAHOO_REQUEST_LIMIT=4

if [ ! -f .env ]; then
  cp env.example .env
  echo "A new .env file was created. Add your Polygon keys and SEC contact identity, save it, then press Return."
  open -t .env
  read -r
fi

if [ ! -x .venv/bin/python ]; then
  python3 -m venv .venv
fi
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python scripts/local_dev_server.py 8080
