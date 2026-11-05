#!/bin/bash
set -e
cd "$(dirname "$0")"
export LOCAL_MAX=1
export PORT=8080

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
