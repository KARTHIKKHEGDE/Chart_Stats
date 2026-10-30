#!/usr/bin/env python3
"""Standalone Flask entry point for Rally and First Red Day statistics."""
import os
from pathlib import Path

from flask import Flask, Response, jsonify, request

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parent / ".env")
    load_dotenv()
except ImportError:
    pass

# Import after dotenv so the analysis module sees the local API key.
from chart_app.runner import analyze

PAGE = (Path(__file__).resolve().parent / "templates" / "runner_stats.html").read_text(encoding="utf-8")
app = Flask(__name__)


@app.route("/api/analyze")
def api():
    ticker = (request.args.get("ticker") or "").strip().upper()
    if not ticker:
        return jsonify(error="ticker required"), 400
    try:
        return jsonify(
            analyze(
                ticker,
                request.args.get("start") or None,
                request.args.get("frd") or None,
                request.args.get("float_m") or None,
            )
        )
    except Exception as exc:
        return jsonify(error=str(exc)), 500


@app.route("/")
def index():
    return Response(PAGE, mimetype="text/html")


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=int(os.getenv("PORT", "5055")), debug=False)
