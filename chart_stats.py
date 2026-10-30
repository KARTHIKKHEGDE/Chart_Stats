#!/usr/bin/env python3
"""Backward-compatible WSGI and local-run entry point."""
from chart_app.application import app, main

__all__ = ["app", "main"]

if __name__ == "__main__":
    main()
