"""Convenience entry point for Dilution Notes.
Run:
    python app.py
"""
import os
import sys

_this_dir = os.path.dirname(os.path.abspath(__file__))
if _this_dir not in sys.path:
    sys.path.insert(0, _this_dir)

from dilution_notes import app, main

if __name__ == "__main__":
    main()
