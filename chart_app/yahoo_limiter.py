"""Process-wide Yahoo Finance request limit shared by chart and scan features."""
from threading import BoundedSemaphore

# Keeps aggregate Yahoo activity bounded even when multiple users open reports.
YAHOO_REQUEST_LIMIT = 16
YAHOO_REQUEST_SEMAPHORE = BoundedSemaphore(YAHOO_REQUEST_LIMIT)
