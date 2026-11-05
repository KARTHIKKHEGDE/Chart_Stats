"""Process-wide Yahoo Finance request limit shared by chart and scan features."""
import os
from threading import BoundedSemaphore

# Yahoo throttles by IP. Keep local max-parallel bursts conservative, even when
# Polygon concurrency is high. Render retains its prior shared cap.
YAHOO_REQUEST_LIMIT = int(os.getenv("YAHOO_REQUEST_LIMIT", "4"))
YAHOO_REQUEST_SEMAPHORE = BoundedSemaphore(YAHOO_REQUEST_LIMIT)
