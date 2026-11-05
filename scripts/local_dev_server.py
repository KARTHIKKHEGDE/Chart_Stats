#!/usr/bin/env python3
"""Run the local Waitress server and restart it when app files change."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import time
import webbrowser


ROOT = Path(__file__).resolve().parent.parent
WATCH_DIRS = (ROOT / "chart_app", ROOT / "templates", ROOT / "static")
WATCH_ROOT_FILES = {ROOT / "chart_stats.py", ROOT / "runner_stats.py", ROOT / ".env"}
WATCH_SUFFIXES = {".py", ".html", ".js", ".css"}
IGNORED_DIRS = {".git", ".venv", "venv", "__pycache__", ".gapcache"}
MARKER = ROOT / ".local_reload_version"


def snapshot() -> dict[str, tuple[int, int]]:
    files = set(WATCH_ROOT_FILES)
    for directory in WATCH_DIRS:
        if not directory.exists():
            continue
        for current, subdirs, names in os.walk(directory):
            subdirs[:] = [name for name in subdirs if name not in IGNORED_DIRS]
            for name in names:
                path = Path(current) / name
                if path.suffix.lower() in WATCH_SUFFIXES:
                    files.add(path)

    result = {}
    for path in files:
        try:
            stat = path.stat()
            result[str(path)] = (stat.st_mtime_ns, stat.st_size)
        except OSError:
            continue
    return result


def start_server(port: int) -> subprocess.Popen:
    environment = os.environ.copy()
    environment.update({
        "LOCAL_MAX": "1",
        "LOCAL_AUTO_RELOAD": "1",
        "LOCAL_NO_BROWSER": "1",
        "PORT": str(port),
    })
    return subprocess.Popen(
        [sys.executable, str(ROOT / "chart_stats.py"), str(port)],
        cwd=ROOT,
        env=environment,
    )


def stop_server(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=8)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def main() -> int:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else int(os.getenv("PORT", "8080"))
    MARKER.write_text(str(time.time_ns()), encoding="utf-8")
    current = snapshot()
    server = start_server(port)
    print(f"Watching app code for changes. Local server: http://127.0.0.1:{port}", flush=True)
    print("Save a Python, template, CSS, or JavaScript file to restart the server and refresh open app tabs.", flush=True)
    time.sleep(1.2)
    webbrowser.open(f"http://127.0.0.1:{port}")

    try:
        while True:
            time.sleep(0.65)
            updated = snapshot()
            if updated == current:
                if server.poll() is not None:
                    print(f"Server exited with code {server.returncode}; restarting in 2 seconds.", flush=True)
                    time.sleep(2)
                    server = start_server(port)
                continue

            time.sleep(0.55)  # debounce editors that save by replacing a temporary file
            updated = snapshot()
            if updated == current:
                continue
            changed = sorted(set(current) | set(updated))
            changed = [path for path in changed if current.get(path) != updated.get(path)]
            print(f"\nDetected {len(changed)} changed app file(s); restarting local server…", flush=True)
            MARKER.write_text(str(time.time_ns()), encoding="utf-8")
            stop_server(server)
            server = start_server(port)
            current = updated
    except KeyboardInterrupt:
        print("\nStopping local server…", flush=True)
        stop_server(server)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
