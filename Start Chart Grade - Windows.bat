@echo off
setlocal
cd /d "%~dp0"
set "LOCAL_MAX=1"
set "PORT=8080"
set "FETCH_WORKERS_CAP=100"
set "ANALYZE_WORKERS=32"
set "REPORT_WORKERS=8"
set "SPIKE_WORKERS=50"
set "WARMUP_WORKERS=8"
set "SERVER_THREADS=50"
set "MAX_EVENTS=50"
set "OPTIONAL_WAIT=1.25"
set "POLYGON_RPM=5"
set "YAHOO_REQUEST_LIMIT=4"

if not exist ".env" (
  copy "env.example" ".env" >nul
  echo A new .env file was created. Add your Polygon keys and SEC contact identity, save, and close Notepad.
  notepad ".env"
)

if not exist ".venv\Scripts\python.exe" py -3 -m venv .venv
if errorlevel 1 goto failed
call ".venv\Scripts\activate.bat"
python -m pip install -r requirements.txt
if errorlevel 1 goto failed
python scripts\local_dev_server.py 8080
goto done

:failed
echo Local setup failed. Check Python 3 and your internet connection, then try again.
pause
:done
endlocal
