@echo off
cd /d "%~dp0"
where py >nul 2>&1
if not errorlevel 1 (
    py -3 server.py
    if not errorlevel 1 exit /b 0
    pause
    exit /b 1
)
where python >nul 2>&1
if errorlevel 1 (
    echo Python 3 is required. Install it, then run this file again.
    pause
    exit /b 1
)
python server.py
if errorlevel 1 pause
