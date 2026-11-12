@echo off
setlocal
if /i not "%~1"=="min" (
  start "PhoneFilter-Start" /min cmd /c ""%~f0" min"
  exit /b
)

cd /d "%~dp0"

docker start PhoneFilter_Engine >nul 2>&1
if errorlevel 1 (
  call :popup "Docker Desktop is not running, or container PhoneFilter_Engine does not exist."
  exit /b 1
)

set /a tries=0
:wait_clickhouse
docker exec PhoneFilter_Engine clickhouse-client --query "SELECT 1" >nul 2>&1
if not errorlevel 1 goto start_api
set /a tries+=1
if %tries% geq 30 (
  call :popup "ClickHouse container started, but it was not ready within 30 seconds."
  exit /b 1
)
ping 127.0.0.1 -n 2 >nul
goto wait_clickhouse

:start_api
netstat -ano | findstr /C:":8000 " | findstr "LISTENING" >nul
if not errorlevel 1 goto open_browser

start "PhoneFilter-API" /min /D "%~dp0" cmd /c python -m uvicorn server:app --host 127.0.0.1 --port 8000

set /a tries=0
:wait_api
netstat -ano | findstr /C:":8000 " | findstr "LISTENING" >nul
if not errorlevel 1 goto open_browser
set /a tries+=1
if %tries% geq 30 (
  call :popup "FastAPI did not listen on port 8000. Open the minimized PhoneFilter-API window for the error."
  exit /b 1
)
ping 127.0.0.1 -n 2 >nul
goto wait_api

:open_browser
start "" "http://127.0.0.1:8000/"
exit /b 0

:popup
powershell -NoProfile -WindowStyle Hidden -Command "Add-Type -AssemblyName System.Windows.Forms; [System.Windows.Forms.MessageBox]::Show('%~1','PhoneFilter')"
exit /b 0
