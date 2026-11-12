@echo off
setlocal
if /i not "%~1"=="min" (
  start "PhoneFilter-Stop" /min cmd /c ""%~f0" min"
  exit /b
)

for /f "tokens=5" %%p in ('netstat -ano ^| findstr /C:":8000 " ^| findstr "LISTENING"') do (
  taskkill /PID %%p /T /F >nul 2>&1
)
taskkill /FI "WINDOWTITLE eq PhoneFilter-API" /T /F >nul 2>&1
exit /b 0
