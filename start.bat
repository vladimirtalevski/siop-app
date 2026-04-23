@echo off
echo ==========================================
echo   SIOP Manager - MotherDuck Mode
echo ==========================================
echo.

:: Kill old instances on ports 8000 and 3000 only
echo Stopping any running instances...
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":8000 " 2^>nul') do taskkill /F /PID %%a 2>nul
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":3000 " 2^>nul') do taskkill /F /PID %%a 2>nul
timeout /t 2 /nobreak >nul

echo [1/2] Starting FastAPI backend (MotherDuck - no SSO needed)...
echo.
cd /d "%~dp0backend"
start "SIOP Backend" cmd /k "python3 -m uvicorn main:app --host 0.0.0.0 --port 8000"

echo [2/2] Starting React frontend...
timeout /t 3 /nobreak >nul
cd /d "%~dp0frontend"
start "SIOP Frontend" cmd /k "npm run dev -- --port 3000"

echo.
echo ==========================================
echo   STEPS:
echo   1. Watch the "SIOP Backend" terminal
echo   2. Wait for: "MotherDuck connected"
echo   3. Open http://localhost:3000
echo   No SSO login required!
echo ==========================================
echo.
pause
