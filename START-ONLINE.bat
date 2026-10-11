@echo off
cd /d "%~dp0"
echo =============================================
echo   ULTIMATE SOCCER - ONLINE HUB SERVER
echo =============================================
where node >nul 2>nul
if errorlevel 1 (
  echo Node.js is not installed or is missing from PATH.
  echo Install Node.js 18 or newer from https://nodejs.org/
  pause
  exit /b 1
)
node --version
echo Starting the online hub on port 8787...
echo Keep this window open while playing.
echo Then open http://localhost:8787 in your browser.
echo =============================================
npm start
pause
