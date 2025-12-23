@echo off
title Hail Money Map - Local Server

cd "%~dp0"

echo Starting server...
start "" python -m http.server 5500

timeout /t 2 >nul

echo Opening browser...
start "" "http://localhost:5500/index.html"

pause
