@echo off
setlocal

set "ROOT=%~dp0"
cd /d "%ROOT%"
powershell -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%ROOT%platform-control.ps1" start
exit /b %errorlevel%
