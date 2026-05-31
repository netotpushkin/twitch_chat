@echo off
setlocal

set "PORT=5005"
set "OBS_EXE=C:\Program Files\obs-studio\bin\64bit\obs64.exe"
set "OBS_DIR=C:\Program Files\obs-studio\bin\64bit"

cd /d "%~dp0"

start "twitch_chat bot" cmd /k python bot.py

echo Waiting for overlay server on port %PORT% ...
:wait
powershell -NoProfile -Command "try { $c=New-Object Net.Sockets.TcpClient; $c.Connect('localhost',%PORT%); $c.Close(); exit 0 } catch { exit 1 }" >nul 2>&1
if errorlevel 1 (
    timeout /t 1 /nobreak >nul
    goto wait
)

echo Overlay server is up, starting OBS...
cd /d "%OBS_DIR%"
start "" "%OBS_EXE%"

endlocal
