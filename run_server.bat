@echo off
cd /d "%~dp0"
server.exe >> "%~dp0logs\server.log" 2>&1
