@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%") do set "SCRIPT_DIR=%%~fI"
if "%SCRIPT_DIR:~-1%"=="\" set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"

if not defined PYTHON_EXE set "PYTHON_EXE=python"

cd /d "%SCRIPT_DIR%"

"%PYTHON_EXE%" "%SCRIPT_DIR%\generate_trs_targets.py"
exit /b %ERRORLEVEL%
