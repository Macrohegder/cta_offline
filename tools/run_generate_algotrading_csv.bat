@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%") do set "SCRIPT_DIR=%%~fI"
if "%SCRIPT_DIR:~-1%"=="\" set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"

if not defined PYTHON_EXE set "PYTHON_EXE=python"

cd /d "%SCRIPT_DIR%"

"%PYTHON_EXE%" "%SCRIPT_DIR%\generate_trs_targets.py"
if errorlevel 1 exit /b %ERRORLEVEL%

if not exist "%SCRIPT_DIR%\..\output\targets_prev.json" (
  echo prev targets not found: "%SCRIPT_DIR%\..\output\targets_prev.json"
  echo please run generate_targets.py at least twice, or use --allow-initial for the first run
  exit /b 2
)

"%PYTHON_EXE%" "%SCRIPT_DIR%\generate_algotrading_csv.py" --algo TwapAlgo
exit /b %ERRORLEVEL%
