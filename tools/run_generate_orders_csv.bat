@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%") do set "SCRIPT_DIR=%%~fI"
if "%SCRIPT_DIR:~-1%"=="\" set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"

if not defined PYTHON_EXE set "PYTHON_EXE=python"

set "PROJECT_ROOT=%SCRIPT_DIR%\.."
for %%I in ("%PROJECT_ROOT%") do set "PROJECT_ROOT=%%~fI"
cd /d "%PROJECT_ROOT%"

set "FILTER_ARGS="
if not "%~1"=="" set "FILTER_ARGS=--include %*"

"%PYTHON_EXE%" "%SCRIPT_DIR%\generate_targets.py" %FILTER_ARGS%
if errorlevel 1 exit /b %ERRORLEVEL%

if not exist "%SCRIPT_DIR%\..\output\targets_prev.json" (
  echo prev targets not found: "%SCRIPT_DIR%\..\output\targets_prev.json"
  echo using --allow-initial (treat prev target as 0)
  "%PYTHON_EXE%" "%SCRIPT_DIR%\generate_orders_csv.py" --algo BestLimitAlgo --min-volume 1 --max-volume 1 --allow-initial %FILTER_ARGS%
  if errorlevel 1 exit /b %ERRORLEVEL%
  if not exist "%PROJECT_ROOT%\output\algotrading_TwapAlgo_latest.csv" (
    echo ERROR: expected output not found: "%PROJECT_ROOT%\output\algotrading_BestLimitAlgo_latest.csv"
    exit /b 1
  )
  exit /b 0
)

"%PYTHON_EXE%" "%SCRIPT_DIR%\generate_orders_csv.py" --algo BestLimitAlgo --min-volume 1 --max-volume 1 %FILTER_ARGS%
if errorlevel 1 exit /b %ERRORLEVEL%
if not exist "%PROJECT_ROOT%\output\algotrading_BestLimitAlgo_latest.csv" (
  echo ERROR: expected output not found: "%PROJECT_ROOT%\output\algotrading_BestLimitAlgo_latest.csv"
  exit /b 1
)
exit /b 0
