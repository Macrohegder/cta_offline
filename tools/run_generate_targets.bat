@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%") do set "SCRIPT_DIR=%%~fI"
if "%SCRIPT_DIR:~-1%"=="\" set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"

if not defined PYTHON_EXE set "PYTHON_EXE=python"

set "PROJECT_ROOT=%SCRIPT_DIR%\.."
for %%I in ("%PROJECT_ROOT%") do set "PROJECT_ROOT=%%~fI"
cd /d "%PROJECT_ROOT%"

if "%~1"=="" (
  "%PYTHON_EXE%" "%SCRIPT_DIR%\generate_targets.py"
) else (
  "%PYTHON_EXE%" "%SCRIPT_DIR%\generate_targets.py" --include %*
)
exit /b %ERRORLEVEL%
