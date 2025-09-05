@echo off
echo ===================================================
echo Starting Analytics Data Update Process...
echo Timestamp: %date% %time%
echo ===================================================

:: This command changes the directory to where this script is located.
cd /d "%~dp0"

echo.
echo [Step 1/1] Running Python script to fetch data and push to Git...
.\venv\Scripts\python.exe push_data.py

:: Check if the python script ran successfully
if %errorlevel% neq 0 (
    echo.
    echo [ERROR] The Python script failed to run. Check logs for details.
    pause
    exit /b %errorlevel%
)

echo.
echo ===================================================
echo Analytics update process finished.
echo ===================================================
echo