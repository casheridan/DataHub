@echo off
echo ===================================================
echo Starting Reel Viewer Update Process...
echo Timestamp: %date% %time%
echo ===================================================

:: This command changes the directory to where the script is located.
cd /d "%~dp0"

echo.
echo [Step 1/2] Running the Python script to update the local database...
python main.py

:: Check if the python script ran successfully before deploying
if %errorlevel% neq 0 (
    echo.
    echo [ERROR] The Python script failed to run. Aborting deployment.
    pause
    exit /b %errorlevel%
)

echo.
echo [Step 2/2] Deploying the updated files to Vercel...
:: Using 'npx vercel' is more robust as it doesn't rely on the system PATH.
npx vercel --prod

echo.
echo ===================================================
echo Update and deployment process finished.
echo ===================================================
echo.
pause

