@echo off
setlocal
cd /d C:\Services
.\venv\Scripts\python.exe -m uvicorn app:app --host 0.0.0.0 --port 8000 --workers 2 1>>logs\api.out.log 2>>logs\api.err.log
