@echo off
echo Starting SimEngine...

echo [1/3] Starting Docker containers (Postgres + Redis)...
cd /d "D:\JOB ENGINE\simulation-engine"
docker compose up -d
if %errorlevel% neq 0 (
    echo ERROR: Docker failed. Is Docker Desktop running?
    pause
    exit /b 1
)

echo Waiting for Postgres and Redis to be healthy...
timeout /t 5 /nobreak >nul

echo [2/3] Starting FastAPI backend...
start "SimEngine API" cmd /k "cd /d "D:\JOB ENGINE\simulation-engine" && "D:\JOB ENGINE\.venv\Scripts\python.exe" -m uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload"

echo Waiting for API to boot...
timeout /t 3 /nobreak >nul

echo [3/3] Starting Worker...
start "SimEngine Worker" cmd /k "cd /d "D:\JOB ENGINE\simulation-engine" && "D:\JOB ENGINE\.venv\Scripts\python.exe" scripts/run_worker.py"

echo.
echo All services started!
echo   API    → http://localhost:8000
echo   Docs   → http://localhost:8000/docs
echo   Dashboard → http://localhost:5173
echo.
echo Run the dashboard with:  cd "D:\JOB ENGINE\dashboard" ^&^& npm run dev
pause
