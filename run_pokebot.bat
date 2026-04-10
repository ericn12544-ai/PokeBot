@echo off
setlocal EnableExtensions
cd /d "C:\Users\ericn\PokeBot"
call .venv\Scripts\activate.bat

:loop
echo [%date% %time%] Starting Pokebot agent...
python -u agent\run_agent.py
set EXITCODE=%ERRORLEVEL%
echo [%date% %time%] Agent exited with code %EXITCODE%. Restarting in 10 seconds...
timeout /t 10 /nobreak >nul
goto loop