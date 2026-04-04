@echo off
:: ============================================================
:: run_and_sleep.bat
:: Activates the market_data venv, runs the daily pipeline,
:: logs output, then hibernates the PC.
:: ============================================================

set PROJECT_DIR=D:\market_data
set VENV_ACTIVATE=%PROJECT_DIR%\.venv\Scripts\activate.bat
set LOG_FILE=%PROJECT_DIR%\logs\runner.log

:: Move to project directory (required so state.json and data/ resolve correctly)
cd /d %PROJECT_DIR%

:: Activate venv
call %VENV_ACTIVATE%

:: Log start
echo. >> %LOG_FILE%
echo ======================================== >> %LOG_FILE%
echo Started: %date% %time% >> %LOG_FILE%
echo ======================================== >> %LOG_FILE%

:: Run pipeline — stdout and stderr both go to the log
market-data-run --merge >> %LOG_FILE% 2>&1

:: Log finish
echo ======================================== >> %LOG_FILE%
echo Finished: %date% %time% >> %LOG_FILE%
echo ======================================== >> %LOG_FILE%

:: Hibernate (saves state to disk, safest for unattended use)
:: To sleep instead, replace with: rundll32.exe powrprof.dll,SetSuspendState 0,1,0
shutdown /h
