@echo off
setlocal
cd /d "D:\Aplicaciones\Conta"
set "SRI_WORKER_OPERATIONS=compras"
set "SRI_WORKER_USER=%COMPUTERNAME%-COMPRAS-INTERACTIVO"
if not exist "D:\Aplicaciones\Conta\logs" mkdir "D:\Aplicaciones\Conta\logs"
"D:\Aplicaciones\Conta\.venv\Scripts\python.exe" -u -m app.sri_worker >> "D:\Aplicaciones\Conta\logs\sri_worker_interactive.log" 2>&1
