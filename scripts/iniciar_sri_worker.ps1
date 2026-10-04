$ErrorActionPreference = "Stop"

$ContaDir = "D:\Aplicaciones\Conta"
$Python = Join-Path $ContaDir ".venv\Scripts\python.exe"
$BrowserPath = Join-Path $ContaDir "playwright-browsers"

Set-Location $ContaDir
$env:PLAYWRIGHT_BROWSERS_PATH = $BrowserPath

Write-Host "Iniciando Conta - SRI Worker interactivo..."
Write-Host "Directorio: $ContaDir"
Write-Host "Python: $Python"
Write-Host "Playwright browsers: $BrowserPath"
Write-Host ""

& $Python -m app.sri_worker
