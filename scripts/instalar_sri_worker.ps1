$ErrorActionPreference = "Stop"

$TaskName = "Conta - SRI Worker"
$ContaDir = "D:\Aplicaciones\Conta"
$Script = Join-Path $ContaDir "scripts\iniciar_sri_worker.ps1"
$User = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name

$Action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$Script`"" `
    -WorkingDirectory $ContaDir

$Trigger = New-ScheduledTaskTrigger -AtLogOn -User $User
$Principal = New-ScheduledTaskPrincipal -UserId $User -LogonType Interactive -RunLevel Highest
$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable

$Task = New-ScheduledTask `
    -Action $Action `
    -Trigger $Trigger `
    -Principal $Principal `
    -Settings $Settings `
    -Description "Worker interactivo de Conta para sincronizaciones SRI y CAPTCHA."

Register-ScheduledTask -TaskName $TaskName -InputObject $Task -Force | Out-Null

Write-Host ""
Write-Host "Worker SRI instalado correctamente."
Write-Host "Usuario: $User"
Write-Host "Tarea: $TaskName"
Write-Host ""
Write-Host "Para iniciarlo ahora:"
Write-Host "Start-ScheduledTask -TaskName `"$TaskName`""
Write-Host ""