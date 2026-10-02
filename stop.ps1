$ErrorActionPreference = 'Stop'
$gatewayRoot = $PSScriptRoot
$gatewayPidFile = Join-Path $gatewayRoot 'data\server.pid'
if (-not (Test-Path -LiteralPath $gatewayPidFile)) { Write-Output 'No managed server found.'; exit }
$gatewayPid = [int](Get-Content -LiteralPath $gatewayPidFile)
$gatewayProcess = Get-CimInstance Win32_Process -Filter "ProcessId = $gatewayPid" -ErrorAction SilentlyContinue
$gatewayScript = Join-Path $gatewayRoot 'server.mjs'
if ($gatewayProcess -and $gatewayProcess.CommandLine -and $gatewayProcess.CommandLine.Contains($gatewayScript)) {
    $gatewayConfig = Get-Content -Raw -LiteralPath (Join-Path $gatewayRoot 'data\settings.json') | ConvertFrom-Json
    $gatewayUrl = 'http://127.0.0.1:' + $gatewayConfig.port + '/api/admin/shutdown'
    Invoke-RestMethod -Method Post -Uri $gatewayUrl -Headers @{Authorization = ('Bearer ' + $gatewayConfig.adminToken)} -TimeoutSec 5 | Out-Null
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        if (-not (Get-Process -Id $gatewayPid -ErrorAction SilentlyContinue)) { break }
        Start-Sleep -Milliseconds 300
    }
    if (Get-Process -Id $gatewayPid -ErrorAction SilentlyContinue) { throw 'Server is still stopping; retry in a moment.' }
    Remove-Item -LiteralPath $gatewayPidFile
    Write-Output 'Harbor stopped gracefully.'
} elseif (-not $gatewayProcess) { Remove-Item -LiteralPath $gatewayPidFile; Write-Output 'Server already stopped.' }
else { throw 'PID belongs to another process; no process was stopped.' }
