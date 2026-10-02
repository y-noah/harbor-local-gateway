param([switch]$NoBrowser)
$ErrorActionPreference = 'Stop'
$gatewayRoot = $PSScriptRoot
$runtimeNode = (Get-Command node -ErrorAction SilentlyContinue).Source
if (-not $runtimeNode) {
    $runtimeNode = Join-Path $env:USERPROFILE '.cache\codex-runtimes\codex-primary-runtime\dependencies\node\bin\node.exe'
}
if (-not (Test-Path -LiteralPath $runtimeNode)) { throw 'Node.js 24 or newer is required.' }
# Serialize rapid repeated launches while the bootstrap process and server hand off.
$gatewayHashBytes = [System.Security.Cryptography.SHA256]::Create().ComputeHash([System.Text.Encoding]::UTF8.GetBytes($gatewayRoot.ToLowerInvariant()))
$gatewayMutexName = 'Local\HarborLauncher-' + ([System.BitConverter]::ToString($gatewayHashBytes)).Replace('-', '')
$gatewayMutex = New-Object System.Threading.Mutex($false, $gatewayMutexName)
$gatewayOwnsMutex = $false
try {
    try { $gatewayOwnsMutex = $gatewayMutex.WaitOne(120000) } catch [System.Threading.AbandonedMutexException] { $gatewayOwnsMutex = $true }
    if (-not $gatewayOwnsMutex) { throw 'Another Harbor launch is still initializing. Please try again shortly.' }
$gatewaySettings = Join-Path $gatewayRoot 'data\settings.json'
$gatewayPort = 43127
if (Test-Path -LiteralPath $gatewaySettings) {
    $gatewayConfig = Get-Content -Raw -LiteralPath $gatewaySettings | ConvertFrom-Json
    $gatewayPort = $gatewayConfig.port
}
$gatewayUrl = "http://127.0.0.1:$gatewayPort"
$gatewayRunning = $false
try { $gatewayHealth = Invoke-RestMethod "$gatewayUrl/health" -TimeoutSec 2; $gatewayRunning = $gatewayHealth.service -eq 'harbor' } catch {}
if (-not $gatewayRunning) {
    & $runtimeNode (Join-Path $gatewayRoot 'bootstrap.mjs')
    if ($LASTEXITCODE -ne 0) { throw 'Initialization failed.' }
    $gatewayProcess = Start-Process -FilePath $runtimeNode -ArgumentList @(('"' + (Join-Path $gatewayRoot 'server.mjs') + '"')) -WorkingDirectory $gatewayRoot -WindowStyle Hidden -RedirectStandardOutput (Join-Path $gatewayRoot 'data\server.log') -RedirectStandardError (Join-Path $gatewayRoot 'data\server-error.log') -PassThru
    $gatewayProcess.Id | Set-Content -LiteralPath (Join-Path $gatewayRoot 'data\server.pid')
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        Start-Sleep -Milliseconds 300
        try { $gatewayHealth = Invoke-RestMethod "$gatewayUrl/health" -TimeoutSec 1; if ($gatewayHealth.service -eq 'harbor') { $gatewayRunning = $true; break } } catch {}
    }
    if (-not $gatewayRunning) { throw 'Startup failed. See data/server-error.log.' }
}
$gatewayConfig = Get-Content -Raw -LiteralPath $gatewaySettings | ConvertFrom-Json
if (-not $NoBrowser) { Start-Process ($gatewayUrl + '/') }
Write-Output "Harbor is running: $gatewayUrl"

} finally {
    if ($gatewayOwnsMutex) { $gatewayMutex.ReleaseMutex() }
    $gatewayMutex.Dispose()
}
