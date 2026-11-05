param([string]$OutputDirectory = (Join-Path $env:LOCALAPPDATA 'HarborDevice\bin'))
$ErrorActionPreference = 'Stop'
$compiler = Join-Path $env:WINDIR 'Microsoft.NET\Framework64\v4.0.30319\csc.exe'
if (!(Test-Path -LiteralPath $compiler)) { throw '需要 Windows x64 .NET Framework 4.8 编译器' }
New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null
$target = Join-Path $OutputDirectory 'HarborDevice.exe'
if (Test-Path -LiteralPath $target) { throw '目标程序已存在；请在任务结束并停止辅助程序后，选择新的输出目录构建，再明确替换。' }
& $compiler /nologo /target:exe "/out:$target" /r:System.Net.Http.dll /r:System.Management.dll /r:System.Web.Extensions.dll (Join-Path $PSScriptRoot 'HarborDevice.cs')
if ($LASTEXITCODE -ne 0) { throw '设备辅助程序构建失败' }
Write-Output "已生成：$target"
Write-Output '未注册服务、未自动启动、未修改 Codex 配置。'
