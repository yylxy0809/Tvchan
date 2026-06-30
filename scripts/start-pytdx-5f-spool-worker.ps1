param(
    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
    [string]$OutputRoot = "D:\5f数据\5m_price_incremental",
    [string]$Start = "2026-04-18",
    [string]$End = "",
    [int]$SymbolLimit = 0,
    [int]$Concurrency = 4,
    [int]$PageSize = 800,
    [int]$MaxPagesPerSymbol = 0,
    [double]$Sleep = 0.15,
    [switch]$Reset,
    [switch]$DryRun
)

$CollectorDir = Join-Path $RepoRoot "services/collector"
$ProtocolDir = Join-Path $RepoRoot "libs/protocol/python"
$LogDir = Join-Path $RepoRoot "logs/pytdx-5f-spool"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
New-Item -ItemType Directory -Force -Path $OutputRoot | Out-Null

$env:PYTHONPATH = "$CollectorDir;$ProtocolDir"

$arguments = @(
    "-m", "collector.pytdx_5f_spool",
    "--output-root", $OutputRoot,
    "--start", $Start,
    "--symbol-limit", "$SymbolLimit",
    "--concurrency", "$Concurrency",
    "--page-size", "$PageSize",
    "--max-pages-per-symbol", "$MaxPagesPerSymbol",
    "--sleep", "$Sleep"
)

if ($End -ne "") {
    $arguments += @("--end", $End)
}
if ($Reset) {
    $arguments += "--reset"
}
if ($DryRun) {
    $arguments += "--dry-run"
}

$stdout = Join-Path $LogDir "worker.out.log"
$stderr = Join-Path $LogDir "worker.err.log"
$process = Start-Process `
    -FilePath "python" `
    -ArgumentList $arguments `
    -WorkingDirectory $RepoRoot `
    -RedirectStandardOutput $stdout `
    -RedirectStandardError $stderr `
    -WindowStyle Hidden `
    -PassThru

Write-Host "Started pytdx 5f spool worker PID $($process.Id)"
Write-Host "Output root: $OutputRoot"
Write-Host "Logs: $stdout"
