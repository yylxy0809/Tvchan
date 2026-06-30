param(
    [int]$WorkerCount = 2,
    [int]$SymbolLimit = 0,
    [int]$TaskLimit = 1,
    [double]$LoopInterval = 5,
    [string]$BaseTimeframe = "5f",
    [string]$ChanLevels = "5f,30f,1d",
    [string]$Modes = "confirmed,predictive",
    [string]$ChanPyPath = "",
    [string]$DatabaseUrl = "postgresql://trader:change-me-before-long-running@127.0.0.1:5432/tradingview_local"
)

$ErrorActionPreference = "Stop"

if ($WorkerCount -lt 1) {
    throw "WorkerCount must be >= 1"
}

$Root = Split-Path -Parent $PSScriptRoot
$LogDir = Join-Path $Root "work/logs/chan-recompute-fleet"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

if ($ChanPyPath.Trim().Length -eq 0) {
    $ChanPyPath = Join-Path $Root "work/vendor/chan.py-main"
}

$started = @()
for ($i = 1; $i -le $WorkerCount; $i++) {
    $outLog = Join-Path $LogDir ("worker-{0}.out.log" -f $i)
    $errLog = Join-Path $LogDir ("worker-{0}.err.log" -f $i)
    $args = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", (Join-Path $PSScriptRoot "start-chan-recompute-worker.ps1"),
        "-SymbolLimit", $SymbolLimit,
        "-TaskLimit", $TaskLimit,
        "-Concurrency", 1,
        "-Loop",
        "-LoopInterval", $LoopInterval,
        "-BaseTimeframe", $BaseTimeframe,
        "-ChanLevels", $ChanLevels,
        "-Modes", $Modes,
        "-ChanPyPath", $ChanPyPath,
        "-DatabaseUrl", $DatabaseUrl
    )
    $process = Start-Process powershell -ArgumentList $args -WindowStyle Hidden -PassThru -RedirectStandardOutput $outLog -RedirectStandardError $errLog
    $started += [pscustomobject]@{
        worker = $i
        pid = $process.Id
        out_log = $outLog
        err_log = $errLog
    }
}

$started | Format-Table -AutoSize
