$ErrorActionPreference = "Stop"

$matches = Get-CimInstance Win32_Process |
    Where-Object {
        $_.CommandLine -like "*collector.chan_recompute*" -or
        $_.CommandLine -like "*start-chan-recompute-worker.ps1*"
    }

if (-not $matches) {
    Write-Host "No chan recompute worker processes found."
    exit 0
}

$matches |
    Select-Object ProcessId, CommandLine |
    Format-Table -AutoSize

foreach ($process in $matches) {
    Stop-Process -Id $process.ProcessId -Force
}

Write-Host "Stopped $($matches.Count) chan recompute worker process(es)."
