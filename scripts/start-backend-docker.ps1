param(
    [string]$EnvFile = "deploy\backend.env",
    [string[]]$Profiles = @(),
    [switch]$Build,
    [switch]$Pull
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$ComposeFile = Join-Path $Root "deploy\docker-compose.backend.yml"
$EnvPath = Join-Path $Root $EnvFile

if (-not (Test-Path $EnvPath)) {
    throw "Missing env file: $EnvPath. Copy deploy\backend.env.example to deploy\backend.env first."
}

$ArgsList = @(
    "compose",
    "--env-file", $EnvPath,
    "-f", $ComposeFile
)

foreach ($Profile in $Profiles) {
    if ($Profile.Trim().Length -gt 0) {
        $ArgsList += @("--profile", $Profile)
    }
}

$ArgsList += @("up", "-d")

if ($Build) {
    $ArgsList += "--build"
}

if ($Pull) {
    $ArgsList += "--pull", "always"
}

docker @ArgsList
