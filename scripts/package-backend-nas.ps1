param(
    [string]$OutputPath = "work\deploy\tv-backend-nas-package.zip",
    [switch]$IncludeChan = $true
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$StageRoot = Join-Path $RepoRoot "work\deploy\tv-backend-nas-package"
$OutputFullPath = Join-Path $RepoRoot $OutputPath
$RepoResolved = (Resolve-Path $RepoRoot).Path

function Assert-InRepo([string]$Path) {
    $FullPath = [System.IO.Path]::GetFullPath($Path)
    if (-not $FullPath.StartsWith($RepoResolved, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to operate outside repo: $FullPath"
    }
    return $FullPath
}

$StageRoot = Assert-InRepo $StageRoot
$OutputFullPath = Assert-InRepo $OutputFullPath
$OutputDir = Split-Path -Parent $OutputFullPath
New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null

$WebDist = Join-Path $RepoRoot "apps\web\dist"
if (-not (Test-Path $WebDist)) {
    throw "Missing frontend build output: $WebDist. Run npm run build in apps\web before packaging."
}

if (Test-Path $StageRoot) {
    Remove-Item -LiteralPath $StageRoot -Recurse -Force
}
New-Item -ItemType Directory -Force -Path $StageRoot | Out-Null

$Dirs = @(
    "db\sql",
    "deploy",
    "docs\runbooks",
    "apps\web\dist",
    "libs\protocol\python",
    "services\api",
    "services\collector"
)

foreach ($Dir in $Dirs) {
    $Source = Join-Path $RepoRoot $Dir
    $Target = Join-Path $StageRoot $Dir
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $Target) | Out-Null
    Copy-Item -LiteralPath $Source -Destination $Target -Recurse -Force
}

$StageDeploy = Join-Path $StageRoot "deploy"
Get-ChildItem -LiteralPath $StageDeploy -File -Force |
    Where-Object {
        ($_.Name -like "*.env" -or $_.Name -like "*.env.*" -or $_.Name -like "backend.env*") -and
        ($_.Name -notlike "*.example")
    } |
    Remove-Item -Force

$EnvTemplateSource = Join-Path $StageDeploy "backend.env.example"
if (-not (Test-Path $EnvTemplateSource)) {
    throw "Missing env template: $EnvTemplateSource"
}
Copy-Item -LiteralPath $EnvTemplateSource -Destination (Join-Path $StageDeploy "backend.env.template") -Force

if ($IncludeChan) {
    $ChanSource = Join-Path $RepoRoot "work\vendor\chan.py-main"
    if (-not (Test-Path $ChanSource)) {
        throw "Missing chan.py source: $ChanSource"
    }
    $ChanTarget = Join-Path $StageRoot "work\vendor\chan.py-main"
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $ChanTarget) | Out-Null
    Copy-Item -LiteralPath $ChanSource -Destination $ChanTarget -Recurse -Force
}

Get-ChildItem -LiteralPath $StageRoot -Recurse -Directory -Force |
    Where-Object { $_.Name -in @("__pycache__", ".pytest_cache") } |
    Remove-Item -Recurse -Force

$TdxReadme = Join-Path $StageRoot "work\tdx-csv\README.txt"
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $TdxReadme) | Out-Null
Set-Content -LiteralPath $TdxReadme -Encoding UTF8 -Value @(
    "Copy downloaded TDX zip folders here before enabling the tdx-csv-import profile.",
    "Expected subfolders are the original TDX Chinese K-line folders, such as the 5-minute, 15-minute, 30-minute, and 60-minute zip folders.",
    "",
    "Module C Chan workers consume native-timeframe bars."
)

$NasReadme = Join-Path $StageRoot "NAS-README.txt"
Set-Content -LiteralPath $NasReadme -Encoding UTF8 -Value @(
    "TradingView A-share backend NAS package",
    "",
    "1. Copy deploy/backend.env.template to deploy/backend.env on the NAS and fill POSTGRES_PASSWORD, API_TOKEN, ADMIN_API_TOKEN, CORS_ORIGINS, and host paths.",
    "2. Put TDX history zips under work/tdx-csv or point TDX_CSV_HOST_ROOT at another NAS path.",
    "3. Start core services:",
    "   docker compose --env-file deploy/backend.env -f deploy/docker-compose.backend.yml up -d --build",
    "4. Visit local NAS gateway before opening the public tunnel:",
    "   http://<NAS-IP>:8080",
    "5. Put CLOUDFLARED_TOKEN into deploy/backend.env, then start the Cloudflare Tunnel:",
    "   docker compose --env-file deploy/backend.env -f deploy/docker-compose.backend.yml --profile tunnel up -d",
    "",
    "Same-origin local website: http://<NAS-IP>:8080",
    "API endpoint for frontend devices: http://<NAS-IP>:8001"
)

if (Test-Path $OutputFullPath) {
    Remove-Item -LiteralPath $OutputFullPath -Force
}
Add-Type -AssemblyName System.IO.Compression
Add-Type -AssemblyName System.IO.Compression.FileSystem
$ZipStream = [System.IO.File]::Open($OutputFullPath, [System.IO.FileMode]::CreateNew)
try {
    $Zip = New-Object System.IO.Compression.ZipArchive($ZipStream, [System.IO.Compression.ZipArchiveMode]::Create)
    try {
        Get-ChildItem -LiteralPath $StageRoot -Recurse -File -Force | ForEach-Object {
            $RelativePath = $_.FullName.Substring($StageRoot.Length).TrimStart("\", "/")
            $EntryName = $RelativePath.Replace([System.IO.Path]::DirectorySeparatorChar, "/")
            [System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile(
                $Zip,
                $_.FullName,
                $EntryName,
                [System.IO.Compression.CompressionLevel]::Optimal
            ) | Out-Null
        }
    }
    finally {
        $Zip.Dispose()
    }
}
finally {
    $ZipStream.Dispose()
}
Write-Host "Created $OutputFullPath"
