param(
    [int]$CameraIndex = 0,
    [string]$StreamId = "",
    [int]$FrameSkip = 1,
    [switch]$InstallDependencies
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

function Import-DotEnv {
    param([string]$Path)

    if (-not (Test-Path $Path)) {
        return
    }

    foreach ($rawLine in Get-Content $Path) {
        $line = $rawLine.Trim()
        if (-not $line -or $line.StartsWith("#")) {
            continue
        }

        $parts = $line -split "=", 2
        if ($parts.Count -ne 2) {
            continue
        }

        [Environment]::SetEnvironmentVariable($parts[0], $parts[1], "Process")
    }
}

function Resolve-PythonLauncher {
    if (Get-Command python -ErrorAction SilentlyContinue) {
        return ,@("python")
    }

    if (Get-Command py -ErrorAction SilentlyContinue) {
        return ,@("py", "-3")
    }

    throw "Python 3 was not found on PATH."
}

function Invoke-Python {
    param([string[]]$PythonArgs)

    $launcherArgs = @()
    if ($script:python.Count -gt 1) {
        $launcherArgs = $script:python[1..($script:python.Count - 1)]
    }

    & $script:python[0] @launcherArgs @PythonArgs
    if ($LASTEXITCODE -ne 0) {
        throw "Python command failed with exit code $LASTEXITCODE."
    }
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker was not found on PATH."
}

$script:python = @(Resolve-PythonLauncher)

Write-Host "Using Python launcher: $($script:python -join ' ')"

Import-DotEnv ".env"

if ([string]::IsNullOrWhiteSpace($StreamId)) {
    $StreamId = "webcam-$CameraIndex"
}

$env:KAFKA_BROKER = "localhost:9092"
$env:INGEST_SOURCE = "$CameraIndex"
$env:STREAM_ID = $StreamId
$env:FRAME_SKIP = "$FrameSkip"

Write-Host "Starting supporting services in Docker..."
docker compose up --build -d kafka kafka-init inference aggregator prometheus grafana
if ($LASTEXITCODE -ne 0) {
    throw "docker compose up failed."
}

Write-Host ""
docker compose ps

if ($InstallDependencies) {
    Write-Host ""
    Write-Host "Installing local ingest dependencies..."
    Invoke-Python -PythonArgs @("-m", "pip", "install", "-r", "ingest/requirements.txt")
}

Write-Host ""
Write-Host "Starting local webcam ingest"
Write-Host "  camera index : $CameraIndex"
Write-Host "  stream id    : $StreamId"
Write-Host "  kafka broker : $env:KAFKA_BROKER"
Write-Host "  detections   : http://localhost:8080/api/streams/$StreamId/detections"
Write-Host "  websocket    : ws://localhost:8080/ws/$StreamId"
Write-Host ""

Invoke-Python -PythonArgs @("ingest/main.py")
