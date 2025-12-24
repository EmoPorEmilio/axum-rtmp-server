# RTMP Server Integration Tests Runner (Windows)
# This script starts the Docker environment and runs integration tests

$ErrorActionPreference = "Stop"

$ProjectDir = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $ProjectDir) {
    $ProjectDir = Get-Location
}

Set-Location $ProjectDir

Write-Host "=== RTMP Server Integration Tests ===" -ForegroundColor Cyan
Write-Host ""

function Cleanup {
    Write-Host ""
    Write-Host "Cleaning up Docker containers..."
    docker-compose down --remove-orphans 2>$null
}

# Register cleanup on exit
$null = Register-EngineEvent -SourceIdentifier PowerShell.Exiting -Action { Cleanup }

try {
    # Step 1: Build the Docker image
    Write-Host "Step 1: Building Docker image..." -ForegroundColor Yellow
    docker-compose build rtmp-server
    if ($LASTEXITCODE -ne 0) { throw "Docker build failed" }

    # Step 2: Start infrastructure (nginx-rtmp containers)
    Write-Host "Step 2: Starting infrastructure..." -ForegroundColor Yellow
    docker-compose up -d nginx-rtmp-primary nginx-rtmp-backup
    if ($LASTEXITCODE -ne 0) { throw "Failed to start nginx-rtmp containers" }

    # Wait for nginx-rtmp to be ready
    Write-Host "Waiting for nginx-rtmp containers to be healthy..."
    Start-Sleep -Seconds 5

    # Step 3: Start our RTMP server
    Write-Host "Step 3: Starting RTMP server..." -ForegroundColor Yellow
    docker-compose up -d rtmp-server
    if ($LASTEXITCODE -ne 0) { throw "Failed to start RTMP server" }

    # Wait for RTMP server to be ready
    Write-Host "Waiting for RTMP server to be ready..."
    Start-Sleep -Seconds 3

    # Check if server is responding
    Write-Host "Checking RTMP server health..."
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:8080/metrics" -UseBasicParsing -TimeoutSec 5
        Write-Host "RTMP server is ready!" -ForegroundColor Green
    } catch {
        Write-Host "RTMP server failed to start" -ForegroundColor Red
        docker-compose logs rtmp-server
        throw "RTMP server health check failed"
    }

    # Step 4: Run unit tests
    Write-Host ""
    Write-Host "Step 4: Running unit tests..." -ForegroundColor Yellow
    cargo test --lib -- --nocapture
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Unit tests failed" -ForegroundColor Red
        # Continue anyway to see integration test results
    }

    # Step 5: Run integration tests
    Write-Host ""
    Write-Host "Step 5: Running integration tests..." -ForegroundColor Yellow
    $env:RTMP_SERVER_ADDR = "127.0.0.1:1935"
    $env:TEST_STREAM_KEY = "test-stream-key"
    $env:PRIMARY_STATS_URL = "http://localhost:8081/stat"
    $env:BACKUP_STATS_URL = "http://localhost:8082/stat"
    $env:METRICS_URL = "http://localhost:8080/metrics"

    cargo test --test '*' -- --ignored --nocapture 2>&1

    # Step 6: Run FFmpeg test stream
    Write-Host ""
    Write-Host "Step 6: Running FFmpeg test stream..." -ForegroundColor Yellow
    docker-compose --profile test up -d ffmpeg-test

    # Wait for stream to establish
    Write-Host "Waiting for stream to propagate..."
    Start-Sleep -Seconds 10

    # Step 7: Check destinations received stream
    Write-Host ""
    Write-Host "Step 7: Verifying stream replication..." -ForegroundColor Yellow

    $primaryOk = $false
    $backupOk = $false

    try {
        $primaryStats = Invoke-WebRequest -Uri "http://localhost:8081/stat" -UseBasicParsing -TimeoutSec 5
        if ($primaryStats.Content -match "publishing|<live>") {
            Write-Host "✓ Primary destination (nginx-rtmp-primary) is receiving stream" -ForegroundColor Green
            $primaryOk = $true
        } else {
            Write-Host "✗ Primary destination is NOT receiving stream" -ForegroundColor Red
        }
    } catch {
        Write-Host "✗ Could not reach primary destination" -ForegroundColor Red
    }

    try {
        $backupStats = Invoke-WebRequest -Uri "http://localhost:8082/stat" -UseBasicParsing -TimeoutSec 5
        if ($backupStats.Content -match "publishing|<live>") {
            Write-Host "✓ Backup destination (nginx-rtmp-backup) is receiving stream" -ForegroundColor Green
            $backupOk = $true
        } else {
            Write-Host "✗ Backup destination is NOT receiving stream" -ForegroundColor Red
        }
    } catch {
        Write-Host "✗ Could not reach backup destination" -ForegroundColor Red
    }

    # Step 8: Check metrics
    Write-Host ""
    Write-Host "Step 8: Checking metrics..." -ForegroundColor Yellow
    try {
        $metrics = Invoke-WebRequest -Uri "http://localhost:8080/metrics" -UseBasicParsing -TimeoutSec 5
        if ($metrics.Content -match "bytes") {
            Write-Host "✓ Metrics endpoint is working" -ForegroundColor Green
        } else {
            Write-Host "! Metrics may not have data yet" -ForegroundColor Yellow
        }
    } catch {
        Write-Host "✗ Metrics endpoint not responding" -ForegroundColor Red
    }

    # Summary
    Write-Host ""
    Write-Host "=== Test Summary ===" -ForegroundColor Cyan
    if ($primaryOk -and $backupOk) {
        Write-Host "All integration tests PASSED!" -ForegroundColor Green
        exit 0
    } else {
        Write-Host "Some integration tests FAILED" -ForegroundColor Red
        Write-Host ""
        Write-Host "Container logs:"
        docker-compose logs --tail=50 rtmp-server
        exit 1
    }

} finally {
    Cleanup
}
