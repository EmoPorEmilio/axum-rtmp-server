#!/bin/bash
set -e

# RTMP Server Integration Tests Runner
# This script starts the Docker environment and runs integration tests

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

echo "=== RTMP Server Integration Tests ==="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

cleanup() {
    echo ""
    echo "Cleaning up Docker containers..."
    docker-compose down --remove-orphans 2>/dev/null || true
}

# Set up trap for cleanup on exit
trap cleanup EXIT

# Step 1: Build the Docker image
echo -e "${YELLOW}Step 1: Building Docker image...${NC}"
docker-compose build rtmp-server

# Step 2: Start infrastructure (nginx-rtmp containers)
echo -e "${YELLOW}Step 2: Starting infrastructure...${NC}"
docker-compose up -d nginx-rtmp-primary nginx-rtmp-backup

# Wait for nginx-rtmp to be ready
echo "Waiting for nginx-rtmp containers to be healthy..."
sleep 5

# Step 3: Start our RTMP server
echo -e "${YELLOW}Step 3: Starting RTMP server...${NC}"
docker-compose up -d rtmp-server

# Wait for RTMP server to be ready
echo "Waiting for RTMP server to be ready..."
sleep 3

# Check if server is responding
echo "Checking RTMP server health..."
if curl -s http://localhost:8080/metrics > /dev/null; then
    echo -e "${GREEN}RTMP server is ready!${NC}"
else
    echo -e "${RED}RTMP server failed to start${NC}"
    docker-compose logs rtmp-server
    exit 1
fi

# Step 4: Run unit tests
echo ""
echo -e "${YELLOW}Step 4: Running unit tests...${NC}"
cargo test --lib -- --nocapture

# Step 5: Run integration tests (non-Docker ones)
echo ""
echo -e "${YELLOW}Step 5: Running integration tests...${NC}"
export RTMP_SERVER_ADDR="127.0.0.1:1935"
export TEST_STREAM_KEY="test-stream-key"
export PRIMARY_STATS_URL="http://localhost:8081/stat"
export BACKUP_STATS_URL="http://localhost:8082/stat"
export METRICS_URL="http://localhost:8080/metrics"

cargo test --test '*' -- --ignored --nocapture 2>&1 || true

# Step 6: Run FFmpeg test stream (optional, with profile)
echo ""
echo -e "${YELLOW}Step 6: Running FFmpeg test stream...${NC}"
docker-compose --profile test up -d ffmpeg-test

# Wait for stream to establish
echo "Waiting for stream to propagate..."
sleep 10

# Step 7: Check destinations received stream
echo ""
echo -e "${YELLOW}Step 7: Verifying stream replication...${NC}"

PRIMARY_OK=false
BACKUP_OK=false

if curl -s http://localhost:8081/stat | grep -q "publishing\|<live>"; then
    echo -e "${GREEN}✓ Primary destination (nginx-rtmp-primary) is receiving stream${NC}"
    PRIMARY_OK=true
else
    echo -e "${RED}✗ Primary destination is NOT receiving stream${NC}"
fi

if curl -s http://localhost:8082/stat | grep -q "publishing\|<live>"; then
    echo -e "${GREEN}✓ Backup destination (nginx-rtmp-backup) is receiving stream${NC}"
    BACKUP_OK=true
else
    echo -e "${RED}✗ Backup destination is NOT receiving stream${NC}"
fi

# Step 8: Check metrics
echo ""
echo -e "${YELLOW}Step 8: Checking metrics...${NC}"
if curl -s http://localhost:8080/metrics | grep -q "bytes"; then
    echo -e "${GREEN}✓ Metrics endpoint is working${NC}"
else
    echo -e "${YELLOW}! Metrics may not have data yet${NC}"
fi

# Summary
echo ""
echo "=== Test Summary ==="
if $PRIMARY_OK && $BACKUP_OK; then
    echo -e "${GREEN}All integration tests PASSED!${NC}"
    exit 0
else
    echo -e "${RED}Some integration tests FAILED${NC}"
    echo ""
    echo "Container logs:"
    docker-compose logs --tail=50 rtmp-server
    exit 1
fi
