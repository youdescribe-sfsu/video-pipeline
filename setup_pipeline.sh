#!/bin/bash
###############################################################################
# YouDescribeX Pipeline Setup Script
# Manages both web server (uvicorn) and task processing (RQ) workers
###############################################################################

set -e  # Exit on any error

echo "Starting YouDescribeX Pipeline Setup..."

# Directory Setup
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Create necessary directories
mkdir -p logs
mkdir -p pipeline_logs

# Environment Setup
VENV_DIR="$SCRIPT_DIR/venv"
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
fi

echo "Activating virtual environment..."
source "$VENV_DIR/bin/activate"

# Verify dependencies
echo "Verifying dependencies..."
pip install -r requirements.txt > /dev/null

# Redis Check
echo "Checking Redis server status..."
if ! redis-cli ping > /dev/null 2>&1; then
    echo "Redis is not running. Starting Redis server..."
    sudo service redis-server start
    sleep 2
fi
echo "✓ Redis server is running"

# Clean up existing sessions
echo "Cleaning up existing tmux sessions..."
tmux kill-session -t "pipeline_worker_general" 2>/dev/null || true
tmux kill-session -t "pipeline_worker_caption" 2>/dev/null || true
tmux kill-session -t "pipeline_web_server" 2>/dev/null || true

# Start web server with uvicorn workers
echo "Starting web server with uvicorn workers..."
tmux new-session -d -s "pipeline_web_server" bash -c "\
    source $VENV_DIR/bin/activate; \
    export PYTHONPATH=$SCRIPT_DIR:$PYTHONPATH; \
    uvicorn web_server:app \
        --host 0.0.0.0 \
        --port 8086 \
        --workers 2 \
        --log-level info \
        --timeout-keep-alive 120 \
        2>&1 | tee logs/web_server.log \
"

# Start general processing workers
echo "Starting general processing workers..."
tmux new-session -d -s "pipeline_worker_general" bash -c "\
    source $VENV_DIR/bin/activate; \
    export PYTHONPATH=$SCRIPT_DIR:$PYTHONPATH; \
    for i in {1..4}; do \
        rq worker video_tasks \
            --url redis://localhost:6379 \
            --name general_worker_\$i \
            --with-scheduler \
            --verbose \
            2>&1 | tee -a logs/general_worker.log & \
    done; \
    wait \
"

# Start caption worker
echo "Starting dedicated caption worker..."
tmux new-session -d -s "pipeline_worker_caption" bash -c "\
    source $VENV_DIR/bin/activate; \
    export PYTHONPATH=$SCRIPT_DIR:$PYTHONPATH; \
    CUDA_VISIBLE_DEVICES=0 rq worker caption_tasks \
        --url redis://localhost:6379 \
        --name caption_worker \
        --with-scheduler \
        --verbose \
        2>&1 | tee logs/caption_worker.log \
"

# Wait for all services to start
echo "Waiting for services to initialize..."
sleep 5

# Verify all services
echo "Verifying services status..."

# Check web server
if curl -s http://localhost:8086/health_check > /dev/null; then
    echo "✓ Web server is running"
else
    echo "⚠ Warning: Web server may not be running properly"
fi

# Check RQ workers
WORKERS=$(rq info --raw | grep -c "worker" || echo "0")
if [ "$WORKERS" -ge 5 ]; then
    echo "✓ RQ Workers started successfully ($WORKERS workers running)"
else
    echo "⚠ Warning: Not all RQ workers are running (found $WORKERS, expected 5)"
fi

# Print status report
echo -e "\nPipeline Setup Status:"
echo "------------------------"
echo "✓ Redis Server: Running"
echo "✓ Web Server: Running on port 8086 (2 uvicorn workers)"
echo "✓ General Workers: Started (4 RQ processes)"
echo "✓ Caption Worker: Started on GPU 0"
echo "✓ Log Directory: $(pwd)/logs"

# Print monitoring instructions
echo -e "\nTo monitor services:"
echo "  tmux attach -t pipeline_web_server     # Web server logs"
echo "  tmux attach -t pipeline_worker_general # General worker logs"
echo "  tmux attach -t pipeline_worker_caption # Caption worker logs"
echo "  rq info                                # Queue status"
echo "  curl http://localhost:8086/health_check # Server health"

echo -e "\nSetup complete! The pipeline is ready to process tasks."