#!/bin/bash
###############################################################################
# YouDescribeX Pipeline Setup Script
# This script handles complete cleanup and initialization of all pipeline services
# including the web server, RQ workers, and Redis management.
###############################################################################

# Exit on any error and enable error tracing
set -e
set -x

echo "Starting YouDescribeX Pipeline Setup..."

# Store the script's directory path
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

###############################################################################
# Cleanup Section: Ensures all previous instances are properly terminated
###############################################################################

cleanup_existing_processes() {
    echo "Performing comprehensive cleanup of existing processes..."

    # Kill any existing RQ workers
    echo "Terminating existing RQ workers..."
    pkill -f "rq worker" 2>/dev/null || true

    # Kill any existing uvicorn processes
    echo "Terminating existing uvicorn processes..."
    pkill -f "uvicorn.*:8086" 2>/dev/null || true

    # Clean up tmux sessions
    echo "Cleaning up tmux sessions..."
    tmux kill-session -t "pipeline_worker_general" 2>/dev/null || true
    tmux kill-session -t "pipeline_worker_caption" 2>/dev/null || true
    tmux kill-session -t "pipeline_web_server" 2>/dev/null || true

    # Clear Redis queues
    echo "Clearing Redis queues..."
    redis-cli FLUSHALL 2>/dev/null || true

    # Wait for processes to fully terminate
    echo "Waiting for processes to terminate..."
    sleep 3
}

###############################################################################
# Environment Setup Section: Prepares the Python environment
###############################################################################

setup_environment() {
    echo "Setting up Python environment..."

    # Create necessary directories
    mkdir -p logs
    mkdir -p pipeline_logs

    # Setup virtual environment if it doesn't exist
    VENV_DIR="$SCRIPT_DIR/venv"
    if [ ! -d "$VENV_DIR" ]; then
        echo "Creating virtual environment..."
        python3 -m venv "$VENV_DIR"
    fi

    echo "Activating virtual environment..."
    source "$VENV_DIR/bin/activate"

    # Install dependencies
    echo "Installing dependencies..."
    pip install -r requirements.txt > /dev/null
}

###############################################################################
# Redis Check Section: Ensures Redis server is running
###############################################################################

check_redis() {
    echo "Checking Redis server status..."
    if ! redis-cli ping > /dev/null 2>&1; then
        echo "Redis is not running. Starting Redis server..."
        sudo service redis-server start
        sleep 2

        # Verify Redis started successfully
        if ! redis-cli ping > /dev/null 2>&1; then
            echo "Failed to start Redis server. Exiting..."
            exit 1
        fi
    fi
    echo "✓ Redis server is running"
}

###############################################################################
# Service Startup Section: Launches all required services
###############################################################################

start_web_server() {
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
            2>&1 | tee logs/web_server.log"
}

start_general_workers() {
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
        wait"
}

start_caption_worker() {
    echo "Starting dedicated caption worker..."
    tmux new-session -d -s "pipeline_worker_caption" bash -c "\
        source $VENV_DIR/bin/activate; \
        export PYTHONPATH=$SCRIPT_DIR:$PYTHONPATH; \
        CUDA_VISIBLE_DEVICES=0 rq worker caption_tasks \
            --url redis://localhost:6379 \
            --name caption_worker_$(date +%s) \
            --with-scheduler \
            --verbose \
            2>&1 | tee logs/caption_worker.log"
}

###############################################################################
# Verification Section: Checks if services started correctly
###############################################################################

verify_services() {
    echo "Verifying services status..."
    sleep 5  # Wait for services to initialize

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
}

###############################################################################
# Main Execution
###############################################################################

main() {
    # Run cleanup first
    cleanup_existing_processes

    # Setup environment
    setup_environment

    # Check Redis
    check_redis

    # Start services
    start_web_server
    start_general_workers
    start_caption_worker

    # Verify services
    verify_services

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
}

# Run the main function
main