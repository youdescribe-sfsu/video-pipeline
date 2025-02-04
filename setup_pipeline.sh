#!/bin/bash
###############################################################################
# YouDescribeX Pipeline Setup Script
#
# This script sets up and manages the dual-queue worker system for video processing:
# - General processing workers (video_tasks queue)
# - Dedicated image captioning worker (caption_tasks queue)
#
# The script handles:
# 1. Environment setup and validation
# 2. Redis server checking
# 3. Worker process management via tmux
# 4. Proper GPU allocation for image captioning
###############################################################################

# Exit on any error
set -e

echo "Starting YouDescribeX Pipeline Setup..."

# Ensure we're in the right directory
cd "$(dirname "$0")"

# Environment Setup
#-------------------------------------------------------------------------------
# Check and activate virtual environment if needed
if [[ "$VIRTUAL_ENV" == "" ]]; then
    echo "Virtual environment not active, attempting to activate..."
    if [ -d "venv" ]; then
        source venv/bin/activate
    else
        echo "Error: Virtual environment not found in ./venv"
        exit 1
    fi
fi

# Set the number of general processing workers
export MAX_PIPELINE_WORKERS=4

# Set GPU device for captioning
export CAPTION_GPU_DEVICE=0

# Define session names for better organization
TMUX_SESSION_GENERAL="pipeline_worker_general"
TMUX_SESSION_CAPTION="pipeline_worker_caption"

# Redis Check
#-------------------------------------------------------------------------------
echo "Checking Redis server status..."
if ! redis-cli ping > /dev/null 2>&1; then
    echo "Redis is not running. Starting Redis server..."
    sudo service redis-server start

    # Wait for Redis to fully start
    for i in {1..5}; do
        if redis-cli ping > /dev/null 2>&1; then
            break
        fi
        echo "Waiting for Redis to start... ($i/5)"
        sleep 2
    done

    if ! redis-cli ping > /dev/null 2>&1; then
        echo "Failed to start Redis server"
        exit 1
    fi
fi
echo "Redis server is running"

# Clean Up Existing Sessions
#-------------------------------------------------------------------------------
echo "Cleaning up existing tmux sessions..."
tmux kill-session -t "$TMUX_SESSION_GENERAL" 2>/dev/null || true
tmux kill-session -t "$TMUX_SESSION_CAPTION" 2>/dev/null || true

# Start General Processing Workers
#-------------------------------------------------------------------------------
echo "Starting general processing workers..."
tmux new-session -d -s "$TMUX_SESSION_GENERAL" bash -c "\
    source venv/bin/activate; \
    echo 'Starting general processing workers with concurrency=$MAX_PIPELINE_WORKERS'; \
    rq worker video_tasks \
        --url redis://localhost:6379 \
        --concurrency=4 \
        --with-scheduler \
        --verbose \
        --name 'general_worker' \
        2>&1 | tee logs/general_worker.log \
"

# Start Dedicated Caption Worker
#-------------------------------------------------------------------------------
echo "Starting dedicated caption worker..."
tmux new-session -d -s "$TMUX_SESSION_CAPTION" bash -c "\
    source venv/bin/activate; \
    echo 'Starting caption worker on GPU $CAPTION_GPU_DEVICE'; \
    CUDA_VISIBLE_DEVICES=$CAPTION_GPU_DEVICE rq worker caption_tasks \
        --url redis://localhost:6379 \
        --concurrency=1 \
        --with-scheduler \
        --verbose \
        --name 'caption_worker' \
        2>&1 | tee logs/caption_worker.log \
"

# Verify Worker Status
#-------------------------------------------------------------------------------
echo "Verifying worker status..."
sleep 3  # Give workers time to start

echo "Checking RQ workers status..."
rq info --raw | grep -E "general_worker|caption_worker" || echo "No workers found in RQ info"

# Create Status Report
#-------------------------------------------------------------------------------
echo -e "\nPipeline Setup Status:"
echo "------------------------"
echo "✓ Redis Server: Running"
echo "✓ General Workers: Started with concurrency $MAX_PIPELINE_WORKERS"
echo "✓ Caption Worker: Started on GPU $CAPTION_GPU_DEVICE"

# Print Monitoring Instructions
#-------------------------------------------------------------------------------
echo -e "\nTo monitor workers:"
echo "  General workers: tmux attach -t $TMUX_SESSION_GENERAL"
echo "  Caption worker: tmux attach -t $TMUX_SESSION_CAPTION"
echo -e "\nLog files are available in the logs directory:"
echo "  - logs/general_worker.log"
echo "  - logs/caption_worker.log"

# Health Check
#-------------------------------------------------------------------------------
echo -e "\nPerforming final health check..."
if tmux has-session -t "$TMUX_SESSION_GENERAL" 2>/dev/null && \
   tmux has-session -t "$TMUX_SESSION_CAPTION" 2>/dev/null; then
    echo "✓ All pipeline services started successfully"
else
    echo "⚠ Warning: Some services may not have started properly"
    echo "Please check the logs for more information"
fi

echo -e "\nSetup complete! The pipeline is ready to process tasks."