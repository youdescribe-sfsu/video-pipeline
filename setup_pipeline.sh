#!/bin/bash

# screen_pipeline.sh - Setup script for YouDescribeX video pipeline using screen sessions

# First ensure we're in the right directory
cd "$(dirname "$0")"

# Define our screen session names
UVICORN_SESSION="pipeline_server"
RQ_SESSION="pipeline_worker"

# Clean up any existing screen sessions to start fresh
echo "Cleaning up existing screen sessions..."
screen -S $UVICORN_SESSION -X quit > /dev/null 2>&1
screen -S $RQ_SESSION -X quit > /dev/null 2>&1

# Ensure virtual environment is activated for the whole script
if [[ "$VIRTUAL_ENV" == "" ]]; then
    echo "Activating virtual environment..."
    source venv/bin/activate
fi

# Set the number of workers for the pipeline
export MAX_PIPELINE_WORKERS=1

# Check if Redis is running - we need this for the RQ worker
if ! redis-cli ping > /dev/null 2>&1; then
    echo "Redis is not running. Starting Redis..."
    sudo service redis-server start
    sleep 2
fi

# Start the Uvicorn server in a screen session
echo "Starting Uvicorn server in screen session '$UVICORN_SESSION'..."
screen -dmS $UVICORN_SESSION bash -c "\
    source venv/bin/activate; \
    uvicorn web_server:app \
        --host 0.0.0.0 \
        --port 8086 \
        --workers \$MAX_PIPELINE_WORKERS \
        --log-level info\
"

# Start the RQ worker in a screen session
echo "Starting RQ worker in screen session '$RQ_SESSION'..."
screen -dmS $RQ_SESSION bash -c "\
    source venv/bin/activate; \
    rq worker video_tasks --url redis://localhost:6379\
"

# List running screen sessions to verify everything started
echo -e "\nVerifying running screen sessions:"
screen -ls

# Print helpful instructions for the user
echo -e "\nPipeline services started! To interact with the sessions:"
echo "  - View Uvicorn server: screen -r $UVICORN_SESSION"
echo "  - View RQ worker: screen -r $RQ_SESSION"
echo -e "\nScreen session commands:"
echo "  - Detach from session: Press Ctrl-a then d"
echo "  - Kill a session: Press Ctrl-a then k"
echo "  - See all sessions: screen -ls"