#!/bin/bash

# Ensure we're in the right directory
cd "$(dirname "$0")"

# Ensure virtual environment is activated
if [[ "$VIRTUAL_ENV" == "" ]]; then
    echo "Activating virtual environment..."
    source venv/bin/activate
fi

# Set the number of workers
export MAX_PIPELINE_WORKERS=4

# Define session names
TMUX_SESSION_UVICORN="pipeline_server"
TMUX_SESSION_RQ="pipeline_rq_worker"

# Kill existing sessions if they exist
tmux kill-session -t "$TMUX_SESSION_UVICORN" 2>/dev/null
tmux kill-session -t "$TMUX_SESSION_RQ" 2>/dev/null

# Check if Redis is running
if ! redis-cli ping > /dev/null 2>&1; then
    echo "Redis is not running. Starting Redis..."
    sudo service redis-server start
    sleep 2
fi

# Start the Uvicorn server
echo "Starting Uvicorn server..."
tmux new-session -d -s "$TMUX_SESSION_UVICORN" bash -c "\
    source venv/bin/activate; \
    uvicorn web_server:app \
        --host 0.0.0.0 \
        --port 8086 \
        --workers \$MAX_PIPELINE_WORKERS \
        --log-level info\
"

# Start the RQ worker
echo "Starting RQ worker..."
tmux new-session -d -s "$TMUX_SESSION_RQ" bash -c "\
    source venv/bin/activate; \
    rq worker video_tasks --url redis://localhost:6379\
"

# Verify sessions are running
echo "Verifying tmux sessions..."
tmux ls

echo "Pipeline services started. To attach to sessions:"
echo "  tmux attach -t $TMUX_SESSION_UVICORN"
echo "  tmux attach -t $TMUX_SESSION_RQ"