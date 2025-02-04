#!/bin/bash

# Ensure we're in the right directory and capture it for later use
BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$BASE_DIR"

# Ensure virtual environment is activated and store its path
if [[ "$VIRTUAL_ENV" == "" ]]; then
    echo "Activating virtual environment..."
    source venv/bin/activate
fi
VENV_PATH="$VIRTUAL_ENV"

# Set the number of workers and store it for use in child processes
export MAX_PIPELINE_WORKERS=1

# Define session names as variables for consistency and easy modification
TMUX_SESSION_UVICORN="pipeline_server"
TMUX_SESSION_RQ="pipeline_rq_worker"

# Kill existing sessions if they exist, redirecting error output to prevent noise
echo "Cleaning up existing sessions..."
tmux kill-session -t "$TMUX_SESSION_UVICORN" 2>/dev/null
tmux kill-session -t "$TMUX_SESSION_RQ" 2>/dev/null

# Verify Redis is running, start if needed
echo "Checking Redis server..."
if ! redis-cli ping > /dev/null 2>&1; then
    echo "Redis is not running. Starting Redis..."
    sudo service redis-server start
    sleep 2  # Give Redis time to start fully
fi

# Create a log directory for storing session output
mkdir -p "$BASE_DIR/logs"

# Start the Uvicorn server with improved session handling and logging
echo "Starting Uvicorn server..."
tmux new-session -d -s "$TMUX_SESSION_UVICORN" "bash -c '\
    source \"$VENV_PATH/bin/activate\"; \
    cd \"$BASE_DIR\"; \
    exec uvicorn web_server:app \
        --host 0.0.0.0 \
        --port 8086 \
        --workers \$MAX_PIPELINE_WORKERS \
        --log-level info \
    2>\"$BASE_DIR/logs/uvicorn_error.log\" \
    '"

# Start the RQ worker with similar improvements
echo "Starting RQ worker..."
tmux new-session -d -s "$TMUX_SESSION_RQ" "bash -c '\
    source \"$VENV_PATH/bin/activate\"; \
    cd \"$BASE_DIR\"; \
    exec rq worker video_tasks --url redis://localhost:6379 \
    2>\"$BASE_DIR/logs/rq_worker_error.log\" \
    '"

# Give sessions time to initialize
sleep 2

# Verify sessions and provide detailed status
echo "Verifying tmux sessions..."
echo "------------------------"

# Check Uvicorn session
if tmux has-session -t "$TMUX_SESSION_UVICORN" 2>/dev/null; then
    echo "✓ Uvicorn server session running"
    UVICORN_PID=$(tmux list-panes -t "$TMUX_SESSION_UVICORN" -F "#{pane_pid}")
    echo "  → Session PID: $UVICORN_PID"
else
    echo "✗ Failed to create Uvicorn session"
    echo "  → Check logs at: $BASE_DIR/logs/uvicorn_error.log"
fi

# Check RQ Worker session
if tmux has-session -t "$TMUX_SESSION_RQ" 2>/dev/null; then
    echo "✓ RQ worker session running"
    RQ_PID=$(tmux list-panes -t "$TMUX_SESSION_RQ" -F "#{pane_pid}")
    echo "  → Session PID: $RQ_PID"
else
    echo "✗ Failed to create RQ worker session"
    echo "  → Check logs at: $BASE_DIR/logs/rq_worker_error.log"
fi

echo "------------------------"
echo "Active tmux sessions:"
tmux ls || echo "No active sessions found"
echo "------------------------"

# Provide instructions for accessing the sessions
echo "Pipeline services started. To attach to sessions:"
echo "  tmux attach -t $TMUX_SESSION_UVICORN"
echo "  tmux attach -t $TMUX_SESSION_RQ"

# Provide instructions for checking logs
echo
echo "To check service logs:"
echo "  tail -f $BASE_DIR/logs/uvicorn_error.log"
echo "  tail -f $BASE_DIR/logs/rq_worker_error.log"