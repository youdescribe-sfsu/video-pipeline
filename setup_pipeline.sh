#!/bin/bash

# Define the tmux session name
TMUX_SESSION_NAME="pipeline_server"

# Kill any existing tmux session with the same name
tmux kill-session -t "$TMUX_SESSION_NAME" 2>/dev/null

# Activate the virtual environment
source venv/bin/activate

# Start the pipeline in a tmux session
tmux new-session -d -s "$TMUX_SESSION_NAME" bash -c "\
    uvicorn web_server:app \\
        --host 0.0.0.0 \\
        --port 8086 \\
        --workers 1 \\
        --log-level info\
"

echo "Pipeline started in tmux session: $TMUX_SESSION_NAME"
