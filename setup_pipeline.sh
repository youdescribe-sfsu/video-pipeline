#!/bin/bash

# Update for start_pipeline.sh

# Set the number of workers for the pipeline
export MAX_PIPELINE_WORKERS=1

# Define the tmux session name
TMUX_SESSION_NAME="pipeline_server"

# Activate the virtual environment
source venv/bin/activate

# Start the pipeline in a tmux session
tmux new-session -d -s "$TMUX_SESSION_NAME" bash -c "\
    uvicorn web_server:app \\
        --host 0.0.0.0 \\
        --port 8086 \\
        --workers 4 \\
        --log-level info\
"

echo "Pipeline started in tmux session: $TMUX_SESSION_NAME"