#!/bin/bash

# Set the number of workers for the pipeline
export MAX_PIPELINE_WORKERS=4

# Define the tmux session names for the web server and RQ worker
TMUX_SESSION_UVICORN="pipeline_server"
TMUX_SESSION_RQ="pipeline_rq_worker"

# Activate the virtual environment
source venv/bin/activate

# Start the Uvicorn server in a tmux session
tmux new-session -d -s "$TMUX_SESSION_UVICORN" bash -c "\
    uvicorn web_server:app \
        --host 0.0.0.0 \
        --port 8086 \
        --workers \$MAX_PIPELINE_WORKERS \
        --log-level info\
"

# Start the RQ worker for the global Redis queue in a separate tmux session
tmux new-session -d -s "$TMUX_SESSION_RQ" bash -c "\
    rq worker video_tasks --url redis://localhost:6379\
"

echo "Pipeline started in tmux sessions:"
echo " - Uvicorn server session: $TMUX_SESSION_UVICORN"
echo " - RQ worker session: $TMUX_SESSION_RQ"
