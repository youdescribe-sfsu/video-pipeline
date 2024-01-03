# #!/bin/bash

# # Start screen session for pipeline_web_server
# screen -dmS pipeline_web_server bash -c 'cd /home/921416519/video-pipeline && source pipeline_env/bin/activate && uvicorn web_server:app --host 0.0.0.0 --port 8086'

# # Start screen session for yolo_service
# screen -dmS yolo_service bash -c 'cd /home/921416519/yolov8_service && source venv/bin/activate && uvicorn app:app --host 0.0.0.0 --port 8087'

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt