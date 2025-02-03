from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import os
import json
import traceback
import psutil

# --- Redis & RQ Imports for Global Queue ---
from redis import Redis
from rq import Queue

# --- Application Modules ---
from web_server_module.web_server_types import WebServerRequest
from web_server_module.custom_logger import setup_logger
# Import run_pipeline and our new task processing function
from pipeline_module.pipeline_runner import run_pipeline, process_video_task
from web_server_module.web_server_database import (
    create_database, process_incoming_data, update_status, StatusEnum, get_status_for_youtube_id,
    remove_sqlite_entry, get_pending_jobs_with_youtube_ids, get_data_for_youtube_id_and_user_id
)
from service_manager import ServiceManager
from pipeline_module.utils_module.utils import return_video_folder_name

# --- Constants & Service Configurations ---
MAX_WORKERS = int(os.getenv("MAX_PIPELINE_WORKERS", "4"))

YOLO_SERVICES = [
    {"port": "8087", "gpu": "2"}
]
# For captioning and rating, we enforce single-worker mode by setting max_connections=1
CAPTION_SERVICES = [
    {"port": "8085", "gpu": "4"},
]
RATING_SERVICES = [
    {"port": "8082", "gpu": "4"},
]

# --- Logger Setup ---
logger = setup_logger()

# --- Global Redis Queue Setup ---
redis_conn = Redis(host="localhost", port=6379)
global_task_queue = Queue('video_tasks', connection=redis_conn)

# --- Global Service Manager Variable ---
service_manager = None

# --- Lifespan Management ---
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    global service_manager
    try:
        logger.info("Starting application...")
        create_database()

        # Initialize the Service Manager with your service configurations.
        service_manager = ServiceManager(
            yolo_services=YOLO_SERVICES,
            caption_services=CAPTION_SERVICES,
            rating_services=RATING_SERVICES,
            max_workers=MAX_WORKERS
        )
        await service_manager.initialize()

        yield
    except Exception as e:
        logger.error(f"Application startup failed: {str(e)}")
        if service_manager:
            await service_manager.cleanup()
        raise
    finally:
        logger.info("Shutting down application...")
        if service_manager:
            await service_manager.cleanup()

app = FastAPI(lifespan=lifespan)

# --- CORS Middleware ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Endpoints ---

@app.post("/generate_ai_caption")
async def generate_ai_caption(post_data: WebServerRequest):
    """
    Endpoint to generate AI captions for a video.
    This function processes incoming data and enqueues the video processing
    job in a global Redis-backed queue to ensure strict FIFO ordering.
    """
    try:
        # Validate required fields.
        if not post_data.youtube_id or not post_data.AI_USER_ID:
            raise HTTPException(status_code=400, detail="Missing required fields")

        # Process incoming data: update the database with task info.
        data_json = json.loads(post_data.model_dump_json())
        process_incoming_data(
            data_json['user_id'],
            data_json['ydx_server'],
            data_json['ydx_app_host'],
            data_json['AI_USER_ID'],
            data_json['youtube_id']
        )

        # Enqueue the task into the global Redis-backed queue.
        # The 'process_video_task' function (defined in your pipeline_runner module)
        # will be executed by an RQ worker.
        job = global_task_queue.enqueue(
            process_video_task,
            {
                "youtube_id": post_data.youtube_id,
                "ai_user_id": post_data.AI_USER_ID,
                "ydx_server": post_data.ydx_server,
                "ydx_app_host": post_data.ydx_app_host
            }
        )

        return {
            "status": "accepted",
            "message": "Task queued for processing",
            "job_id": job.id,
            "queue_size": len(global_task_queue)
        }

    except Exception as e:
        logger.error(f"Error in generate_ai_caption: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health_check")
async def health_check():
    """
    Health check endpoint that returns system status,
    including current global queue size and service health.
    """
    try:
        if not service_manager:
            raise HTTPException(status_code=503, detail="Service manager not initialized")

        # Get overall service health status from the service manager.
        service_health = await service_manager.check_all_services_health()
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "global_queue_size": len(global_task_queue),
            "worker_id": os.getpid(),
            "memory_usage_mb": psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024,
            "services": service_health
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# --- Main Entry Point ---
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "web_server:app",
        host="0.0.0.0",
        port=8086,
        workers=MAX_WORKERS,
        loop="asyncio",
        log_level="info"
    )