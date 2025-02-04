"""
web_server.py - FastAPI Web Server for Video Processing Pipeline

This module implements the web server that handles video processing requests,
following a single-instance architecture for each service type. It manages
task queuing and service health monitoring through a simplified ServiceManager.
"""
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import os
import json
import traceback
import psutil
from rq import Worker

from queue_config import global_task_queue, caption_queue, get_queue_for_task, redis_conn
from web_server_module.web_server_types import WebServerRequest
from web_server_module.custom_logger import setup_logger
from pipeline_module.pipeline_runner import run_pipeline
from web_server_module.web_server_database import (
    create_database, process_incoming_data, update_status, StatusEnum,
    get_status_for_youtube_id, remove_sqlite_entry, get_pending_jobs_with_youtube_ids,
    get_data_for_youtube_id_and_user_id, get_module_output
)
from pipeline_module.global_task_processor import process_video_task
from service_manager import ServiceManager
from pipeline_module.utils_module.utils import return_video_folder_name

# Environment Configuration
MAX_WORKERS = int(os.getenv("MAX_PIPELINE_WORKERS", "4"))

# Logger Setup
logger = setup_logger()

# Global Service Manager Instance
service_manager = None


def needs_captioning(video_id: str, ai_user_id: str) -> bool:
    """
    Determines if a video needs captioning by checking prerequisites and status.

    This function ensures we don't duplicate work and maintains proper task ordering
    by verifying all required preprocessing steps are complete before allowing
    captioning to proceed.
    """
    # First check if captioning is already done
    if get_status_for_youtube_id(video_id, ai_user_id) == "done":
        logger.info(f"Video {video_id} already has captions")
        return False

    # Verify all prerequisites are complete
    required_modules = ["frame_extraction", "object_detection", "keyframe_selection"]
    prerequisites_met = all(
        get_module_output(video_id, ai_user_id, module) is not None
        for module in required_modules
    )

    if not prerequisites_met:
        logger.info(f"Prerequisites not met for video {video_id}")

    return prerequisites_met


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages the application lifecycle, including service initialization and cleanup.
    """
    global service_manager
    try:
        logger.info("Starting application...")
        create_database()

        # Initialize ServiceManager with single-instance configuration
        service_manager = ServiceManager()
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

# CORS Configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/generate_ai_caption")
async def generate_ai_caption(post_data: WebServerRequest):
    """
    Endpoint to generate AI captions for a video.

    This endpoint handles the initial processing of video caption requests,
    routing tasks to appropriate queues based on their current processing stage
    and maintaining strict FIFO ordering through Redis-backed queues.
    """
    try:
        # Validate required fields
        if not post_data.youtube_id or not post_data.AI_USER_ID:
            raise HTTPException(status_code=400, detail="Missing required fields")

        # Process incoming data and update database
        data_json = json.loads(post_data.model_dump_json())
        process_incoming_data(
            data_json['user_id'],
            data_json['ydx_server'],
            data_json['ydx_app_host'],
            data_json['AI_USER_ID'],
            data_json['youtube_id']
        )

        # Prepare task data with explicit type
        task_data = {
            "youtube_id": post_data.youtube_id,
            "ai_user_id": post_data.AI_USER_ID,
            "ydx_server": post_data.ydx_server,
            "ydx_app_host": post_data.ydx_app_host,
            "task_type": "image_captioning" if needs_captioning(post_data.youtube_id,
                                                                post_data.AI_USER_ID) else "general"
        }

        # Route task to appropriate queue and enqueue
        queue = get_queue_for_task(task_data["task_type"])
        job = queue.enqueue(process_video_task, task_data)

        logger.info(f"Task enqueued to {queue.name} queue with job ID {job.id}")

        return {
            "status": "accepted",
            "message": "Task queued for processing",
            "job_id": job.id,
            "queue_type": queue.name
        }

    except Exception as e:
        logger.error(f"Error in generate_ai_caption: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health_check")
async def health_check():
    """
    Comprehensive health check endpoint providing system status and service health.

    Returns detailed information about:
    - Service health status
    - Queue sizes and statistics
    - System resource usage
    - Worker status
    """
    try:
        if not service_manager:
            raise HTTPException(status_code=503, detail="Service manager not initialized")

        # Get service health status
        service_health = await service_manager.check_all_services_health()

        # Get queue statistics
        queue_stats = {
            "global_queue": {
                "size": len(global_task_queue),
                "active_jobs": len(global_task_queue.jobs),
                "failed_jobs": len(global_task_queue.failed_job_registry)
            },
            "caption_queue": {
                "size": len(caption_queue),
                "active_jobs": len(caption_queue.jobs),
                "failed_jobs": len(caption_queue.failed_job_registry)
            }
        }

        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "queues": queue_stats,
            "worker_id": os.getpid(),
            "memory_usage_mb": psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024,
            "services": service_health
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/queue_status")
async def queue_status():
    """
    Detailed queue status endpoint showing current queue state and worker information.
    """
    try:
        workers = Worker.all(connection=redis_conn)
        worker_info = [{
            "name": worker.name,
            "queue_names": [queue.name for queue in worker.queues],
            "state": worker.state
        } for worker in workers]

        return {
            "queues": {
                "global_queue": {
                    "size": len(global_task_queue),
                    "active_jobs": len(global_task_queue.jobs),
                    "failed_jobs": len(global_task_queue.failed_job_registry)
                },
                "caption_queue": {
                    "size": len(caption_queue),
                    "active_jobs": len(caption_queue.jobs),
                    "failed_jobs": len(caption_queue.failed_job_registry)
                }
            },
            "redis_connected": redis_conn.ping(),
            "workers": worker_info
        }
    except Exception as e:
        logger.error(f"Error getting queue status: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


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