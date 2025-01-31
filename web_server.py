# web_server.py
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import aiohttp
import os
import json
import traceback
import psutil
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from shutil import rmtree

from web_server_module.web_server_types import WebServerRequest
from web_server_module.custom_logger import setup_logger
from pipeline_module.pipeline_runner import PipelineRunner
from web_server_module.web_server_database import (
    create_database, process_incoming_data, update_status, StatusEnum, get_status_for_youtube_id,
    remove_sqlite_entry, get_pending_jobs_with_youtube_ids,
    get_data_for_youtube_id_and_user_id
)
from service_manager import ServiceManager
from pipeline_module.utils_module.utils import return_video_folder_name

# Constants
MAX_WORKERS = int(os.getenv("MAX_PIPELINE_WORKERS", "4"))

# Service configurations
YOLO_SERVICES = [
    {"port": "8087", "gpu": "2"}
]

CAPTION_SERVICES = [
    {"port": "8085", "gpu": "4"},
]

RATING_SERVICES = [
    {"port": "8082", "gpu": "4"},
]

# Setup logging
logger = setup_logger()

# Global variables
active_tasks = set()
task_queue = asyncio.Queue()
event_loop = None
thread_pool = ThreadPoolExecutor(max_workers=MAX_WORKERS)
service_manager = None


async def handle_pipeline_failure(youtube_id: str, ai_user_id: str, ydx_server: str, ydx_app_host: str):
    """Handle pipeline failures with cleanup and notification"""
    try:
        await cleanup_failed_pipeline(youtube_id, "Pipeline processing failed", ai_user_id)
        await remove_sqlite_entry(youtube_id, ai_user_id)

        error_notification_url = f"{ydx_server}/api/users/pipeline-failure"
        async with aiohttp.ClientSession() as session:
            await session.post(
                error_notification_url,
                json={
                    "youtube_id": youtube_id,
                    "ai_user_id": ai_user_id,
                    "error_message": "Pipeline processing failed",
                    "ydx_app_host": ydx_app_host
                }
            )
    except Exception as e:
        logger.error(f"Error handling pipeline failure: {str(e)}")
        logger.error(traceback.format_exc())


async def cleanup_failed_pipeline(youtube_id: str, error_message: str, ai_user_id: str = None):
    """Clean up resources for failed pipeline"""
    try:
        video_folder = return_video_folder_name({"video_id": youtube_id})
        if os.path.exists(video_folder):
            rmtree(video_folder)
        await remove_sqlite_entry(youtube_id, ai_user_id)
        await service_manager.release_all_services()
    except Exception as e:
        logger.error(f"Cleanup error: {str(e)}")


async def recover_pending_tasks():
    """Recover tasks that were in progress when server was shut down"""
    try:
        pending_jobs = get_pending_jobs_with_youtube_ids()
        for job in pending_jobs:
            youtube_id = job['youtube_id']
            ai_user_id = job['ai_user_id']

            task_data = get_data_for_youtube_id_and_user_id(youtube_id, ai_user_id)
            if task_data and len(task_data) > 0:
                task = task_data[0]
                await task_queue.put({
                    "youtube_id": youtube_id,
                    "ai_user_id": ai_user_id,
                    "ydx_server": task['ydx_server'],
                    "ydx_app_host": task['ydx_app_host']
                })
                logger.info(f"Recovered task for YouTube ID: {youtube_id}")
    except Exception as e:
        logger.error(f"Error recovering tasks: {str(e)}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan manager with queue recovery"""
    global event_loop, service_manager
    processor = None

    try:
        logger.info("Starting application...")
        create_database()

        # Initialize service manager
        service_manager = ServiceManager(
            yolo_services=YOLO_SERVICES,
            caption_services=CAPTION_SERVICES,
            rating_services=RATING_SERVICES,
            max_workers=MAX_WORKERS
        )
        await service_manager.initialize()

        # Recover pending tasks
        await recover_pending_tasks()

        # Start task processor
        event_loop = asyncio.get_event_loop()
        processor = asyncio.create_task(task_processor())

        yield

    except Exception as e:
        logger.error(f"Application startup failed: {str(e)}")
        if service_manager:
            await service_manager.cleanup()
        raise

    finally:
        logger.info("Shutting down application...")
        if processor:
            processor.cancel()
            try:
                await processor
            except asyncio.CancelledError:
                pass
        if service_manager:
            await service_manager.cleanup()
        if thread_pool:
            thread_pool.shutdown(wait=True)


app = FastAPI(lifespan=lifespan)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/generate_ai_caption")
async def generate_ai_caption(post_data: WebServerRequest):
    """Handle incoming AI caption generation requests"""
    try:
        if not post_data.youtube_id or not post_data.AI_USER_ID:
            raise HTTPException(status_code=400, detail="Missing required fields")

        task_key = (post_data.youtube_id, post_data.AI_USER_ID)
        status = get_status_for_youtube_id(post_data.youtube_id, post_data.AI_USER_ID)

        # Return status if task exists and is completed
        if status == "done":
            return {
                "status": "completed",
                "message": "Task already processed"
            }

        # Process incoming data
        data_json = json.loads(post_data.model_dump_json())
        process_incoming_data(
            data_json['user_id'],
            data_json['ydx_server'],
            data_json['ydx_app_host'],
            data_json['AI_USER_ID'],
            data_json['youtube_id']
        )

        # If task is already queued, return position
        if task_key in active_tasks:
            return {
                "status": "in_progress",
                "message": "Task is being processed",
                "position": task_queue.qsize()
            }

        # Add to tracking and queue
        active_tasks.add(task_key)
        await task_queue.put({
            "youtube_id": post_data.youtube_id,
            "ai_user_id": post_data.AI_USER_ID,
            "ydx_server": post_data.ydx_server,
            "ydx_app_host": post_data.ydx_app_host
        })

        return {
            "status": "accepted",
            "message": "Task queued for processing",
            "position": task_queue.qsize()
        }

    except Exception as e:
        logger.error(f"Error in generate_ai_caption: {str(e)}")
        logger.error(traceback.format_exc())
        if task_key:
            active_tasks.discard(task_key)  # Ensure cleanup on error
        raise HTTPException(status_code=500, detail=str(e))


async def task_processor():
    """Task processor with error handling and queue management"""
    while True:
        try:
            data = await task_queue.get()
            youtube_id = data["youtube_id"]
            ai_user_id = data["ai_user_id"]
            task_key = (youtube_id, ai_user_id)

            try:
                # Check current status
                status = get_status_for_youtube_id(youtube_id, ai_user_id)
                if status == "done":
                    logger.info(f"Task {youtube_id} already completed, skipping")
                    continue

                pipeline_runner = PipelineRunner(
                    video_id=youtube_id,
                    video_start_time=None,
                    video_end_time=None,
                    upload_to_server=True,
                    service_manager=service_manager,
                    ydx_server=data["ydx_server"],
                    ydx_app_host=data["ydx_app_host"],
                    userId=None,
                    AI_USER_ID=ai_user_id,
                )

                await pipeline_runner.run_full_pipeline()
                update_status(youtube_id, ai_user_id, StatusEnum.done.value)
                logger.info(f"Pipeline completed for YouTube ID: {youtube_id}")

            except Exception as e:
                logger.error(f"Pipeline failed for {youtube_id}: {str(e)}")
                logger.error(traceback.format_exc())
                update_status(youtube_id, ai_user_id, StatusEnum.failed.value)
                await handle_pipeline_failure(youtube_id, ai_user_id,
                                              data["ydx_server"],
                                              data["ydx_app_host"])

            finally:
                active_tasks.discard(task_key)
                task_queue.task_done()

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Task processor error: {str(e)}")
            logger.error(traceback.format_exc())
            await asyncio.sleep(1)  # Prevent tight loop on errors


@app.get("/health_check")
async def health_check():
    """Health check endpoint with queue status"""
    try:
        if not service_manager or not service_manager._initialized:
            raise HTTPException(
                status_code=503,
                detail="Service manager not initialized"
            )

        service_health = await service_manager.check_all_services_health()
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "active_tasks": len(active_tasks),
            "queue_size": task_queue.qsize(),
            "worker_id": os.getpid(),
            "memory_usage": psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024,
            "services": service_health
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
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