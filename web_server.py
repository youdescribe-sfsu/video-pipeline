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
from typing import Dict
from shutil import rmtree

from web_server_module.web_server_types import WebServerRequest
from web_server_module.custom_logger import setup_logger
from pipeline_module.pipeline_runner import PipelineRunner
from web_server_module.web_server_database import (
    create_database, process_incoming_data, update_status,
    get_data_for_youtube_id_ai_user_id, StatusEnum, get_status_for_youtube_id,
    remove_sqlite_entry
)
from service_manager import ServiceManager
from pipeline_module.utils_module.utils import return_video_folder_name

# Load environment variables and setup logging
logger = setup_logger()

# Constants
MAX_WORKERS = int(os.getenv("MAX_PIPELINE_WORKERS", "4"))

# Service configurations
YOLO_SERVICES = [
    {"port": "8087", "gpu": "2"}
]

CAPTION_SERVICES = [
    {"port": "8085", "gpu": "4"},
    {"port": "8093", "gpu": "3"},
    {"port": "8095", "gpu": "1"}
]

RATING_SERVICES = [
    {"port": "8082", "gpu": "4"},
    {"port": "8092", "gpu": "3"},
    {"port": "8094", "gpu": "1"}
]

# Global variables
active_tasks = set()
task_queue = asyncio.Queue()
event_loop = None
thread_pool = ThreadPoolExecutor(max_workers=MAX_WORKERS)
service_manager = None

async def handle_pipeline_failure(youtube_id: str, ai_user_id: str, ydx_server: str, ydx_app_host: str):
    """Handle pipeline failures with proper cleanup and notification"""
    try:
        # Clean up pipeline resources
        await cleanup_failed_pipeline(youtube_id, "Pipeline processing failed", ai_user_id)

        # Remove database entry
        await remove_sqlite_entry(youtube_id, ai_user_id)

        # Notify YouDescribe service about the failure
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
            logger.info(f"Sent failure notification for video {youtube_id}")

    except Exception as e:
        logger.error(f"Error handling pipeline failure: {str(e)}")
        logger.error(traceback.format_exc())


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Enhanced lifecycle manager with proper error handling"""
    global event_loop, service_manager
    processor = None

    try:
        # Startup
        logger.info("Starting application...")
        create_database()
        logger.info("Database initialized")

        # Initialize service manager with proper error handling
        service_manager = ServiceManager(
            yolo_services=YOLO_SERVICES,
            caption_services=CAPTION_SERVICES,
            rating_services=RATING_SERVICES,
            max_workers=MAX_WORKERS
        )

        try:
            await service_manager.initialize()
            logger.info("Service manager initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize service manager: {str(e)}")
            if service_manager:
                await service_manager.cleanup()
            raise RuntimeError(f"Service manager initialization failed: {str(e)}")

        # Start task processor with proper error handling
        event_loop = asyncio.get_event_loop()
        processor = asyncio.create_task(task_processor())
        logger.info("Task processor started")

        yield

    finally:
        # Enhanced cleanup with null checks
        logger.info("Shutting down application...")

        if processor:
            processor.cancel()
            try:
                await processor
            except asyncio.CancelledError:
                pass

        if service_manager:
            try:
                await service_manager.release_all_services()
                await service_manager.cleanup()
            except Exception as e:
                logger.error(f"Error during service cleanup: {str(e)}")

        if thread_pool:
            thread_pool.shutdown(wait=True)

        logger.info("Application shutdown complete")
app = FastAPI(lifespan=lifespan)

# CORS middleware configuration (maintain existing configuration)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

async def cleanup_failed_pipeline(video_id: str, error_message: str, ai_user_id: str = None):
    """Enhanced cleanup for failed pipelines with service management"""
    try:
        # Clean up filesystem resources
        video_folder = return_video_folder_name({"video_id": video_id})
        if os.path.exists(video_folder):
            rmtree(video_folder)
            logger.info(f"Cleaned up video folder: {video_folder}")

        # Clean up database entries
        await remove_sqlite_entry(video_id, ai_user_id)
        logger.info(f"Cleaned up database entries for video {video_id}")

        # Release any held services
        await service_manager.release_all_services()
        logger.info("Released all held services")

    except Exception as e:
        logger.error(f"Cleanup error: {str(e)}")

async def task_processor():
    """Enhanced task processor with proper error handling"""
    while True:
        try:
            if not service_manager or not service_manager._initialized:
                logger.error("Service manager not properly initialized")
                await asyncio.sleep(5)  # Wait before retry
                continue

            data = await task_queue.get()
            youtube_id = data["youtube_id"]
            ai_user_id = data["ai_user_id"]
            task_key = (youtube_id, ai_user_id)

            try:
                # Process task
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
                if task_key:  # Check if task_key was defined
                    active_tasks.discard(task_key)
                task_queue.task_done()

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Task processor error: {str(e)}")
            logger.error(traceback.format_exc())

# Maintain existing endpoint handlers with minor modifications
@app.post("/generate_ai_caption")
async def generate_ai_caption(post_data: WebServerRequest):
    """Handle incoming AI caption generation requests"""
    try:
        if not post_data.youtube_id or not post_data.AI_USER_ID:
            raise HTTPException(status_code=400, detail="Missing required fields")

        task_key = (post_data.youtube_id, post_data.AI_USER_ID)

        if task_key in active_tasks:
            return {
                "status": "pending",
                "message": "Task already in progress"
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

        active_tasks.add(task_key)

        # Queue task
        await task_queue.put({
            "youtube_id": post_data.youtube_id,
            "ai_user_id": post_data.AI_USER_ID,
            "ydx_server": post_data.ydx_server,
            "ydx_app_host": post_data.ydx_app_host
        })

        return {
            "status": "accepted",
            "message": "Task queued for processing"
        }

    except Exception as e:
        logger.error(f"Error in generate_ai_caption: {str(e)}")
        logger.error(traceback.format_exc())
        active_tasks.discard(task_key)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health_check")
async def health_check():
    """Enhanced health check endpoint with proper error handling"""
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

@app.get("/service_stats")
async def get_service_stats():
    """Enhanced service statistics endpoint with proper error handling"""
    try:
        if not service_manager or not service_manager._initialized:
            raise HTTPException(
                status_code=503,
                detail="Service manager not initialized"
            )

        stats = await service_manager.get_stats()
        return {
            "status": "success",
            "stats": stats,
            "timestamp": datetime.now().isoformat(),
            "queue_info": {
                "active_tasks": len(active_tasks),
                "queue_size": task_queue.qsize()
            }
        }
    except Exception as e:
        logger.error(f"Error getting service stats: {str(e)}")
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