from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import aiohttp
import os
import json
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import Dict
import traceback
import psutil

from web_server_module.web_server_types import WebServerRequest
from web_server_module.custom_logger import setup_logger
from pipeline_module.pipeline_runner import run_pipeline
from web_server_module.web_server_database import (
    create_database, process_incoming_data, update_status,
    get_data_for_youtube_id_ai_user_id, StatusEnum, get_status_for_youtube_id
)
from service_manager import ServiceManager

# Load environment variables and setup logging
logger = setup_logger()

# Constants
MAX_WORKERS = int(os.getenv("MAX_PIPELINE_WORKERS", "4"))

# Service configurations
YOLO_SERVICES = [
    {"port": "8087", "gpu": "2"}  # Using GPU 2 for YOLO as specified
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

# Initialize service manager
service_manager = ServiceManager(
    yolo_services=YOLO_SERVICES,
    caption_services=CAPTION_SERVICES,
    rating_services=RATING_SERVICES,
    max_workers=MAX_WORKERS
)

# Replace the old on_event with new lifespan context manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager for the FastAPI application"""
    global event_loop

    # Startup
    logger.info("Starting application...")
    create_database()
    logger.info("Database initialized")

    # Start task processor
    event_loop = asyncio.get_event_loop()
    processor = asyncio.create_task(task_processor())
    logger.info("Task processor started")

    try:
        yield  # Application runs here
    finally:
        # Cleanup on shutdown
        logger.info("Shutting down application...")

        # Cancel task processor
        processor.cancel()
        try:
            await processor
        except asyncio.CancelledError:
            pass

        # Clean up thread pool
        thread_pool.shutdown(wait=True)

        # Clean up service manager
        await service_manager.cleanup()

        # Close any remaining connections
        sessions = [session for session in aiohttp.ClientSession._instances]
        for session in sessions:
            try:
                await session.close()
            except:
                pass

        logger.info("Application shutdown complete")

# Initialize FastAPI with lifespan manager
app = FastAPI(lifespan=lifespan)

# CORS middleware configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Rest of your existing code remains the same
async def task_processor():
    """Process tasks from queue"""
    while True:
        try:
            data = await task_queue.get()
            asyncio.create_task(process_task(data))
            task_queue.task_done()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Task processor error: {str(e)}")
            logger.error(traceback.format_exc())


async def process_task(data: Dict):
    """Process a single task"""
    youtube_id = data["youtube_id"]
    ai_user_id = data["ai_user_id"]
    task_key = (youtube_id, ai_user_id)

    try:
        # Get services for this task
        services = await service_manager.get_services(f"{youtube_id}_{ai_user_id}")

        # Run pipeline
        await run_pipeline(
            video_id=youtube_id,
            service_urls=services,
            video_start_time=None,
            video_end_time=None,
            upload_to_server=True,
            ydx_server=data["ydx_server"],
            ydx_app_host=data["ydx_app_host"],
            userId=None,
            AI_USER_ID=ai_user_id
        )

        # Update status
        update_status(youtube_id, ai_user_id, StatusEnum.done.value)
        logger.info(f"Pipeline completed for YouTube ID: {youtube_id}")

    except Exception as e:
        logger.error(f"Pipeline failed for {youtube_id}: {str(e)}")
        logger.error(traceback.format_exc())
        update_status(youtube_id, ai_user_id, StatusEnum.failed.value)

    finally:
        active_tasks.discard(task_key)
        await service_manager.release_task_services(f"{youtube_id}_{ai_user_id}")


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
    """Health check endpoint"""
    try:
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "active_tasks": len(active_tasks),
            "queue_size": task_queue.qsize(),
            "worker_id": os.getpid(),
            "memory_usage": psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024  # MB
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/service_stats")
async def get_service_stats():
    """Get service statistics"""
    try:
        return {
            "status": "success",
            "stats": service_manager.get_stats(),
            "timestamp": datetime.now().isoformat(),
            "queue_info": {
                "active_tasks": len(active_tasks),
                "queue_size": task_queue.qsize()
            }
        }
    except Exception as e:
        logger.error(f"Error getting service stats: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Main entry point
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