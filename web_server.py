from fastapi import FastAPI, HTTPException, status, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import os
import json
import asyncio
import traceback
from datetime import datetime
import uvicorn
import aiohttp
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from itertools import cycle
from typing import Dict, List
from threading import Lock

# Import custom modules
from web_server_module.web_server_types import WebServerRequest
from web_server_module.custom_logger import setup_logger
from pipeline_module.pipeline_runner import run_pipeline
from web_server_module.web_server_database import (
    create_database, process_incoming_data, update_status, update_ai_user_data,
    get_data_for_youtube_id_ai_user_id, get_data_for_youtube_id_and_user_id,
    StatusEnum, remove_sqlite_entry
)

# Load environment variables
load_dotenv()
logger = setup_logger()

# Global variables
GPU_URL = os.getenv("GPU_URL")
AI_USER_ID = os.getenv("AI_USER_ID")
YDX_SERVER = os.getenv("YDX_SERVER")
MAX_WORKERS = int(os.getenv("MAX_PIPELINE_WORKERS", "6"))  # Increased for multiple GPUs

# Service configurations for different GPUs
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

YOLO_SERVICES = [
    {"port": "8087", "gpu": "4"},
    {"port": "8088", "gpu": "3"},
    {"port": "8089", "gpu": "1"}
]

class ServiceBalancer:
    """Load balancer for distributing tasks across services"""

    def __init__(self, services: List[Dict[str, str]]):
        self.services = cycle(services)
        self.lock = Lock()

    def get_next_service(self) -> Dict[str, str]:
        """Get next available service using round-robin"""
        with self.lock:
            return next(self.services)


# Initialize service balancers
caption_balancer = ServiceBalancer(CAPTION_SERVICES)
rating_balancer = ServiceBalancer(RATING_SERVICES)
yolo_balancer = ServiceBalancer(YOLO_SERVICES)

# Task management
active_tasks = set()
task_queue = asyncio.Queue()
event_loop = None
thread_pool = ThreadPoolExecutor(max_workers=MAX_WORKERS)


def get_service_urls(youtube_id: str) -> tuple:
    """Get next available service URLs using round-robin"""
    caption_service = caption_balancer.get_next_service()
    rating_service = rating_balancer.get_next_service()
    yolo_service = yolo_balancer.get_next_service()
    logger.info(
        f"Using caption service on GPU {caption_service['gpu']} and rating service on GPU {rating_service['gpu']} for video {youtube_id}")
    return (
        f"http://localhost:{caption_service['port']}/upload",
        f"http://localhost:{rating_service['port']}/api"
        f"http://localhost:{yolo_service['port']}/detect_batch_folder"
    )


def run_sync_pipeline(video_id: str, ydx_server: str, ydx_app_host: str, ai_user_id: str):
    """Synchronous execution of pipeline with load-balanced services"""
    # Get service URLs for this pipeline run
    caption_url, rating_url = get_service_urls(video_id)

    # Set them in environment for the pipeline run
    os.environ["CAPTION_SERVICE_URL"] = caption_url
    os.environ["RATING_SERVICE_URL"] = rating_url
    os.environ["YOLO_SERVICE_URL"] = rating_url

    return asyncio.run(run_pipeline(
        video_id=video_id,
        video_end_time=None,
        video_start_time=None,
        upload_to_server=True,
        tasks=None,
        ydx_server=ydx_server,
        ydx_app_host=ydx_app_host,
        userId=None,
        AI_USER_ID=ai_user_id
    ))


async def notify_youdescribe(youtube_id: str, ai_user_id: str, ydx_server: str, ydx_app_host: str):
    """Notify YouDescribe service about pipeline failure"""
    try:
        url = f"{ydx_server}/api/users/pipeline-failure"
        async with aiohttp.ClientSession() as session:
            await session.post(url, json={
                "youtube_id": youtube_id,
                "ai_user_id": ai_user_id,
                "error_message": "Pipeline processing failed",
                "ydx_app_host": ydx_app_host
            })
    except Exception as e:
        logger.error(f"Failed to notify YouDescribe: {str(e)}")


async def process_task(data: dict):
    """Process a single pipeline task"""
    youtube_id = data["youtube_id"]
    ai_user_id = data["ai_user_id"]
    ydx_server = data["ydx_server"]
    ydx_app_host = data["ydx_app_host"]
    task_key = (youtube_id, ai_user_id)

    try:
        # Run pipeline in thread pool
        await asyncio.get_event_loop().run_in_executor(
            thread_pool,
            run_sync_pipeline,
            youtube_id,
            ydx_server,
            ydx_app_host,
            ai_user_id
        )

        # Update status on success
        update_status(youtube_id, ai_user_id, StatusEnum.done.value)

        # Update user data
        user_data = get_data_for_youtube_id_and_user_id(youtube_id, ai_user_id)
        for data in user_data:
            update_ai_user_data(
                youtube_id=youtube_id,
                ai_user_id=ai_user_id,
                user_id=data.get("user_id"),
                status=StatusEnum.done.value
            )

        logger.info(f"Pipeline completed for YouTube ID: {youtube_id}")

    except Exception as e:
        logger.error(f"Pipeline failed for {youtube_id}: {str(e)}")
        logger.error(traceback.format_exc())
        update_status(youtube_id, ai_user_id, StatusEnum.failed.value)
        await notify_youdescribe(youtube_id, ai_user_id, ydx_server, ydx_app_host)
        await remove_sqlite_entry(youtube_id, ai_user_id)

    finally:
        active_tasks.discard(task_key)


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


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle manager"""
    global event_loop

    logger.info("Starting application...")
    create_database()
    logger.info("Database initialized")

    # Start task processor
    event_loop = asyncio.get_event_loop()
    processor = asyncio.create_task(task_processor())
    logger.info("Task processor started")

    yield

    # Cleanup
    processor.cancel()
    thread_pool.shutdown(wait=True)
    logger.info("Application shutdown complete")


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

        if task_key in active_tasks:
            return {
                "status": "pending",
                "message": "Task already in progress"
            }

        data_json = json.loads(post_data.model_dump_json())
        process_incoming_data(
            data_json['user_id'],
            data_json['ydx_server'],
            data_json['ydx_app_host'],
            data_json['AI_USER_ID'],
            data_json['youtube_id']
        )

        active_tasks.add(task_key)

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
            "queue_size": task_queue.qsize()
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run("web_server:app", host="0.0.0.0", port=8086, reload=True)