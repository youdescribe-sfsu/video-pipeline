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
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from contextlib import asynccontextmanager
import multiprocessing

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
MAX_WORKERS = int(os.getenv("MAX_PIPELINE_WORKERS", "2"))

# Tracking active tasks and process pool
active_tasks = set()
process_pool = None


def run_pipeline_sync(video_id, video_end_time, video_start_time, upload_to_server,
                      tasks, ydx_server, ydx_app_host, userId, AI_USER_ID):
    """Synchronous wrapper for pipeline execution"""
    try:
        # Direct synchronous call to run_pipeline
        run_pipeline(
            video_id=video_id,
            video_end_time=video_end_time,
            video_start_time=video_start_time,
            upload_to_server=upload_to_server,
            tasks=tasks,
            ydx_server=ydx_server,
            ydx_app_host=ydx_app_host,
            userId=userId,
            AI_USER_ID=AI_USER_ID
        )
        return True
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        return False


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle manager"""
    global process_pool

    logger.info("Starting application...")

    # Initialize database
    create_database()
    logger.info("Database initialized")

    # Initialize process pool for heavy pipeline work
    process_pool = ProcessPoolExecutor(max_workers=MAX_WORKERS)
    logger.info(f"Process pool initialized with {MAX_WORKERS} workers")

    yield

    # Cleanup on shutdown
    if process_pool:
        process_pool.shutdown(wait=True)
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


async def handle_pipeline_failure(youtube_id: str, ai_user_id: str, ydx_server: str, ydx_app_host: str):
    """Handle pipeline failures"""
    try:
        # Remove database entry
        await remove_sqlite_entry(youtube_id, ai_user_id)

        # Notify YouDescribe service
        url = f"{ydx_server}/api/users/pipeline-failure"
        async with aiohttp.ClientSession() as session:
            await session.post(url, json={
                "youtube_id": youtube_id,
                "ai_user_id": ai_user_id,
                "error_message": "Pipeline processing failed",
                "ydx_app_host": ydx_app_host
            })

    except Exception as e:
        logger.error(f"Error handling pipeline failure: {str(e)}")
        logger.error(traceback.format_exc())


async def process_pipeline_task(youtube_id: str, ai_user_id: str, ydx_server: str, ydx_app_host: str):
    """Process a single pipeline task"""
    task_key = (youtube_id, ai_user_id)

    try:
        # Run pipeline in process pool using synchronous wrapper
        loop = asyncio.get_event_loop()
        success = await loop.run_in_executor(
            process_pool,
            run_pipeline_sync,
            youtube_id,  # video_id
            None,  # video_end_time
            None,  # video_start_time
            True,  # upload_to_server
            None,  # tasks
            ydx_server,  # ydx_server
            ydx_app_host,  # ydx_app_host
            None,  # userId
            ai_user_id  # AI_USER_ID
        )

        if success:
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
        else:
            raise Exception("Pipeline execution failed")

    except Exception as e:
        logger.error(f"Pipeline failed for {youtube_id}: {str(e)}")
        logger.error(traceback.format_exc())
        update_status(youtube_id, ai_user_id, StatusEnum.failed.value)
        await handle_pipeline_failure(youtube_id, ai_user_id, ydx_server, ydx_app_host)

    finally:
        # Always remove from active tasks
        active_tasks.discard(task_key)


@app.post("/generate_ai_caption")
async def generate_ai_caption(post_data: WebServerRequest, background_tasks: BackgroundTasks):
    """Handle incoming AI caption generation requests"""
    try:
        # Validate request
        if not post_data.youtube_id or not post_data.AI_USER_ID:
            raise HTTPException(status_code=400, detail="Missing required fields")

        # Create task key for tracking
        task_key = (post_data.youtube_id, post_data.AI_USER_ID)

        # Check if task already running
        if task_key in active_tasks:
            return {
                "status": "pending",
                "message": "Task already in progress"
            }

        # Check service availability
        if not process_pool:
            raise HTTPException(
                status_code=503,
                detail="Processing service unavailable"
            )

        # Process request data
        data_json = json.loads(post_data.model_dump_json())
        process_incoming_data(
            data_json['user_id'],
            data_json['ydx_server'],
            data_json['ydx_app_host'],
            data_json['AI_USER_ID'],
            data_json['youtube_id']
        )

        # Add to active tasks
        active_tasks.add(task_key)

        # Queue task in background
        background_tasks.add_task(
            process_pipeline_task,
            post_data.youtube_id,
            post_data.AI_USER_ID,
            post_data.ydx_server,
            post_data.ydx_app_host
        )

        return {
            "status": "accepted",
            "message": "Task queued for processing"
        }

    except Exception as e:
        logger.error(f"Error in generate_ai_caption: {str(e)}")
        logger.error(traceback.format_exc())
        # Remove from active tasks if added
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
            "process_pool": "active" if process_pool else "inactive"
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run("web_server:app", host="0.0.0.0", port=8086, reload=True)