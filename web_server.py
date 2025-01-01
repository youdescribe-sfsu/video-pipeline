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
task_queue = asyncio.Queue()


def sync_run_pipeline(video_id: str, ydx_server: str, ydx_app_host: str, AI_USER_ID: str):
    """Synchronous pipeline runner"""
    try:
        return run_pipeline(
            video_id=video_id,
            video_end_time=None,
            video_start_time=None,
            upload_to_server=True,
            tasks=None,
            ydx_server=ydx_server,
            ydx_app_host=ydx_app_host,
            userId=None,
            AI_USER_ID=AI_USER_ID
        )
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        raise


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle manager"""
    logger.info("Starting application...")

    # Initialize database
    create_database()
    logger.info("Database initialized")

    # Start task processor
    task_processor = asyncio.create_task(process_task_queue())
    logger.info("Task processor started")

    yield

    # Cleanup
    task_processor.cancel()
    try:
        await task_processor
    except asyncio.CancelledError:
        pass
    logger.info("Application shutdown complete")


app = FastAPI(lifespan=lifespan)

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


async def process_task_queue():
    """Process tasks from the queue"""
    while True:
        try:
            # Get next task
            task = await task_queue.get()
            youtube_id = task["youtube_id"]
            ai_user_id = task["ai_user_id"]
            ydx_server = task["ydx_server"]
            ydx_app_host = task["ydx_app_host"]

            try:
                # Run pipeline in thread pool
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None,  # Use default executor
                    sync_run_pipeline,
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
                await handle_pipeline_failure(youtube_id, ai_user_id, ydx_server, ydx_app_host)

            finally:
                # Always remove from active tasks
                active_tasks.discard((youtube_id, ai_user_id))
                task_queue.task_done()

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in task processor: {str(e)}")
            logger.error(traceback.format_exc())
            continue


@app.post("/generate_ai_caption")
async def generate_ai_caption(post_data: WebServerRequest):
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

        # Add to task queue
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
            "queue_size": task_queue.qsize()
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run("web_server:app", host="0.0.0.0", port=8086, reload=True)