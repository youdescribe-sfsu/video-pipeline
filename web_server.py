from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
from dotenv import load_dotenv
import os
import json
import asyncio
import traceback
from datetime import datetime
import queue
import uvicorn
import aiohttp

# Import custom modules
from web_server_module.web_server_types import WebServerRequest
from web_server_module.custom_logger import setup_logger
from pipeline_module.pipeline_runner import run_pipeline, cleanup_failed_pipeline
from web_server_module.web_server_database import (
    create_database, process_incoming_data, update_status, update_ai_user_data,
    get_data_for_youtube_id_ai_user_id, get_data_for_youtube_id_and_user_id,
    StatusEnum, remove_sqlite_entry
)

# Load environment variables
load_dotenv()

# Setup FastAPI app
app = FastAPI()

# Setup CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust this in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Setup logger
logger = setup_logger()

# Global variables
GPU_URL = os.getenv("GPU_URL")
AI_USER_ID = os.getenv("AI_USER_ID")
YDX_SERVER = os.getenv("YDX_SERVER")

# Queue for managing pipeline tasks
pipeline_queue = asyncio.Queue()

# Set for tracking enqueued tasks
enqueued_tasks = set()


@app.on_event("startup")
async def startup_event():
    logger.info("Starting application...")
    create_database()
    logger.info("Database initialized")
    asyncio.create_task(process_queue())
    logger.info("Queue processing task started")


@app.post("/generate_ai_caption")
async def generate_ai_caption(post_data: WebServerRequest):
    try:
        data_json = json.loads(post_data.model_dump_json())
        print("data_json :: {}".format((data_json)))
        logger.info(f"Received request for YouTube ID: {post_data.youtube_id}")

        if not post_data.youtube_id or not post_data.AI_USER_ID:
            raise HTTPException(status_code=400, detail="Missing required fields")

        user_id = data_json['user_id']
        ydx_server = data_json['ydx_server']
        ydx_app_host = data_json['ydx_app_host']
        ai_user_id = data_json['AI_USER_ID']
        youtube_id = data_json['youtube_id']

        process_incoming_data(user_id, ydx_server, ydx_app_host, ai_user_id, youtube_id)

        # Add task to queue if not already enqueued
        task_key = (post_data.youtube_id, post_data.AI_USER_ID)
        if task_key not in enqueued_tasks:
            await pipeline_queue.put(post_data)
            enqueued_tasks.add(task_key)
            logger.info(f"Task added to queue: {task_key}")

        return {"status": "success", "message": "AI caption generation request queued"}

    except Exception as e:
        logger.error(f"Error in generate_ai_caption: {str(e)}")
        logger.error(traceback.format_exc())
        return {"status": "failure", "message": str(e)}, status.HTTP_500_INTERNAL_SERVER_ERROR


async def process_queue():
    logger.info("Queue processing started")
    while True:
        try:
            task = await pipeline_queue.get()
            await asyncio.create_task(run_pipeline_task(
                youtube_id=task.youtube_id,
                ai_user_id=task.AI_USER_ID,
                ydx_server=task.ydx_server,
                ydx_app_host=task.ydx_app_host
            ))
        except Exception as e:
            logger.error(f"Error processing queue: {str(e)}")
            logger.error(traceback.format_exc())
        logger.info("Queue processing iteration completed")


async def handle_pipeline_failure(youtube_id: str, ai_user_id: str, error_message: str, ydx_server: str,
                                  ydx_app_host: str):
    logger.error(f"Pipeline failed for YouTube ID: {youtube_id}, AI User ID: {ai_user_id}")

    # Cleanup failed pipeline
    await cleanup_failed_pipeline(youtube_id, ai_user_id, error_message)

    # Remove SQLite entry
    await remove_sqlite_entry(youtube_id, ai_user_id)

    # Notify YouDescribe service about the failure
    await notify_youdescribe_service(youtube_id, ai_user_id, error_message, ydx_server, ydx_app_host)

    # Notify admin (implement this based on your needs)
    await notify_admin(youtube_id, ai_user_id, error_message)


async def notify_youdescribe_service(youtube_id: str, ai_user_id: str, error_message: str, ydx_server: str,
                                     ydx_app_host: str):
    url = f"{ydx_server}/api/pipeline-failure"
    data = {
        "youtube_id": youtube_id,
        "ai_user_id": ai_user_id,
        "error_message": error_message,
        "ydx_app_host": ydx_app_host
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=data) as response:
            if response.status != 200:
                logger.error(f"Failed to notify YouDescribe service: {await response.text()}")


async def notify_admin(youtube_id: str, ai_user_id: str, error_message: str):
    # Implement email notification to admin
    logger.info(f"Admin notification: Pipeline failed for YouTube ID: {youtube_id}, AI User ID: {ai_user_id}")
    # Add your implementation here


async def run_pipeline_task(youtube_id: str, ai_user_id: str, ydx_server: str, ydx_app_host: str):
    try:
        await run_pipeline(
            video_id=youtube_id,
            video_end_time=None,
            video_start_time=None,
            upload_to_server=True,
            tasks=None,
            ydx_server=ydx_server,
            ydx_app_host=ydx_app_host,
            userId=None,
            AI_USER_ID=ai_user_id,
        )
        update_status(youtube_id, ai_user_id, StatusEnum.done.value)

        user_data = get_data_for_youtube_id_and_user_id(youtube_id, ai_user_id)
        for data in user_data:
            update_ai_user_data(
                youtube_id=youtube_id,
                ai_user_id=ai_user_id,
                user_id=data.get("user_id", None),
                status=StatusEnum.done.value,
            )
        logger.info(f"Pipeline completed for YouTube ID: {youtube_id}")
    except Exception as e:
        logger.error(f"Pipeline failed for YouTube ID {youtube_id}: {str(e)}")
        logger.error(traceback.format_exc())
        update_status(youtube_id, ai_user_id, StatusEnum.failed.value)
        await handle_pipeline_failure(youtube_id, ai_user_id, str(e), ydx_server, ydx_app_host)
    finally:
        # Remove task from enqueued set
        enqueued_tasks.remove((youtube_id, ai_user_id))


@app.get("/ai_description_status/{youtube_id}")
async def ai_description_status(youtube_id: str):
    try:
        status = get_data_for_youtube_id_ai_user_id(youtube_id, AI_USER_ID)
        if not status:
            raise HTTPException(status_code=404, detail="AI description not found")
        return {"status": status}
    except Exception as e:
        logger.error(f"Error in ai_description_status: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/health_check")
async def health_check():
    try:
        return {"status": "OK", "timestamp": datetime.now().isoformat(), "queue_size": pipeline_queue.qsize()}
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return {"status": "Error", "message": str(e)}, status.HTTP_500_INTERNAL_SERVER_ERROR


if __name__ == "__main__":
    uvicorn.run("web_server:app", host="0.0.0.0", port=8086, reload=True)