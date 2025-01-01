from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import os
import json
import asyncio
import traceback
from datetime import datetime
import uvicorn
import aiohttp
from contextlib import asynccontextmanager
import aiosmtplib
from email.message import EmailMessage

# Import custom modules
from web_server_module.web_server_types import WebServerRequest
from web_server_module.custom_logger import setup_logger
from pipeline_module.pipeline_runner import run_pipeline, cleanup_failed_pipeline
from web_server_module.web_server_database import (
    create_database, process_incoming_data, update_status, update_ai_user_data,
    get_data_for_youtube_id_ai_user_id, get_data_for_youtube_id_and_user_id,
    StatusEnum, remove_sqlite_entry
)

# Load environment variables and setup logger
load_dotenv()
logger = setup_logger()

# Global variables
GPU_URL = os.getenv("GPU_URL")
AI_USER_ID = os.getenv("AI_USER_ID")
YDX_SERVER = os.getenv("YDX_SERVER")

# Queue and tracking set for managing pipeline tasks
pipeline_queue = asyncio.Queue()
enqueued_tasks = set()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager for the FastAPI application"""
    logger.info("Starting application...")
    create_database()
    logger.info("Database initialized")

    # Start queue processing task
    asyncio.create_task(process_queue())
    logger.info("Queue processing task started")

    yield

    logger.info("Application shutting down...")


app = FastAPI(lifespan=lifespan)

# CORS middleware configuration
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
        data_json = json.loads(post_data.model_dump_json())
        if not post_data.youtube_id or not post_data.AI_USER_ID:
            raise HTTPException(status_code=400, detail="Missing required fields")

        logger.info(f'Processing request for video: {post_data.youtube_id}')

        # Process incoming data in database
        process_incoming_data(
            data_json['user_id'],
            data_json['ydx_server'],
            data_json['ydx_app_host'],
            data_json['AI_USER_ID'],
            data_json['youtube_id']
        )

        # Add task to queue if not already enqueued
        task_key = (post_data.youtube_id, post_data.AI_USER_ID)
        if task_key not in enqueued_tasks:
            await pipeline_queue.put(post_data)
            enqueued_tasks.add(task_key)
            logger.info(f"Task enqueued: {task_key}")

        return {"status": "success", "message": "AI caption generation request queued"}

    except Exception as e:
        logger.error(f"Error in generate_ai_caption: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


async def process_queue():
    """Background task to process the pipeline queue"""
    logger.info("Queue processing started")
    while True:
        task = None
        try:
            # Wait for next task
            task = await pipeline_queue.get()

            # Process the task
            await run_pipeline_task(
                youtube_id=task.youtube_id,
                ai_user_id=task.AI_USER_ID,
                ydx_server=task.ydx_server,
                ydx_app_host=task.ydx_app_host
            )

        except asyncio.CancelledError:
            logger.info("Queue processing task was cancelled")
            if task:
                logger.info(f"Cleaning up task: {task.youtube_id}")
            raise

        except Exception as e:
            logger.error(f"Error processing queue: {str(e)}")
            logger.error(traceback.format_exc())

            if task:
                await handle_pipeline_failure(
                    task.youtube_id,
                    task.AI_USER_ID,
                    str(e),
                    task.ydx_server,
                    task.ydx_app_host
                )

        finally:
            # Remove task from tracking set
            if task:
                enqueued_tasks.discard((task.youtube_id, task.AI_USER_ID))
                pipeline_queue.task_done()


async def run_pipeline_task(youtube_id: str, ai_user_id: str, ydx_server: str, ydx_app_host: str):
    """Execute a single pipeline task"""
    logger.info(f"Processing pipeline task: {youtube_id}")
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
        logger.error(f"Pipeline failed for YouTube ID {youtube_id}: {str(e)}")
        logger.error(traceback.format_exc())
        update_status(youtube_id, ai_user_id, StatusEnum.failed.value)
        raise


async def handle_pipeline_failure(youtube_id: str, ai_user_id: str, error_message: str,
                                  ydx_server: str, ydx_app_host: str):
    """Handle pipeline failures gracefully"""
    logger.error(f"Pipeline failed for YouTube ID: {youtube_id}")

    try:
        # Cleanup failed pipeline
        await cleanup_failed_pipeline(youtube_id, ai_user_id, error_message)

        # Remove from database
        await remove_sqlite_entry(youtube_id, ai_user_id)

        # Notify services
        await notify_youdescribe_service(youtube_id, ai_user_id, error_message, ydx_server, ydx_app_host)
        await notify_admin(youtube_id, ai_user_id, error_message)

    except Exception as e:
        logger.error(f"Error in failure handling: {str(e)}")
        logger.error(traceback.format_exc())


async def notify_youdescribe_service(youtube_id: str, ai_user_id: str, error_message: str,
                                     ydx_server: str, ydx_app_host: str):
    """Notify YouDescribe service about pipeline failure"""
    url = f"{ydx_server}/api/users/pipeline-failure"
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
    """Send email notification to admin about pipeline failure"""
    # SMTP Configuration
    SMTP_HOST = os.getenv("SMTP_HOST")
    SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USERNAME = os.getenv("SMTP_USERNAME")
    SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
    SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "True").lower() == "true"

    ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "admin@example.com")
    SENDER_EMAIL = os.getenv("SENDER_EMAIL", SMTP_USERNAME)

    if not all([SMTP_HOST, SMTP_USERNAME, SMTP_PASSWORD]):
        logger.error("Missing SMTP configuration")
        return

    message = EmailMessage()
    message["From"] = SENDER_EMAIL
    message["To"] = ADMIN_EMAIL
    message["Subject"] = f"Pipeline Failure - Video {youtube_id}"

    content = f"""
    Pipeline Failure Notification

    Video ID: {youtube_id}
    AI User ID: {ai_user_id}
    Error: {error_message}
    Time: {datetime.now().isoformat()}

    Please investigate this issue.
    """
    message.set_content(content)

    try:
        await aiosmtplib.send(
            message,
            hostname=SMTP_HOST,
            port=SMTP_PORT,
            username=SMTP_USERNAME,
            password=SMTP_PASSWORD,
            use_tls=SMTP_USE_TLS
        )
        logger.info(f"Admin notification sent for {youtube_id}")
    except Exception as e:
        logger.error(f"Failed to send admin notification: {str(e)}")


@app.get("/health_check")
async def health_check():
    """Health check endpoint"""
    try:
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "queue_size": pipeline_queue.qsize(),
            "active_tasks": len(enqueued_tasks)
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run("web_server:app", host="0.0.0.0", port=8086, reload=True)