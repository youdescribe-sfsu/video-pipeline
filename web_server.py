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
import os
import traceback

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


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting application...")
    create_database()
    logger.info("Database initialized")
    asyncio.create_task(process_queue())
    logger.info("Queue processing task started")

    yield

    # Shutdown logic goes here
    logger.info("Application shutting down...")


# Apply lifespan to the app

# Setup FastAPI app
app = FastAPI(lifespan=lifespan)


@app.post("/generate_ai_caption")
async def generate_ai_caption(post_data: WebServerRequest):
    try:
        data_json = json.loads(post_data.model_dump_json())
        if not post_data.youtube_id or not post_data.AI_USER_ID:
            raise HTTPException(status_code=400, detail="Missing required fields")

        print(f'data_json - {data_json}')

        user_id = data_json['user_id']
        ydx_server = data_json['ydx_server']
        ydx_app_host = data_json['ydx_app_host']
        ai_user_id = data_json['AI_USER_ID']
        youtube_id = data_json['youtube_id']

        print(f'INFORMATION - {user_id}, {ydx_server}, {ydx_app_host}, {ai_user_id}, {youtube_id}')

        process_incoming_data(user_id, ydx_server, ydx_app_host, ai_user_id, youtube_id)

        # Add task to queue if not already enqueued
        task_key = (post_data.youtube_id, post_data.AI_USER_ID)
        if task_key not in enqueued_tasks:
            await pipeline_queue.put(post_data)
            enqueued_tasks.add(task_key)
            print(f"Task added to queue: {task_key}")

        return {"status": "success", "message": "AI caption generation request queued"}

    except Exception as e:
        logger.error(f"Error in generate_ai_caption: {str(e)}")
        logger.error(traceback.format_exc())
        return {"status": "failure", "message": str(e)}, status.HTTP_500_INTERNAL_SERVER_ERROR


async def process_queue():
    print("Queue processing started")
    while True:
        task = None
        try:
            task = await pipeline_queue.get()  # Fetch task from queue
            await run_pipeline_task(
                youtube_id=task.youtube_id,
                ai_user_id=task.AI_USER_ID,
                ydx_server=task.ydx_server,
                ydx_app_host=task.ydx_app_host
            )
        except asyncio.CancelledError:
            logger.info("Queue processing task was cancelled.")
            if task:
                logger.info(f"Cleaning up task: {task.youtube_id}, {task.AI_USER_ID}")
            raise
        except Exception as e:
            logger.error(f"Error processing queue: {str(e)}")
            logger.error(traceback.format_exc())
        finally:
            if task:
                enqueued_tasks.discard((task.youtube_id, task.AI_USER_ID))
        print("Queue processing iteration completed")


async def clean_up_queue(youtube_id: str, ai_user_id: str):
    global pipeline_queue
    # Create a new queue without the failed task
    new_queue = asyncio.Queue()
    while not pipeline_queue.empty():
        task = await pipeline_queue.get()
        if task.youtube_id != youtube_id or task.AI_USER_ID != ai_user_id:
            await new_queue.put(task)

    # Replace the old queue with the new one
    pipeline_queue = new_queue

    # Remove the task from enqueued_tasks set
    enqueued_tasks.discard((youtube_id, ai_user_id))


async def handle_pipeline_failure(youtube_id: str, ai_user_id: str, error_message: str, ydx_server: str,
                                  ydx_app_host: str):
    logger.error(f"Pipeline failed for YouTube ID: {youtube_id}, AI User ID: {ai_user_id}")

    # Cleanup failed pipeline
    await cleanup_failed_pipeline(youtube_id, ai_user_id, error_message)

    # Remove SQLite entry
    await remove_sqlite_entry(youtube_id, ai_user_id)

    # Clean up the queue
    await clean_up_queue(youtube_id, ai_user_id)

    # Notify YouDescribe service about the failure
    await notify_youdescribe_service(youtube_id, ai_user_id, error_message, ydx_server, ydx_app_host)

    # Notify admin (implement this based on your needs)
    await notify_admin(youtube_id, ai_user_id, error_message)


async def notify_youdescribe_service(youtube_id: str, ai_user_id: str, error_message: str, ydx_server: str,
                                     ydx_app_host: str):
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
    # SMTP Configuration
    SMTP_HOST = os.getenv("SMTP_HOST")
    SMTP_PORT = int(os.getenv("SMTP_PORT"))
    SMTP_USERNAME = os.getenv("SMTP_USERNAME")
    SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
    SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "True") == "True"

    ADMIN_EMAIL = "smirani1@mail.sfsu.edu"
    SENDER_EMAIL = os.getenv("SENDER_EMAIL", SMTP_USERNAME)

    if not SMTP_PASSWORD:
        logger.error("SMTP password not set in environment variables.")
        return

    # Create the email message
    message = EmailMessage()
    message["From"] = SENDER_EMAIL
    message["To"] = ADMIN_EMAIL
    message["Subject"] = f"Video Pipeline Error: YouTube ID {youtube_id}"

    # Construct the email content
    email_content = f"""
    Dear Admin,

    An error occurred in the video pipeline.

    Details:
    - YouTube ID: {youtube_id}
    - AI User ID: {ai_user_id}
    - Error Message: {error_message}

    Please investigate the issue.

    Best regards,
    Video Pipeline System
    """
    message.set_content(email_content)

    try:
        # Send the email asynchronously
        await aiosmtplib.send(
            message,
            hostname=SMTP_HOST,
            port=SMTP_PORT,
            username=SMTP_USERNAME,
            password=SMTP_PASSWORD,
            start_tls=SMTP_USE_TLS,
        )
        logger.info(f"Admin notified via email about failure for YouTube ID: {youtube_id}, AI User ID: {ai_user_id}")
    except Exception as e:
        logger.error(f"Failed to send email notification to admin: {str(e)}")
        logger.error(traceback.format_exc())


async def run_pipeline_task(youtube_id: str, ai_user_id: str, ydx_server: str, ydx_app_host: str):
    print("INFO ", youtube_id, ai_user_id, ydx_server, ydx_app_host)
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
        print(f"Pipeline completed for YouTube ID: {youtube_id}")
    except Exception as e:
        logger.error(f"Pipeline failed for YouTube ID {youtube_id}: {str(e)}")
        logger.error(traceback.format_exc())
        update_status(youtube_id, ai_user_id, StatusEnum.failed.value)
        await handle_pipeline_failure(youtube_id, ai_user_id, str(e), ydx_server, ydx_app_host)
    finally:
        # Remove task from enqueued set
        enqueued_tasks.discard((youtube_id, ai_user_id))

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