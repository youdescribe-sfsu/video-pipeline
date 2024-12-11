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


class RequestManager:
    def __init__(self):
        self.pipeline_queue = asyncio.Queue()
        self.active_tasks = set()
        self.logger = setup_logger()

    async def process_queue(self):
        while True:
            try:
                # Get task from queue
                task = await self.pipeline_queue.get()
                task_key = (task.youtube_id, task.AI_USER_ID)

                try:
                    self.logger.info(f"Processing video {task.youtube_id}")
                    await run_pipeline_task(
                        youtube_id=task.youtube_id,
                        ai_user_id=task.AI_USER_ID,
                        ydx_server=task.ydx_server,
                        ydx_app_host=task.ydx_app_host
                    )
                except Exception as e:
                    self.logger.error(f"Pipeline failed for video {task.youtube_id}: {str(e)}")
                    await handle_pipeline_failure(
                        task.youtube_id,
                        task.AI_USER_ID,
                        str(e),
                        task.ydx_server,
                        task.ydx_app_host
                    )
                finally:
                    # Always clean up
                    self.active_tasks.discard(task_key)
                    self.pipeline_queue.task_done()
                    self.logger.info(f"Task for video {task.youtube_id} completed and removed from active tasks")

            except Exception as e:
                self.logger.error(f"Queue processing error: {str(e)}")


# Initialize request manager
request_manager = RequestManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting application...")
    create_database()

    # Start the queue processor
    asyncio.create_task(request_manager.process_queue())
    logger.info("Queue processing started")

    yield

    logger.info("Application shutting down...")


app = FastAPI(lifespan=lifespan)


async def notify_admin_status(youtube_id: str, ai_user_id: str, status: str):
    """
    Sends status update emails to admin about pipeline progress.
    """
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

    message = EmailMessage()
    message["From"] = SENDER_EMAIL
    message["To"] = ADMIN_EMAIL

    # Set appropriate subject and content based on status
    if status == "started":
        message["Subject"] = f"Pipeline Started: YouTube ID {youtube_id}"
        email_content = f"""
        Dear Admin,
        Pipeline processing has started for:
        - YouTube ID: {youtube_id}
        - AI User ID: {ai_user_id}
        You will be notified when processing completes.
        Best regards,
        Video Pipeline System
        """
    elif status == "completed":
        message["Subject"] = f"Pipeline Completed: YouTube ID {youtube_id}"
        email_content = f"""
        Dear Admin,
        Pipeline processing has successfully completed for:
        - YouTube ID: {youtube_id}
        - AI User ID: {ai_user_id}
        The video is now ready for review.
        Best regards,
        Video Pipeline System
        """

    message.set_content(email_content)

    try:
        await aiosmtplib.send(
            message,
            hostname=SMTP_HOST,
            port=SMTP_PORT,
            username=SMTP_USERNAME,
            password=SMTP_PASSWORD,
            start_tls=SMTP_USE_TLS,
        )
        logger.info(f"Admin notified about {status} status for YouTube ID: {youtube_id}")
    except Exception as e:
        logger.error(f"Failed to send status email to admin: {str(e)}")
        logger.error(traceback.format_exc())


async def run_pipeline_task(youtube_id: str, ai_user_id: str, ydx_server: str, ydx_app_host: str):
    print("INFO ", youtube_id, ai_user_id, ydx_server, ydx_app_host)
    try:
        # Send starting notification
        await notify_admin_status(youtube_id, ai_user_id, "started")

        # Run the pipeline
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

        # Update status and send completion notification
        update_status(youtube_id, ai_user_id, StatusEnum.done.value)
        await notify_admin_status(youtube_id, ai_user_id, "completed")

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


async def handle_pipeline_failure(youtube_id: str, ai_user_id: str, error_message: str, ydx_server: str,
                                  ydx_app_host: str):
    logger.error(f"Pipeline failed for YouTube ID: {youtube_id}, AI User ID: {ai_user_id}")

    try:
        # Remove SQLite entry
        await remove_sqlite_entry(youtube_id, ai_user_id)

        # Notify YouDescribe service about the failure
        await notify_youdescribe_service(youtube_id, ai_user_id, error_message, ydx_server, ydx_app_host)

    except Exception as e:
        logger.error(f"Error in handle_pipeline_failure: {str(e)}")


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


@app.post("/generate_ai_caption")
async def generate_ai_caption(post_data: WebServerRequest):
    try:
        task_key = (post_data.youtube_id, post_data.AI_USER_ID)

        # First check if task is already being processed
        if task_key in request_manager.active_tasks:
            logger.info(f"Video {post_data.youtube_id} is already being processed")
            # Return proper response with status code
            return JSONResponse(
                status_code=200,
                content={
                    "status": "in_progress",
                    "message": "Video is already being processed"
                }
            )

        try:
            # Process incoming data first
            process_incoming_data(
                post_data.user_id,
                post_data.ydx_server,
                post_data.ydx_app_host,
                post_data.AI_USER_ID,
                post_data.youtube_id
            )

            # Add to queue and active tasks
            request_manager.active_tasks.add(task_key)
            await request_manager.pipeline_queue.put(post_data)
            logger.info(f"Added video {post_data.youtube_id} to processing queue")

            # Return immediate response
            return JSONResponse(
                status_code=202,  # Accepted
                content={
                    "status": "queued",
                    "message": "AI caption generation request queued",
                    "youtube_id": post_data.youtube_id
                }
            )

        except Exception as e:
            logger.error(f"Failed to queue task: {str(e)}")
            # Return error response
            return JSONResponse(
                status_code=500,
                content={
                    "status": "error",
                    "message": f"Failed to process request: {str(e)}"
                }
            )

    except Exception as e:
        logger.error(f"Error in generate_ai_caption: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": str(e)
            }
        )

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
        return {
            "status": "OK",
            "timestamp": datetime.now().isoformat(),
            "queue_size": request_manager.pipeline_queue.qsize(),
            "active_tasks": len(request_manager.active_tasks)
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return {"status": "Error", "message": str(e)}, status.HTTP_500_INTERNAL_SERVER_ERROR


if __name__ == "__main__":
    uvicorn.run("web_server:app", host="0.0.0.0", port=8086, reload=True)