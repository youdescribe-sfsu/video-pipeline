from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from typing import Optional, Dict, Any
import asyncio
import traceback
from datetime import datetime
import uvicorn
import aiohttp
import os
from contextlib import asynccontextmanager
import aiosmtplib
from email.message import EmailMessage
from dotenv import load_dotenv

# Import custom modules
from web_server_module.web_server_types import WebServerRequest
from web_server_module.custom_logger import setup_logger
from pipeline_module.pipeline_runner import run_pipeline, cleanup_failed_pipeline
from web_server_module.web_server_database import (
    create_database, process_incoming_data, update_status,
    update_ai_user_data, get_data_for_youtube_id_ai_user_id,
    get_data_for_youtube_id_and_user_id, StatusEnum, remove_sqlite_entry
)

# Load environment variables
load_dotenv()

# Setup logger
logger = setup_logger()

# Global variables
GPU_URL = os.getenv("GPU_URL")
AI_USER_ID = os.getenv("AI_USER_ID")
YDX_SERVER = os.getenv("YDX_SERVER")


class EnhancedRequestManager:
    def __init__(self):
        self.pipeline_queue = asyncio.Queue()
        self.active_tasks = set()
        self.processing_history = {}
        self.logger = setup_logger()
        self.max_retries = 3
        self.retry_delay = 5  # seconds

    async def add_task(self, task: WebServerRequest) -> bool:
        """Add task to queue with duplicate checking"""
        task_key = (task.youtube_id, task.AI_USER_ID)

        if task_key in self.active_tasks:
            self.logger.info(f"Task {task.youtube_id} already in processing")
            return False

        if task_key in self.processing_history:
            last_processed = self.processing_history[task_key]
            if (datetime.now() - last_processed).seconds < 3600:
                self.logger.info(f"Task {task.youtube_id} processed too recently")
                return False

        await self.pipeline_queue.put(task)
        self.active_tasks.add(task_key)
        return True

    async def process_queue(self):
        """Main queue processing loop with enhanced error handling"""
        while True:
            try:
                task = await self.pipeline_queue.get()
                task_key = (task.youtube_id, task.AI_USER_ID)

                for attempt in range(self.max_retries):
                    try:
                        self.logger.info(f"Processing video {task.youtube_id} (attempt {attempt + 1})")

                        await run_pipeline(
                            youtube_id=task.youtube_id,
                            video_end_time=None,
                            video_start_time=None,
                            upload_to_server=True,
                            tasks=None,
                            ydx_server=task.ydx_server,
                            ydx_app_host=task.ydx_app_host,
                            userId=task.user_id,
                            AI_USER_ID=task.AI_USER_ID,
                        )

                        self.processing_history[task_key] = datetime.now()
                        update_status(task.youtube_id, task.AI_USER_ID, StatusEnum.done.value)

                        # Notify users about completion
                        await self.notify_completion(task)
                        break  # Success, exit retry loop

                    except Exception as e:
                        self.logger.error(f"Attempt {attempt + 1} failed: {str(e)}")
                        if attempt == self.max_retries - 1:  # Last attempt
                            await self.handle_pipeline_failure(task, str(e))
                        else:
                            await asyncio.sleep(self.retry_delay * (attempt + 1))

            except Exception as e:
                self.logger.error(f"Queue processing error: {str(e)}")
                await asyncio.sleep(5)

            finally:
                if task_key in self.active_tasks:
                    self.active_tasks.discard(task_key)
                self.pipeline_queue.task_done()

    async def handle_pipeline_failure(self, task: WebServerRequest, error_message: str):
        """Handle pipeline failure with cleanup and notifications"""
        self.logger.error(f"Pipeline failed for video {task.youtube_id}: {error_message}")

        try:
            # Update status to failed
            update_status(task.youtube_id, task.AI_USER_ID, StatusEnum.failed.value)

            # Cleanup database entries
            await remove_sqlite_entry(task.youtube_id, task.AI_USER_ID)

            # Notify service about failure
            await self.notify_youdescribe_service(
                task.youtube_id,
                task.AI_USER_ID,
                error_message,
                task.ydx_server,
                task.ydx_app_host
            )

            # Cleanup pipeline artifacts
            await cleanup_failed_pipeline(task.youtube_id, task.AI_USER_ID)

        except Exception as e:
            self.logger.error(f"Error in failure handling: {str(e)}")
            self.logger.error(traceback.format_exc())

    async def notify_youdescribe_service(self, youtube_id: str, ai_user_id: str,
                                         error_message: str, ydx_server: str,
                                         ydx_app_host: str):
        """Notify YouDescribe service about pipeline failure"""
        url = f"{ydx_server}/api/users/pipeline-failure"
        data = {
            "youtube_id": youtube_id,
            "ai_user_id": ai_user_id,
            "error_message": error_message,
            "ydx_app_host": ydx_app_host
        }

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(url, json=data) as response:
                    if response.status != 200:
                        self.logger.error(f"Failed to notify YouDescribe service: {await response.text()}")
            except Exception as e:
                self.logger.error(f"Error notifying YouDescribe service: {str(e)}")

    async def notify_completion(self, task: WebServerRequest):
        """Notify users about successful pipeline completion"""
        try:
            user_data = get_data_for_youtube_id_and_user_id(task.youtube_id, task.AI_USER_ID)
            for data in user_data:
                update_ai_user_data(
                    youtube_id=task.youtube_id,
                    ai_user_id=task.AI_USER_ID,
                    user_id=data.get("user_id"),
                    status=StatusEnum.done.value
                )

            await self.send_completion_email(task)

        except Exception as e:
            self.logger.error(f"Error in completion notification: {str(e)}")

    async def send_completion_email(self, task: WebServerRequest):
        """Send completion email to user"""
        try:
            SMTP_HOST = os.getenv("SMTP_HOST")
            SMTP_PORT = int(os.getenv("SMTP_PORT"))
            SMTP_USERNAME = os.getenv("SMTP_USERNAME")
            SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
            SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "True") == "True"

            if not all([SMTP_HOST, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD]):
                self.logger.error("Missing SMTP configuration")
                return

            message = EmailMessage()
            message["From"] = SMTP_USERNAME
            message["To"] = task.user_id  # Assuming user_id is email
            message["Subject"] = "AI Description Generation Complete"

            message.set_content(
                f"Your AI description for video {task.youtube_id} is now ready.\n"
                f"You can view it at: {task.ydx_app_host}/video/{task.youtube_id}"
            )

            await aiosmtplib.send(
                message,
                hostname=SMTP_HOST,
                port=SMTP_PORT,
                username=SMTP_USERNAME,
                password=SMTP_PASSWORD,
                start_tls=SMTP_USE_TLS
            )

        except Exception as e:
            self.logger.error(f"Error sending completion email: {str(e)}")

    async def get_queue_status(self) -> Dict[str, Any]:
        """Get current queue status"""
        return {
            'queue_size': self.pipeline_queue.qsize(),
            'active_tasks': len(self.active_tasks),
            'total_processed': len(self.processing_history)
        }


# Initialize request manager
request_manager = EnhancedRequestManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events"""
    logger.info("Starting application...")
    create_database()

    # Start queue processor
    asyncio.create_task(request_manager.process_queue())
    logger.info("Queue processor started")

    yield

    logger.info("Shutting down...")


app = FastAPI(lifespan=lifespan)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/generate_ai_caption")
async def generate_ai_caption(post_data: WebServerRequest):
    """Handle AI caption generation requests"""
    try:
        # Try to add task to queue
        was_queued = await request_manager.add_task(post_data)

        if not was_queued:
            # Task already being processed
            return JSONResponse(
                status_code=200,
                content={
                    "status": "in_progress",
                    "message": "Video is already being processed"
                }
            )

        # Process incoming data
        process_incoming_data(
            post_data.user_id,
            post_data.ydx_server,
            post_data.ydx_app_host,
            post_data.AI_USER_ID,
            post_data.youtube_id
        )

        return JSONResponse(
            status_code=202,
            content={
                "status": "queued",
                "message": "AI caption generation request queued",
                "youtube_id": post_data.youtube_id
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
    """Get AI description status"""
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
    """Health check endpoint"""
    try:
        queue_status = await request_manager.get_queue_status()
        return {
            "status": "OK",
            "timestamp": datetime.now().isoformat(),
            **queue_status
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return {"status": "Error", "message": str(e)}, status.HTTP_500_INTERNAL_SERVER_ERROR


if __name__ == "__main__":
    uvicorn.run("web_server:app", host="0.0.0.0", port=8086, reload=True)