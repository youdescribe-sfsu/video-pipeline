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
import psutil
import threading

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
    def __init__(self, max_parallel_tasks=3):
        self.pipeline_queue = asyncio.Queue()
        self.active_tasks = {}  # Dict to track task status and metadata
        self.processing = False
        self.max_parallel_tasks = max_parallel_tasks
        self.worker_tasks = []  # Track worker coroutines
        self.logger = setup_logger()

    async def process_queue(self):
        self.processing = True
        # Launch multiple worker tasks
        self.worker_tasks = [
            asyncio.create_task(self._worker(f"worker-{i}"))
            for i in range(self.max_parallel_tasks)
        ]

        # Wait for all workers to complete
        await asyncio.gather(*self.worker_tasks, return_exceptions=True)

    async def _worker(self, worker_id: str):
        """Individual worker that processes queue items"""
        while self.processing:
            try:
                # Try to get a task with timeout
                task = await asyncio.wait_for(self.pipeline_queue.get(), timeout=5.0)
                task_key = (task.youtube_id, task.AI_USER_ID)

                if task_key in self.active_tasks:
                    self.logger.info(f"Task {task_key} already being processed, skipping")
                    self.pipeline_queue.task_done()
                    continue

                # Update task metadata
                self.active_tasks[task_key] = {
                    'status': 'processing',
                    'worker': worker_id,
                    'start_time': datetime.now(),
                    'retries': 0
                }

                try:
                    self.logger.info(f"Worker {worker_id} processing video {task.youtube_id}")
                    await run_pipeline_task(
                        youtube_id=task.youtube_id,
                        ai_user_id=task.AI_USER_ID,
                        ydx_server=task.ydx_server,
                        ydx_app_host=task.ydx_app_host
                    )
                    self.active_tasks[task_key]['status'] = 'completed'

                except Exception as e:
                    self.logger.error(f"Pipeline failed for video {task.youtube_id}: {str(e)}")
                    self.active_tasks[task_key]['status'] = 'failed'
                    self.active_tasks[task_key]['error'] = str(e)

                    # Implement retry logic for recoverable errors
                    if self.active_tasks[task_key]['retries'] < 2:  # Max 2 retries
                        self.active_tasks[task_key]['retries'] += 1
                        await self.pipeline_queue.put(task)  # Re-queue for retry
                        self.logger.info(
                            f"Requeuing task {task_key} for retry #{self.active_tasks[task_key]['retries']}")
                    else:
                        await handle_pipeline_failure(
                            task.youtube_id,
                            task.AI_USER_ID,
                            str(e),
                            task.ydx_server,
                            task.ydx_app_host
                        )
                finally:
                    if self.active_tasks[task_key]['retries'] == 0:  # Only remove if not being retried
                        del self.active_tasks[task_key]
                    self.pipeline_queue.task_done()

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self.logger.error(f"Worker {worker_id} error: {str(e)}")
                await asyncio.sleep(1)

    async def stop(self):
        """Graceful shutdown with task completion tracking"""
        self.processing = False
        self.logger.info("Initiating graceful shutdown...")

        # Wait for current tasks to complete with timeout
        try:
            await asyncio.wait_for(self.pipeline_queue.join(), timeout=300)  # 5 minutes timeout
        except asyncio.TimeoutError:
            self.logger.warning("Shutdown timeout reached, some tasks may not have completed")

        # Cancel all workers
        for task in self.worker_tasks:
            task.cancel()

        # Wait for workers to properly shut down
        await asyncio.gather(*self.worker_tasks, return_exceptions=True)
        self.logger.info("All workers shut down successfully")

    def get_queue_stats(self):
        """Get detailed queue and processing statistics"""
        return {
            'queue_size': self.pipeline_queue.qsize(),
            'active_tasks': len(self.active_tasks),
            'worker_count': len(self.worker_tasks),
            'processing_state': self.processing,
            'active_task_details': {
                str(k): {
                    'status': v['status'],
                    'worker': v['worker'],
                    'duration': str(datetime.now() - v['start_time']),
                    'retries': v['retries']
                } for k, v in self.active_tasks.items()
            }
        }


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting application...")
    create_database()

    queue_task = asyncio.create_task(request_manager.process_queue())
    logger.info("Queue processing started")

    yield

    logger.info("Application shutting down...")
    await request_manager.stop()
    queue_task.cancel()
    try:
        await queue_task
    except asyncio.CancelledError:
        pass
    logger.info("Queue processing stopped")


# Initialize request manager with 3 parallel workers
request_manager = RequestManager(max_parallel_tasks=3)

# Setup FastAPI app
app = FastAPI(lifespan=lifespan)


@app.post("/generate_ai_caption")
async def generate_ai_caption(post_data: WebServerRequest):
    try:
        task_key = (post_data.youtube_id, post_data.AI_USER_ID)

        if task_key in request_manager.active_tasks:
            return {
                "status": "success",
                "message": "Video is already being processed",
                "task_info": request_manager.active_tasks[task_key],
                "queue_position": None
            }

        queue_position = request_manager.pipeline_queue.qsize() + 1

        await request_manager.pipeline_queue.put(post_data)
        logger.info(f"Added video {post_data.youtube_id} to processing queue at position {queue_position}")

        return {
            "status": "success",
            "message": "AI caption generation request queued",
            "queue_position": queue_position
        }

    except Exception as e:
        logger.error(f"Error in generate_ai_caption: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


async def handle_pipeline_failure(youtube_id: str, ai_user_id: str, error_message: str, ydx_server: str,
                                  ydx_app_host: str):
    logger.error(f"Pipeline failed for YouTube ID: {youtube_id}, AI User ID: {ai_user_id}")

    await cleanup_failed_pipeline(youtube_id, ai_user_id, error_message)
    await remove_sqlite_entry(youtube_id, ai_user_id)
    await notify_youdescribe_service(youtube_id, ai_user_id, error_message, ydx_server, ydx_app_host)
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


async def notify_admin_status(youtube_id: str, ai_user_id: str, status: str):
    """Sends status update emails to admin about pipeline progress."""
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
        await notify_admin_status(youtube_id, ai_user_id, "started")

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
        stats = request_manager.get_queue_stats()
        return {
            "status": "OK",
            "timestamp": datetime.now().isoformat(),
            **stats,  # Include all detailed statistics
            "system_info": {
                "memory_usage": psutil.Process().memory_info().rss / 1024 / 1024,  # MB
                "cpu_percent": psutil.Process().cpu_percent(),
                "thread_count": threading.active_count()
            }
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


if __name__ == "__main__":
    uvicorn.run("web_server:app", host="0.0.0.0", port=8086, reload=True)