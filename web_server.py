from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
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
from typing import Dict, Any, Optional

# Import custom modules
from web_server_module.web_server_types import WebServerRequest
from web_server_module.custom_logger import setup_logger
from pipeline_module.pipeline_runner import run_pipeline
from web_server_module.web_server_database import (
    create_database, process_incoming_data, update_status, update_ai_user_data,
    get_data_for_youtube_id_ai_user_id, get_data_for_youtube_id_and_user_id,
    StatusEnum, remove_sqlite_entry, DatabaseManager
)

# Load environment variables
load_dotenv()

# Setup logger
logger = setup_logger()

# Global variables with type hints
GPU_URL: str = os.getenv("GPU_URL", "")
AI_USER_ID: str = os.getenv("AI_USER_ID", "")
YDX_SERVER: str = os.getenv("YDX_SERVER", "")


class PipelineTask:
    def __init__(
            self,
            youtube_id: str,
            ai_user_id: str,
            ydx_server: str,
            ydx_app_host: str,
            user_id: Optional[str] = None
    ):
        self.youtube_id = youtube_id
        self.ai_user_id = ai_user_id
        self.ydx_server = ydx_server
        self.ydx_app_host = ydx_app_host
        self.user_id = user_id
        self.created_at = datetime.utcnow()


class RequestManager:
    def __init__(self, max_parallel_tasks: int = 3, max_queue_size: int = 1000):
        self.pipeline_queue = asyncio.Queue(maxsize=max_queue_size)
        self.active_tasks: Dict[str, Dict[str, Any]] = {}
        self.processing = False
        self.max_parallel_tasks = max_parallel_tasks
        self.worker_tasks = []
        self.logger = setup_logger()
        self.task_locks: Dict[str, asyncio.Lock] = {}
        self.db_manager = DatabaseManager(os.getenv("DB_PATH", "pipeline.db"))

    async def process_queue(self):
        self.processing = True
        self.worker_tasks = [
            asyncio.create_task(self._worker(f"worker-{i}"))
            for i in range(self.max_parallel_tasks)
        ]
        await asyncio.gather(*self.worker_tasks, return_exceptions=True)

    async def _worker(self, worker_id: str):
        while self.processing:
            try:
                task = await asyncio.wait_for(self.pipeline_queue.get(), timeout=5.0)
                task_key = f"{task.youtube_id}:{task.ai_user_id}"

                if task_key not in self.task_locks:
                    self.task_locks[task_key] = asyncio.Lock()

                async with self.task_locks[task_key]:
                    if task_key in self.active_tasks:
                        self.logger.info(f"Task {task_key} already processing, skipping")
                        self.pipeline_queue.task_done()
                        continue

                    try:
                        await self._process_task(task, worker_id, task_key)
                    except Exception as e:
                        await self._handle_task_error(task, e, task_key)
                    finally:
                        self.pipeline_queue.task_done()

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self.logger.error(f"Worker {worker_id} error: {str(e)}")
                await asyncio.sleep(1)

    async def _process_task(self, task: PipelineTask, worker_id: str, task_key: str):
        self.active_tasks[task_key] = {
            'status': 'processing',
            'worker': worker_id,
            'start_time': datetime.utcnow(),
            'retries': 0
        }

        try:
            await run_pipeline_task(
                youtube_id=task.youtube_id,
                ai_user_id=task.ai_user_id,
                ydx_server=task.ydx_server,
                ydx_app_host=task.ydx_app_host
            )
            self.active_tasks[task_key]['status'] = 'completed'
            await self.db_manager.update_status(
                task.youtube_id,
                task.ai_user_id,
                StatusEnum.done.value,
                {'completion_time': datetime.utcnow().isoformat()}
            )
        except Exception as e:
            raise e

    async def _handle_task_error(self, task: PipelineTask, error: Exception, task_key: str):
        self.logger.error(f"Error processing task {task_key}: {str(error)}")
        self.active_tasks[task_key]['status'] = 'failed'
        self.active_tasks[task_key]['error'] = str(error)

        if self.active_tasks[task_key].get('retries', 0) < 2:
            self.active_tasks[task_key]['retries'] = self.active_tasks[task_key].get('retries', 0) + 1
            await self.pipeline_queue.put(task)
            self.logger.info(f"Requeuing task {task_key} for retry #{self.active_tasks[task_key]['retries']}")
        else:
            await handle_pipeline_failure(
                task.youtube_id,
                task.ai_user_id,
                str(error),
                task.ydx_server,
                task.ydx_app_host
            )
            await self.db_manager.update_status(
                task.youtube_id,
                task.ai_user_id,
                StatusEnum.failed.value,
                {'error': str(error), 'error_time': datetime.utcnow().isoformat()}
            )

    async def stop(self):
        """Graceful shutdown with task completion tracking"""
        self.processing = False
        self.logger.info("Initiating graceful shutdown...")

        try:
            await asyncio.wait_for(self.pipeline_queue.join(), timeout=300)
        except asyncio.TimeoutError:
            self.logger.warning("Shutdown timeout reached, some tasks may not have completed")

        for task in self.worker_tasks:
            task.cancel()

        await asyncio.gather(*self.worker_tasks, return_exceptions=True)
        self.logger.info("All workers shut down successfully")

    def get_queue_stats(self) -> Dict[str, Any]:
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
                    'duration': str(datetime.utcnow() - v['start_time']),
                    'retries': v['retries']
                } for k, v in self.active_tasks.items()
            }
        }


# Initialize request manager
request_manager = RequestManager(max_parallel_tasks=3)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting application...")
    create_database()

    # Start queue processing
    queue_task = asyncio.create_task(request_manager.process_queue())
    logger.info("Queue processing started")

    # Start cleanup task
    cleanup_task = asyncio.create_task(request_manager.db_manager.cleanup_stale_entries())

    yield

    logger.info("Application shutting down...")
    await request_manager.stop()
    queue_task.cancel()
    cleanup_task.cancel()
    try:
        await asyncio.gather(queue_task, cleanup_task)
    except asyncio.CancelledError:
        pass
    logger.info("Queue processing stopped")


# Setup FastAPI app
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
    try:
        task = PipelineTask(
            youtube_id=post_data.youtube_id,
            ai_user_id=post_data.AI_USER_ID,
            ydx_server=post_data.ydx_server,
            ydx_app_host=post_data.ydx_app_host,
            user_id=post_data.user_id
        )

        # Process incoming data
        process_incoming_data(
            post_data.user_id,
            post_data.ydx_server,
            post_data.ydx_app_host,
            post_data.AI_USER_ID,
            post_data.youtube_id
        )

        await request_manager.pipeline_queue.put(task)
        queue_position = request_manager.pipeline_queue.qsize()

        logger.info(f"Added video {post_data.youtube_id} to processing queue at position {queue_position}")

        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={
                "status": "success",
                "message": "AI caption generation request queued",
                "queue_position": queue_position
            }
        )

    except asyncio.QueueFull:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service is currently at capacity. Please try again later."
        )
    except Exception as e:
        logger.error(f"Error in generate_ai_caption: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.get("/ai_description_status/{youtube_id}")
async def ai_description_status(youtube_id: str):
    try:
        status = await request_manager.db_manager.get_status(youtube_id, AI_USER_ID)
        if not status:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="AI description not found"
            )
        return JSONResponse(content={"status": status})
    except Exception as e:
        logger.error(f"Error in ai_description_status: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@app.get("/health_check")
async def health_check():
    try:
        stats = request_manager.get_queue_stats()
        return {
            "status": "OK",
            "timestamp": datetime.utcnow().isoformat(),
            **stats,
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