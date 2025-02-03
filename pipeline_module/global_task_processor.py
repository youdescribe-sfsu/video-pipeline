# global_task_processor.py
import asyncio
from dotenv import load_dotenv
from service_manager import ServiceManager
from pipeline_module.pipeline_runner import run_pipeline


def process_video_task(task_data):
    """
    This function is enqueued by the Redis-backed queue and executed by an RQ worker.
    It wraps the asynchronous pipeline execution in asyncio.run so that the entire video processing
    runs to completion for a given task.

    Parameters:
        task_data (dict): Contains 'youtube_id', 'ai_user_id', 'ydx_server', and 'ydx_app_host'.
    """
    # Load environment variables (if needed)
    load_dotenv()

    # Extract task details
    youtube_id = task_data.get("youtube_id")
    ai_user_id = task_data.get("ai_user_id")
    ydx_server = task_data.get("ydx_server")
    ydx_app_host = task_data.get("ydx_app_host")

    # Initialize the Service Manager with your service configurations.
    # Adjust these settings to match your production configurations.
    yolo_services = [{"port": "8087", "gpu": "2"}]
    # For captioning and rating, we enforce single-worker mode by using max_connections=1 (configured in ServiceManager)
    caption_services = [{"port": "8085", "gpu": "4"}]
    rating_services = [{"port": "8082", "gpu": "4"}]
    max_workers = 4  # This is for modules that can run in parallel; captioning/rating remain single-threaded.

    service_manager = ServiceManager(
        yolo_services=yolo_services,
        caption_services=caption_services,
        rating_services=rating_services,
        max_workers=max_workers
    )

    # Initialize the service manager
    asyncio.run(service_manager.initialize())

    # Execute the full pipeline for the video using the existing run_pipeline function.
    # We assume start_time/end_time are not provided (adjust if needed).
    asyncio.run(run_pipeline(
        video_id=youtube_id,
        video_start_time=None,
        video_end_time=None,
        upload_to_server=True,
        service_manager=service_manager,
        ydx_server=ydx_server,
        ydx_app_host=ydx_app_host,
        userId=None,
        AI_USER_ID=ai_user_id
    ))
