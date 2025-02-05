"""
global_task_processor.py

This module handles the execution of video processing tasks by RQ workers.
It includes comprehensive error handling, cleanup procedures, and service management.
The processor ensures proper resource cleanup on both success and failure.
"""

import asyncio
import os
import logging
import traceback
import aiohttp
from typing import Dict, Any, Optional
from shutil import rmtree
from dotenv import load_dotenv

from service_manager import ServiceManager
from pipeline_module.pipeline_runner import run_pipeline
from pipeline_module.utils_module.utils import return_video_folder_name
from web_server_module.web_server_database import remove_sqlite_entry, update_status
from web_server_module.custom_logger import setup_logger

# Initialize logger for task processing
logger = setup_logger()


async def cleanup_failed_pipeline(youtube_id: str, error_message: str,
                                  ai_user_id: str = None) -> None:
    """
    Clean up all resources associated with a failed pipeline execution.
    This includes removing video files, database entries, and releasing services.

    Args:
        youtube_id: The ID of the video being processed
        error_message: Description of what went wrong
        ai_user_id: The AI user ID associated with the processing
    """
    try:
        logger.info(f"Starting cleanup for failed pipeline {youtube_id}")

        # Clean up video directory if it exists
        video_folder = return_video_folder_name({"video_id": youtube_id})
        if os.path.exists(video_folder):
            logger.info(f"Removing video folder: {video_folder}")
            rmtree(video_folder)
        else:
            logger.warning(f"Video folder {video_folder} not found, skipping removal")

        # Remove database entries
        if ai_user_id:
            await remove_sqlite_entry(youtube_id, ai_user_id)
            logger.info(f"Removed database entries for video {youtube_id}")

        # Update status to failed
        update_status(youtube_id, ai_user_id, "failed")
        logger.info(f"Updated pipeline status to 'failed' for {youtube_id}")

    except Exception as e:
        logger.error(f"Error during cleanup for {youtube_id}: {str(e)}")
        logger.error(traceback.format_exc())


async def handle_pipeline_failure(youtube_id: str, ai_user_id: str,
                                  ydx_server: str, ydx_app_host: str) -> None:
    """
    Handle a pipeline failure by cleaning up resources and notifying relevant parties.
    This function orchestrates the complete failure handling process.

    Args:
        youtube_id: The ID of the failed video
        ai_user_id: The AI user ID for the process
        ydx_server: YDX server URL for notifications
        ydx_app_host: YDX app host for notifications
    """
    try:
        # First clean up resources
        await cleanup_failed_pipeline(youtube_id, "Pipeline processing failed", ai_user_id)

        # Notify YDX server about the failure
        error_notification_url = f"{ydx_server}/api/users/pipeline-failure"
        async with aiohttp.ClientSession() as session:
            await session.post(
                error_notification_url,
                json={
                    "youtube_id": youtube_id,
                    "ai_user_id": ai_user_id,
                    "error_message": "Pipeline processing failed",
                    "ydx_app_host": ydx_app_host
                }
            )
        logger.info(f"Sent failure notification for video {youtube_id}")

    except Exception as e:
        logger.error(f"Error handling pipeline failure: {str(e)}")
        logger.error(traceback.format_exc())


def process_video_task(task_data: Dict[str, Any]) -> None:
    """
    Main entry point for processing video tasks through RQ workers.
    Handles the complete lifecycle of a video processing task including setup,
    execution, and cleanup on both success and failure.

    Args:
        task_data: Dictionary containing task details including:
            - youtube_id: Video identifier
            - ai_user_id: AI user identifier
            - ydx_server: YDX server URL
            - ydx_app_host: YDX app host URL
    """
    # Load environment variables
    load_dotenv()

    # Extract task details
    youtube_id = task_data.get("youtube_id")
    ai_user_id = task_data.get("ai_user_id")
    ydx_server = task_data.get("ydx_server")
    ydx_app_host = task_data.get("ydx_app_host")

    # Initialize service manager for single-instance services
    service_manager = ServiceManager()

    try:
        # Initialize services
        asyncio.run(service_manager.initialize())
        logger.info(f"Starting pipeline for video {youtube_id}")

        # Execute the pipeline
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

        logger.info(f"Successfully completed pipeline for video {youtube_id}")

    except Exception as e:
        logger.error(f"Pipeline failed for video {youtube_id}: {str(e)}")
        logger.error(traceback.format_exc())

        # Handle the failure
        asyncio.run(handle_pipeline_failure(
            youtube_id=youtube_id,
            ai_user_id=ai_user_id,
            ydx_server=ydx_server,
            ydx_app_host=ydx_app_host
        ))

        # Re-raise to mark the task as failed in RQ
        raise

    finally:
        # Ensure services are cleaned up regardless of success/failure
        try:
            asyncio.run(service_manager.cleanup())
        except Exception as cleanup_error:
            logger.error(f"Error during service cleanup: {str(cleanup_error)}")