import logging
import os
import asyncio
import json
from enum import Enum
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from yt_dlp.compat import shutil
import traceback
from datetime import datetime

from pipeline_module.utils_module.utils import PipelineTask, return_video_folder_name
from web_server_module.web_server_database import (
    update_status, get_status_for_youtube_id, DatabaseManager,
    StatusEnum
)
from pipeline_module.utils_module.google_services import service_manager, GoogleServiceError

# Import all submodules
from .import_video_submodule.import_video import ImportVideo
from .extract_audio_submodule.extract_audio import ExtractAudio
from .speech_to_text_submodule.speech_to_text import SpeechToText
from .frame_extraction_submodule.frame_extraction import FrameExtraction
from .ocr_extraction_submodule.ocr_extraction import OcrExtraction
from .object_detection_submodule.object_detection import ObjectDetection
from .keyframe_selection_submodule.keyframe_selection import KeyframeSelection
from .image_captioning_submodule.image_captioning import ImageCaptioning
from .caption_rating_submodule.caption_rating import CaptionRating
from .scene_segmentation_submodule.scene_segmentation import SceneSegmentation
from .text_summarization_submodule.text_summary import TextSummaryCoordinator
from .upload_to_YDX_submodule.upload_to_YDX import UploadToYDX
from .generate_YDX_caption_submodule.generate_ydx_caption import GenerateYDXCaption


class PipelineError(Exception):
    """Custom exception for pipeline errors with detailed context"""

    def __init__(self, message: str, task: str, details: Dict[str, Any]):
        self.message = message
        self.task = task
        self.details = details
        super().__init__(self.message)


class PipelineTask(str, Enum):
    """Enumeration of pipeline tasks"""
    IMPORT_VIDEO = "import_video"
    EXTRACT_AUDIO = "extract_audio"
    SPEECH_TO_TEXT = "speech_to_text"
    FRAME_EXTRACTION = "frame_extraction"
    OCR_EXTRACTION = "ocr_extraction"
    OBJECT_DETECTION = "object_detection"
    KEYFRAME_SELECTION = "keyframe_selection"
    IMAGE_CAPTIONING = "image_captioning"
    CAPTION_RATING = "caption_rating"
    SCENE_SEGMENTATION = "scene_segmentation"
    TEXT_SUMMARIZATION = "text_summarization"
    UPLOAD_TO_YDX = "upload_to_ydx"
    GENERATE_YDX_CAPTION = "generate_ydx_caption"


class PipelineRunner:
    def __init__(
            self,
            video_id: str,
            video_start_time: Optional[str],
            video_end_time: Optional[str],
            upload_to_server: bool,
            tasks: Optional[List[str]] = None,
            ydx_server: Optional[str] = None,
            ydx_app_host: Optional[str] = None,
            userId: Optional[str] = None,
            AI_USER_ID: Optional[str] = None,
    ):
        self.video_id = video_id
        self.video_start_time = video_start_time
        self.video_end_time = video_end_time
        self.upload_to_server = upload_to_server
        self.tasks = tasks or [t.value for t in PipelineTask]
        self.ydx_server = ydx_server
        self.ydx_app_host = ydx_app_host
        self.userId = userId
        self.AI_USER_ID = AI_USER_ID
        self.logger = self.setup_logger()
        self.db_manager = DatabaseManager(os.getenv("DB_PATH", "pipeline.db"))
        self.task_metrics: Dict[str, Dict[str, Any]] = {}

        # Initialize Google Services
        try:
            service_manager.validate_credentials()
            self.logger.info("Google services initialized successfully")
        except GoogleServiceError as e:
            self.logger.error(f"Failed to initialize Google services: {str(e)}")
            raise

        self.progress = self.load_progress()
        self.video_runner_obj = {
            "video_id": video_id,
            "logger": self.logger,
            "AI_USER_ID": self.AI_USER_ID
        }

    def setup_logger(self) -> logging.Logger:
        """Set up a dedicated logger for this pipeline instance"""
        logger = logging.getLogger(f"PipelineLogger-{self.video_id}")
        logger.setLevel(logging.INFO)

        log_dir = "pipeline_logs"
        os.makedirs(log_dir, exist_ok=True)

        # Add file handler
        file_handler = logging.FileHandler(
            f"{log_dir}/{self.video_id}_{self.AI_USER_ID}_pipeline.log"
        )
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - [%(name)s] - %(message)s"
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Add stream handler for console output
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        return logger

    def load_progress(self) -> Dict[str, Any]:
        """Load current progress from database"""
        status = get_status_for_youtube_id(self.video_id, self.AI_USER_ID)
        return {"status": status} if isinstance(status, str) else status if isinstance(status, dict) else {}

    async def save_progress(self, task: str, status: str, metadata: Optional[Dict[str, Any]] = None):
        """Save progress to database with metadata"""
        try:
            await self.db_manager.update_status(
                self.video_id,
                self.AI_USER_ID,
                status,
                {
                    "task": task,
                    "metadata": metadata or {},
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
        except Exception as e:
            self.logger.error(f"Failed to save progress: {e}")
            raise

    async def run_task(self, task: str, *args, **kwargs) -> Any:
        """Execute a single task with proper error handling and progress tracking"""
        task_start_time = datetime.utcnow()
        self.task_metrics[task] = {"start_time": task_start_time}

        try:
            self.logger.info(f"Starting task: {task}")
            await self.save_progress(task, "running")

            result = await getattr(self, f"run_{task}")(*args, **kwargs)

            task_end_time = datetime.utcnow()
            duration = (task_end_time - task_start_time).total_seconds()
            self.task_metrics[task].update({
                "end_time": task_end_time,
                "duration": duration,
                "status": "completed"
            })

            await self.save_progress(
                task,
                "completed",
                {
                    "duration": duration,
                    "metrics": self.task_metrics[task]
                }
            )

            self.logger.info(f"Completed task: {task} in {duration:.2f} seconds")
            return result

        except Exception as e:
            task_end_time = datetime.utcnow()
            duration = (task_end_time - task_start_time).total_seconds()
            self.task_metrics[task].update({
                "end_time": task_end_time,
                "duration": duration,
                "status": "failed",
                "error": str(e)
            })

            self.logger.error(f"Error in task {task}: {str(e)}", exc_info=True)
            await self.save_progress(
                task,
                "failed",
                {
                    "error": str(e),
                    "traceback": traceback.format_exc(),
                    "metrics": self.task_metrics[task]
                }
            )

            raise PipelineError(str(e), task, self.task_metrics[task])

    async def run_import_video(self) -> None:
        """Step 1: Import video"""
        self.logger.info(f"Importing video: {self.video_id}")
        import_video = ImportVideo(self.video_runner_obj)

        try:
            success = import_video.download_video()
            if not success:
                raise PipelineError(
                    "Video import failed",
                    "import_video",
                    {"video_id": self.video_id}
                )

            self.logger.info(f"Successfully imported video: {self.video_id}")
            return success

        except Exception as e:
            self.logger.error(f"Error importing video: {str(e)}")
            raise PipelineError(
                f"Video import failed: {str(e)}",
                "import_video",
                {"video_id": self.video_id, "error": str(e)}
            )

    async def run_extract_audio(self) -> None:
        """Step 2: Extract audio"""
        self.logger.info(f"Extracting audio from video: {self.video_id}")
        extract_audio = ExtractAudio(self.video_runner_obj)

        try:
            success = extract_audio.extract_audio()
            if not success:
                raise PipelineError(
                    "Audio extraction failed",
                    "extract_audio",
                    {"video_id": self.video_id}
                )

            # Verify FLAC format for Speech-to-Text
            if not extract_audio.check_audio_format():
                raise PipelineError(
                    "Audio not in required FLAC format",
                    "extract_audio",
                    {"video_id": self.video_id}
                )

            self.logger.info(f"Successfully extracted audio from video: {self.video_id}")
            return success

        except Exception as e:
            self.logger.error(f"Error extracting audio: {str(e)}")
            raise PipelineError(
                f"Audio extraction failed: {str(e)}",
                "extract_audio",
                {"video_id": self.video_id, "error": str(e)}
            )

    async def run_speech_to_text(self) -> None:
        """Step 3: Speech to text"""
        self.logger.info(f"Converting speech to text for video: {self.video_id}")
        speech_to_text = SpeechToText(self.video_runner_obj)

        try:
            success = speech_to_text.get_speech_from_audio()
            if not success:
                raise PipelineError(
                    "Speech to text conversion failed",
                    "speech_to_text",
                    {"video_id": self.video_id}
                )

            self.logger.info(f"Successfully converted speech to text for video: {self.video_id}")
            return success

        except Exception as e:
            self.logger.error(f"Error in speech to text conversion: {str(e)}")
            raise PipelineError(
                f"Speech to text conversion failed: {str(e)}",
                "speech_to_text",
                {"video_id": self.video_id, "error": str(e)}
            )

    async def run_frame_extraction(self) -> None:
        """Step 4: Frame extraction"""
        self.logger.info(f"Extracting frames from video: {self.video_id}")
        frame_extraction = FrameExtraction(
            self.video_runner_obj,
            int(os.environ.get("FRAME_EXTRACTION_RATE", 3))
        )

        try:
            success = frame_extraction.extract_frames()
            if not success:
                raise PipelineError(
                    "Frame extraction failed",
                    "frame_extraction",
                    {"video_id": self.video_id}
                )

            self.logger.info(f"Successfully extracted frames from video: {self.video_id}")
            return success

        except Exception as e:
            self.logger.error(f"Error extracting frames: {str(e)}")
            raise PipelineError(
                f"Frame extraction failed: {str(e)}",
                "frame_extraction",
                {"video_id": self.video_id, "error": str(e)}
            )

    async def run_ocr_extraction(self) -> None:
        """Step 5: OCR extraction"""
        self.logger.info(f"Running OCR extraction for video: {self.video_id}")
        ocr_extraction = OcrExtraction(self.video_runner_obj)

        try:
            success = ocr_extraction.run_ocr_detection()
            if not success:
                raise PipelineError(
                    "OCR extraction failed",
                    "ocr_extraction",
                    {"video_id": self.video_id}
                )

            self.logger.info(f"Successfully completed OCR extraction for video: {self.video_id}")
            return success

        except Exception as e:
            self.logger.error(f"Error in OCR extraction: {str(e)}")
            raise PipelineError(
                f"OCR extraction failed: {str(e)}",
                "ocr_extraction",
                {"video_id": self.video_id, "error": str(e)}
            )

    async def run_object_detection(self) -> None:
        """Step 6: Object detection"""
        self.logger.info(f"Running object detection for video: {self.video_id}")
        object_detection = ObjectDetection(self.video_runner_obj)

        try:
            success = object_detection.run_object_detection()
            if not success:
                raise PipelineError(
                    "Object detection failed",
                    "object_detection",
                    {"video_id": self.video_id}
                )

            self.logger.info(f"Successfully completed object detection for video: {self.video_id}")
            return success

        except Exception as e:
            self.logger.error(f"Error in object detection: {str(e)}")
            raise PipelineError(
                f"Object detection failed: {str(e)}",
                "object_detection",
                {"video_id": self.video_id, "error": str(e)}
            )

    async def run_keyframe_selection(self) -> None:
        """Step 7: Keyframe selection"""
        self.logger.info(f"Running keyframe selection for video: {self.video_id}")
        keyframe_selection = KeyframeSelection(self.video_runner_obj)

        try:
            success = keyframe_selection.run_keyframe_selection()
            if not success:
                raise PipelineError(
                    "Keyframe selection failed",
                    "keyframe_selection",
                    {"video_id": self.video_id}
                )

            self.logger.info(f"Successfully completed keyframe selection for video: {self.video_id}")
            return success

        except Exception as e:
            self.logger.error(f"Error in keyframe selection: {str(e)}")
            raise PipelineError(
                f"Keyframe selection failed: {str(e)}",
                "keyframe_selection",
                {"video_id": self.video_id, "error": str(e)}
            )

    async def run_image_captioning(self) -> None:
        """Step 8: Image captioning"""
        self.logger.info(f"Running image captioning for video: {self.video_id}")
        image_captioning = ImageCaptioning(self.video_runner_obj)

        try:
            success = image_captioning.run_image_captioning()
            if not success:
                raise PipelineError(
                    "Image captioning failed",
                    "image_captioning",
                    {"video_id": self.video_id}
                )

            combined_success = image_captioning.combine_image_caption()
            if not combined_success:
                raise PipelineError(
                    "Combining image captions failed",
                    "image_captioning",
                    {"video_id": self.video_id, "stage": "combination"}
                )

            self.logger.info(f"Successfully completed image captioning for video: {self.video_id}")
            return success

        except Exception as e:
            self.logger.error(f"Error in image captioning: {str(e)}")
            raise PipelineError(
                f"Image captioning failed: {str(e)}",
                "image_captioning",
                {"video_id": self.video_id, "error": str(e)}
            )

    async def run_caption_rating(self) -> None:
        """Step 9: Caption rating"""
        self.logger.info(f"Running caption rating for video: {self.video_id}")
        caption_rating = CaptionRating(self.video_runner_obj)

        try:
            success = caption_rating.perform_caption_rating()
            if not success:
                raise PipelineError(
                    "Caption rating failed",
                    "caption_rating",
                    {"video_id": self.video_id}
                )

            self.logger.info(f"Successfully completed caption rating for video: {self.video_id}")
            return success

        except Exception as e:
            self.logger.error(f"Error in caption rating: {str(e)}")
            raise PipelineError(
                f"Caption rating failed: {str(e)}",
                "caption_rating",
                {"video_id": self.video_id, "error": str(e)}
            )

    async def run_scene_segmentation(self) -> None:
        """Step 10: Scene segmentation"""
        self.logger.info(f"Running scene segmentation for video: {self.video_id}")
        scene_segmentation = SceneSegmentation(self.video_runner_obj)

        try:
            success = scene_segmentation.run_scene_segmentation()
            if not success:
                raise PipelineError(
                    "Scene segmentation failed",
                    "scene_segmentation",
                    {"video_id": self.video_id}
                )

            self.logger.info(f"Successfully completed scene segmentation for video: {self.video_id}")
            return success

        except Exception as e:
            self.logger.error(f"Error in scene segmentation: {str(e)}")
            raise PipelineError(
                f"Scene segmentation failed: {str(e)}",
                "scene_segmentation",
                {"video_id": self.video_id, "error": str(e)}
            )

    async def run_text_summarization(self) -> None:
        """Step 11: Text summarization"""
        self.logger.info(f"Running text summarization for video: {self.video_id}")
        text_summarization = TextSummaryCoordinator(self.video_runner_obj)

        try:
            success = text_summarization.generate_text_summary()
            if not success:
                raise PipelineError(
                    "Text summarization failed",
                    "text_summarization",
                    {"video_id": self.video_id}
                )

            self.logger.info(f"Successfully completed text summarization for video: {self.video_id}")
            return success

        except Exception as e:
            self.logger.error(f"Error in text summarization: {str(e)}")
            raise PipelineError(
                f"Text summarization failed: {str(e)}",
                "text_summarization",
                {"video_id": self.video_id, "error": str(e)}
            )

    async def run_upload_to_ydx(self) -> None:
        """Step 12: Upload to YDX"""
        self.logger.info(f"Uploading to YDX for video: {self.video_id}")
        upload_to_ydx = UploadToYDX(
            self.video_runner_obj,
            upload_to_server=self.upload_to_server
        )

        try:
            success = upload_to_ydx.upload_to_ydx(
                ydx_server=self.ydx_server,
                AI_USER_ID=self.AI_USER_ID
            )
            if not success:
                raise PipelineError(
                    "Upload to YDX failed",
                    "upload_to_ydx",
                    {
                        "video_id": self.video_id,
                        "ydx_server": self.ydx_server
                    }
                )

            self.logger.info(f"Successfully uploaded to YDX for video: {self.video_id}")
            return success

        except Exception as e:
            self.logger.error(f"Error uploading to YDX: {str(e)}")
            raise PipelineError(
                f"Upload to YDX failed: {str(e)}",
                "upload_to_ydx",
                {
                    "video_id": self.video_id,
                    "ydx_server": self.ydx_server,
                    "error": str(e)
                }
            )

    async def run_generate_ydx_caption(self) -> None:
        """Step 13: Generate YDX caption"""
        self.logger.info(f"Generating YDX caption for video: {self.video_id}")
        generate_ydx_caption = GenerateYDXCaption(self.video_runner_obj)

        try:
            success = await generate_ydx_caption.generateYDXCaption(
                ydx_server=self.ydx_server,
                ydx_app_host=self.ydx_app_host,
                userId=self.userId,
                AI_USER_ID=self.AI_USER_ID,
            )
            if not success:
                raise PipelineError(
                    "Generate YDX caption failed",
                    "generate_ydx_caption",
                    {
                        "video_id": self.video_id,
                        "ydx_server": self.ydx_server,
                        "ydx_app_host": self.ydx_app_host
                    }
                )

            self.logger.info(f"Successfully generated YDX caption for video: {self.video_id}")
            return success

        except Exception as e:
            self.logger.error(f"Error generating YDX caption: {str(e)}")
            raise PipelineError(
                f"Generate YDX caption failed: {str(e)}",
                "generate_ydx_caption",
                {
                    "video_id": self.video_id,
                    "ydx_server": self.ydx_server,
                    "ydx_app_host": self.ydx_app_host,
                    "error": str(e)
                }
            )

    async def run_full_pipeline(self) -> None:
        """Execute the full pipeline with proper error handling and cleanup"""
        self.logger.info(f"Starting pipeline for video: {self.video_id}")
        start_time = datetime.utcnow()

        try:
            # Update initial status
            await self.save_progress("pipeline_start", "running", {
                "start_time": start_time.isoformat(),
                "tasks": self.tasks
            })

            # Execute each task in sequence
            for task in self.tasks:
                await self.run_task(task)

            # Update final status
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()

            await self.save_progress("pipeline_complete", "completed", {
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration": duration,
                "metrics": self.task_metrics
            })

            self.logger.info(f"Pipeline completed successfully for video: {self.video_id}")

        except Exception as e:
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()

            error_details = {
                "error": str(e),
                "traceback": traceback.format_exc(),
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration": duration,
                "metrics": self.task_metrics
            }

            self.logger.error(
                f"Pipeline failed for video {self.video_id}: {str(e)}",
                exc_info=True
            )

            await self.save_progress("pipeline_failed", "failed", error_details)
            await self.cleanup_failed_pipeline(str(e))

            raise

    async def cleanup_failed_pipeline(self, error_message: str):
        """Clean up resources on pipeline failure"""
        try:
            # Remove video folder if it exists
            video_folder = return_video_folder_name({"video_id": self.video_id})
            if os.path.exists(video_folder):
                shutil.rmtree(video_folder)
                self.logger.info(f"Cleaned up video folder: {video_folder}")

            # Update status to failed
            await self.save_progress("cleanup", "failed", {
                "error_message": error_message,
                "cleanup_time": datetime.utcnow().isoformat()
            })

            self.logger.info(f"Updated status to failed for video {self.video_id}")

        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")
            # Don't raise here as we're already handling an error

    def cleanup_resources(self):
        """Clean up temporary resources and connections"""
        try:
            # Clean up service manager resources
            service_manager.cleanup()
            self.logger.info("Cleaned up service manager resources")

            # Close database connections
            self.db_manager.close_all_connections()
            self.logger.info("Closed all database connections")

        except Exception as e:
            self.logger.error(f"Error during resource cleanup: {str(e)}")
            # Don't raise here as this is final cleanup


async def run_pipeline(
        video_id: str,
        video_start_time: Optional[str] = None,
        video_end_time: Optional[str] = None,
        upload_to_server: bool = False,
        tasks: Optional[List[str]] = None,
        ydx_server: Optional[str] = None,
        ydx_app_host: Optional[str] = None,
        userId: Optional[str] = None,
        AI_USER_ID: Optional[str] = None,
) -> None:
    """Main pipeline execution function with comprehensive error handling"""
    pipeline_runner = None
    start_time = datetime.utcnow()

    try:
        # Initialize pipeline runner
        pipeline_runner = PipelineRunner(
            video_id=video_id,
            video_start_time=video_start_time,
            video_end_time=video_end_time,
            upload_to_server=upload_to_server,
            tasks=tasks,
            ydx_server=ydx_server,
            ydx_app_host=ydx_app_host,
            userId=userId,
            AI_USER_ID=AI_USER_ID,
        )

        # Check if pipeline is already completed
        if isinstance(pipeline_runner.progress, dict):
            if all(pipeline_runner.progress.get(task) == "completed" for task in pipeline_runner.tasks):
                pipeline_runner.logger.info(f"Pipeline already completed for video: {video_id}")
                return
        else:
            pipeline_runner.logger.error(f"Invalid progress data for video: {video_id}")
            raise ValueError("Progress data is not a valid dictionary")

        # Execute pipeline
        await pipeline_runner.run_full_pipeline()

    except Exception as e:
        error_time = datetime.utcnow()
        duration = (error_time - start_time).total_seconds()

        if pipeline_runner:
            pipeline_runner.logger.error(
                f"Pipeline failed after {duration:.2f} seconds: {str(e)}",
                exc_info=True
            )
            await pipeline_runner.cleanup_failed_pipeline(str(e))
        else:
            logger = setup_logger()
            logger.error(
                f"Pipeline initialization failed for video {video_id}: {str(e)}",
                exc_info=True
            )

        # Ensure the error is properly propagated
        raise

    finally:
        if pipeline_runner:
            pipeline_runner.cleanup_resources()


if __name__ == "__main__":
    # Load environment variables
    load_dotenv()

    # Set up argument parser
    import argparse

    parser = argparse.ArgumentParser(description="Video Pipeline Runner")
    parser.add_argument("--video_id", required=True, help="YouTube Video ID")
    parser.add_argument("--upload_to_server", action="store_true", help="Upload To YDX Server")
    parser.add_argument("--start_time", help="Video Start Time")
    parser.add_argument("--end_time", help="Video End Time")
    parser.add_argument("--tasks", nargs="*", help="Specific tasks to run")
    parser.add_argument("--ydx_server", help="YDX Server URL")
    parser.add_argument("--ydx_app_host", help="YDX App Host")
    parser.add_argument("--user_id", help="User ID")
    parser.add_argument("--ai_user_id", help="AI User ID")

    args = parser.parse_args()

    # Run the pipeline
    asyncio.run(run_pipeline(
        video_id=args.video_id,
        video_start_time=args.start_time,
        video_end_time=args.end_time,
        upload_to_server=args.upload_to_server,
        tasks=args.tasks,
        ydx_server=args.ydx_server,
        ydx_app_host=args.ydx_app_host,
        userId=args.user_id,
        AI_USER_ID=args.ai_user_id
    ))