import logging
import os
import asyncio
import traceback
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from yt_dlp.compat import shutil

# Import utility modules
from pipeline_module.utils_module.utils import PipelineTask, return_video_folder_name
from web_server_module.web_server_database import (
    update_status, get_status_for_youtube_id, update_module_output,
    get_module_output, StatusEnum
)
from pipeline_module.utils_module.google_services import service_manager, GoogleServiceError

# Import pipeline step modules
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
    """Custom exception for pipeline errors"""

    def __init__(self, message: str, step: str, recoverable: bool = True):
        super().__init__(message)
        self.step = step
        self.recoverable = recoverable


class PipelineRunner:
    """Manages the video processing pipeline"""

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
        """Initialize pipeline with configuration"""
        self.video_id = video_id
        self.video_start_time = video_start_time
        self.video_end_time = video_end_time
        self.upload_to_server = upload_to_server
        self.tasks = tasks or [t.value for t in PipelineTask]
        self.ydx_server = ydx_server
        self.ydx_app_host = ydx_app_host
        self.userId = userId
        self.AI_USER_ID = AI_USER_ID

        # Set up logging
        self.logger = self.setup_logger()

        # Initialize video runner object
        self.video_runner_obj = {
            "video_id": video_id,
            "logger": self.logger,
            "AI_USER_ID": self.AI_USER_ID
        }

        # Load configuration
        self.config = self.load_config()

        # Initialize progress tracking
        self.progress = self.load_progress()

        # Initialize Google services
        self.init_google_services()

    def setup_logger(self) -> logging.Logger:
        """Configure logging for this pipeline instance"""
        logger = logging.getLogger(f"PipelineLogger-{self.video_id}")
        logger.setLevel(logging.INFO)

        # Create logs directory
        log_dir = Path("pipeline_logs")
        log_dir.mkdir(exist_ok=True)

        # Set up file handler
        log_file = log_dir / f"{self.video_id}_{self.AI_USER_ID}_pipeline.log"
        handler = logging.FileHandler(log_file)
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        return logger

    def load_config(self) -> Dict[str, Any]:
        """Load pipeline configuration"""
        config = {
            "max_retries": int(os.getenv("PIPELINE_MAX_RETRIES", "3")),
            "retry_delay": int(os.getenv("PIPELINE_RETRY_DELAY", "5")),
            "cleanup_on_failure": os.getenv("CLEANUP_ON_FAILURE", "True").lower() == "true",
            "frame_extraction_rate": int(os.getenv("FRAME_EXTRACTION_RATE", "3"))
        }
        self.logger.info(f"Loaded configuration: {config}")
        return config

    def load_progress(self) -> Dict[str, Any]:
        """Load progress from database"""
        status = get_status_for_youtube_id(self.video_id, self.AI_USER_ID)
        progress = {"status": status} if isinstance(status, str) else (
            status if isinstance(status, dict) else {}
        )
        self.logger.info(f"Loaded progress: {progress}")
        return progress

    def init_google_services(self) -> None:
        """Initialize Google API services"""
        try:
            service_manager.validate_credentials()
            self.logger.info("Google services initialized successfully")
        except GoogleServiceError as e:
            self.logger.error(f"Failed to initialize Google services: {str(e)}")
            raise

    async def run_task(self, task: str, *args, **kwargs) -> Any:
        """Execute a single pipeline task with retries"""
        self.progress = self.load_progress()

        # Skip completed tasks
        if self.progress.get(task) == "completed":
            self.logger.info(f"Skipping completed task: {task}")
            return

        self.logger.info(f"Starting task: {task}")

        # Initialize retry counter
        retries = 0
        last_error = None

        while retries < self.config["max_retries"]:
            try:
                # Execute task
                result = await getattr(self, f"run_{task}")(*args, **kwargs)

                # Update progress
                self.progress[task] = "completed"
                self.save_progress()

                self.logger.info(f"Completed task: {task}")
                return result

            except Exception as e:
                retries += 1
                last_error = e
                self.logger.error(
                    f"Error in task {task} (attempt {retries}): {str(e)}",
                    exc_info=True
                )

                if retries < self.config["max_retries"]:
                    # Wait before retry
                    await asyncio.sleep(self.config["retry_delay"] * retries)
                    self.logger.info(f"Retrying task: {task}")
                else:
                    # Max retries reached
                    self.progress[task] = "failed"
                    self.save_progress()
                    raise PipelineError(
                        f"Task {task} failed after {retries} attempts: {str(e)}",
                        task
                    )

    def save_progress(self) -> None:
        """Save progress to database"""
        update_status(self.video_id, self.AI_USER_ID, self.progress)

    async def cleanup_resources(self) -> None:
        """Clean up temporary resources"""
        try:
            # Clean up service manager
            service_manager.cleanup()

            # Clean up video folder if configured
            if self.config["cleanup_on_failure"]:
                video_folder = return_video_folder_name(self.video_runner_obj)
                if os.path.exists(video_folder):
                    shutil.rmtree(video_folder)
                    self.logger.info(f"Cleaned up video folder: {video_folder}")

            self.logger.info("Resource cleanup completed")

        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")

    async def run_pipeline_step(self, step_name: str,
                                step_class: Any) -> bool:
        """Run a single pipeline step"""
        try:
            # Initialize step
            step = step_class(self.video_runner_obj)

            # Run main step method
            method_name = f"run_{step_name.lower()}"
            success = getattr(step, method_name)()

            if not success:
                raise PipelineError(
                    f"{step_name} failed",
                    step_name
                )

            return True

        except Exception as e:
            self.logger.error(
                f"Error in {step_name}: {str(e)}",
                exc_info=True
            )
            raise PipelineError(str(e), step_name)

    # Pipeline step implementations

    async def run_import_video(self) -> None:
        """Import video step"""
        await self.run_pipeline_step("import_video", ImportVideo)

    async def run_extract_audio(self) -> None:
        """Extract audio step"""
        success = await self.run_pipeline_step("extract_audio", ExtractAudio)

        # Verify audio format
        if success and not ExtractAudio(self.video_runner_obj).check_audio_format():
            raise PipelineError("Audio not in required format", "extract_audio")

    async def run_speech_to_text(self) -> None:
        """Speech to text step"""
        await self.run_pipeline_step("speech_to_text", SpeechToText)

    async def run_frame_extraction(self) -> None:
        """Frame extraction step"""
        extractor = FrameExtraction(
            self.video_runner_obj,
            self.config["frame_extraction_rate"]
        )
        if not extractor.extract_frames():
            raise PipelineError("Frame extraction failed", "frame_extraction")

    async def run_ocr_extraction(self) -> None:
        """OCR extraction step"""
        await self.run_pipeline_step("ocr_extraction", OcrExtraction)

    async def run_object_detection(self) -> None:
        """Object detection step"""
        await self.run_pipeline_step("object_detection", ObjectDetection)

    async def run_keyframe_selection(self) -> None:
        """Keyframe selection step"""
        await self.run_pipeline_step("keyframe_selection", KeyframeSelection)

    async def run_image_captioning(self) -> None:
        """Image captioning step"""
        captioner = ImageCaptioning(self.video_runner_obj)

        if not captioner.run_image_captioning():
            raise PipelineError("Image captioning failed", "image_captioning")

        if not captioner.combine_image_caption():
            raise PipelineError("Caption combination failed", "image_captioning")

    async def run_caption_rating(self) -> None:
        """Caption rating step"""
        await self.run_pipeline_step("caption_rating", CaptionRating)

    async def run_scene_segmentation(self) -> None:
        """Scene segmentation step"""
        await self.run_pipeline_step("scene_segmentation", SceneSegmentation)

    async def run_text_summarization(self) -> None:
        """Text summarization step"""
        await self.run_pipeline_step("text_summarization", TextSummaryCoordinator)

    async def run_upload_to_ydx(self) -> None:
        """Upload to YDX step"""
        uploader = UploadToYDX(
            self.video_runner_obj,
            upload_to_server=self.upload_to_server
        )
        if not uploader.upload_to_ydx(
                ydx_server=self.ydx_server,
                AI_USER_ID=self.AI_USER_ID
        ):
            raise PipelineError("Upload to YDX failed", "upload_to_ydx")

    async def run_generate_ydx_caption(self) -> None:
        """Generate YDX caption step"""
        generator = GenerateYDXCaption(self.video_runner_obj)
        if not await generator.generateYDXCaption(
                ydx_server=self.ydx_server,
                ydx_app_host=self.ydx_app_host,
                userId=self.userId,
                AI_USER_ID=self.AI_USER_ID,
        ):
            raise PipelineError("Caption generation failed", "generate_ydx_caption")

    async def run_full_pipeline(self) -> None:
        """Run complete pipeline"""
        self.logger.info(f"Starting pipeline for video: {self.video_id}")

        try:
            # Run each task in sequence
            for task in self.tasks:
                await self.run_task(task)

            self.logger.info(f"Pipeline completed for video: {self.video_id}")

        except Exception as e:
            self.logger.error(
                f"Pipeline failed: {str(e)}",
                exc_info=True
            )
            await self.cleanup_resources()
            raise


async def run_pipeline(
        video_id: str,
        video_start_time: Optional[str],
        video_end_time: Optional[str],
        upload_to_server: bool = False,
        tasks: Optional[List[str]] = None,
        ydx_server: Optional[str] = None,
        ydx_app_host: Optional[str] = None,
        userId: Optional[str] = None,
        AI_USER_ID: Optional[str] = None,
) -> None:
    """Main pipeline execution function"""
    try:
        # Initialize pipeline
        pipeline = PipelineRunner(
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

        # Check if pipeline already complete
        if isinstance(pipeline.progress, dict):
            if all(pipeline.progress.get(task) == "completed" for task in pipeline.tasks):
                pipeline.logger.info(f"Pipeline already completed for video: {video_id}")
                return
        else:
            pipeline.logger.error(f"Invalid progress data for video: {video_id}")
            raise ValueError("Progress data is not a valid dictionary")

        # Run the pipeline
        await pipeline.run_full_pipeline()

    except Exception as e:
        logger = logging.getLogger(f"PipelineLogger-{video_id}")
        logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
        raise


async def cleanup_failed_pipeline(video_id: str, ai_user_id: str) -> None:
    """Clean up resources after pipeline failure"""
    logger = logging.getLogger(f"PipelineLogger-{video_id}")

    try:
        # Clean up video folder
        video_folder = return_video_folder_name({
            "video_id": video_id,
            "AI_USER_ID": ai_user_id
        })
        if os.path.exists(video_folder):
            shutil.rmtree(video_folder)
            logger.info(f"Cleaned up video folder: {video_folder}")

        # Update status to failed
        update_status(video_id, ai_user_id, StatusEnum.FAILED.value)
        logger.info(f"Updated status to failed for video {video_id}")

    except Exception as e:
        logger.error(f"Cleanup error: {str(e)}")
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    # Load environment variables
    load_dotenv()

    import argparse

    parser = argparse.ArgumentParser(description="Run video processing pipeline")

    # Add command line arguments
    parser.add_argument("--video_id", help="Video ID to process", type=str, required=True)
    parser.add_argument("--upload_to_server", help="Upload to YDX Server", action="store_true")
    parser.add_argument("--start_time", help="Start Time", type=str, default=None)
    parser.add_argument("--end_time", help="End Time", type=str, default=None)

    # Parse arguments
    args = parser.parse_args()

    # Run pipeline
    asyncio.run(run_pipeline(
        video_id=args.video_id,
        video_start_time=args.start_time,
        video_end_time=args.end_time,
        upload_to_server=args.upload_to_server
    ))
