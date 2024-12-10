import logging
import os
import asyncio
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from yt_dlp.compat import shutil

from pipeline_module.utils_module.utils import PipelineTask, return_video_folder_name
from web_server_module.web_server_database import update_status, get_status_for_youtube_id
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
        logger = logging.getLogger(f"PipelineLogger-{self.video_id}")
        logger.setLevel(logging.INFO)

        log_dir = "pipeline_logs"
        os.makedirs(log_dir, exist_ok=True)

        handler = logging.FileHandler(f"{log_dir}/{self.video_id}_{self.AI_USER_ID}_pipeline.log")
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def load_progress(self) -> Dict[str, Any]:
        status = get_status_for_youtube_id(self.video_id, self.AI_USER_ID)
        return {"status": status} if isinstance(status, str) else status if isinstance(status, dict) else {}

    def save_progress(self):
        update_status(self.video_id, self.AI_USER_ID, self.progress)

    async def run_task(self, task: str, *args, **kwargs) -> Any:
        """Execute a single task with proper error handling and progress tracking."""
        self.progress = self.load_progress()

        if self.progress.get(task) == "completed":
            self.logger.info(f"Skipping completed task: {task}")
            return

        self.logger.info(f"Starting task: {task}")
        try:
            result = await getattr(self, f"run_{task}")(*args, **kwargs)
            self.progress[task] = "completed"
            self.save_progress()
            self.logger.info(f"Completed task: {task}")
            return result
        except Exception as e:
            self.logger.error(f"Error in task {task}: {str(e)}", exc_info=True)
            self.progress[task] = "failed"
            self.save_progress()
            raise

    # Task implementations with dependencies ensured

    async def run_import_video(self) -> None:
        """Step 1: Import video"""
        import_video = ImportVideo(self.video_runner_obj)
        success = import_video.download_video()
        if not success:
            raise Exception("Video import failed")

    async def run_extract_audio(self) -> None:
        """Step 2: Extract audio (Depends on import_video)"""
        extract_audio = ExtractAudio(self.video_runner_obj)
        success = extract_audio.extract_audio()
        if not success:
            raise Exception("Audio extraction failed")
        # Verify FLAC format for Speech-to-Text
        if not extract_audio.check_audio_format():
            raise Exception("Audio not in required FLAC format")

    async def run_speech_to_text(self) -> None:
        """Step 3: Speech to text (Depends on extract_audio)"""
        try:
            speech_to_text = SpeechToText(self.video_runner_obj)
            success = speech_to_text.get_speech_from_audio()
            if not success:
                raise Exception("Speech to text conversion failed")
            self.logger.info("Speech to text completed successfully")
        except Exception as e:
            self.logger.error(f"Speech to text error: {str(e)}")
            raise

    async def run_frame_extraction(self) -> None:
        """Step 4: Frame extraction"""
        frame_extraction = FrameExtraction(
            self.video_runner_obj,
            int(os.environ.get("FRAME_EXTRACTION_RATE", 3))
        )
        success = frame_extraction.extract_frames()
        if not success:
            raise Exception("Frame extraction failed")

    async def run_ocr_extraction(self) -> None:
        """Step 5: OCR extraction (Depends on frame_extraction)"""
        self.logger.info("Starting OCR extraction process")
        try:
            ocr_extraction = OcrExtraction(self.video_runner_obj)
            success = ocr_extraction.run_ocr_detection()
            if not success:
                raise Exception("OCR extraction failed")
            self.logger.info("OCR extraction completed successfully")
        except Exception as e:
            self.logger.error(f"OCR extraction error: {str(e)}")
            raise

    async def run_object_detection(self) -> None:
        object_detection = ObjectDetection(self.video_runner_obj)
        success = object_detection.run_object_detection()
        if not success:
            raise Exception("Object detection failed")

    async def run_keyframe_selection(self) -> None:
        keyframe_selection = KeyframeSelection(self.video_runner_obj)
        success = keyframe_selection.run_keyframe_selection()
        if not success:
            raise Exception("Keyframe selection failed")

    async def run_image_captioning(self) -> None:
        image_captioning = ImageCaptioning(self.video_runner_obj)
        success = image_captioning.run_image_captioning()
        if not success:
            raise Exception("Image captioning failed")
        combined_success = image_captioning.combine_image_caption()
        if not combined_success:
            raise Exception("Combining image captions failed")

    async def run_caption_rating(self) -> None:
        caption_rating = CaptionRating(self.video_runner_obj)
        success = caption_rating.perform_caption_rating()
        if not success:
            raise Exception("Caption rating failed")

    async def run_scene_segmentation(self) -> None:
        scene_segmentation = SceneSegmentation(self.video_runner_obj)
        success = scene_segmentation.run_scene_segmentation()
        if not success:
            raise Exception("Scene segmentation failed")

    async def run_text_summarization(self) -> None:
        text_summarization = TextSummaryCoordinator(self.video_runner_obj)
        success = text_summarization.generate_text_summary()
        if not success:
            raise Exception("Text summarization failed")

    async def run_upload_to_ydx(self) -> None:
        upload_to_ydx = UploadToYDX(
            self.video_runner_obj,
            upload_to_server=self.upload_to_server
        )
        success = upload_to_ydx.upload_to_ydx(ydx_server=self.ydx_server, AI_USER_ID=self.AI_USER_ID)
        if not success:
            raise Exception("Upload to YDX failed")

    async def run_generate_ydx_caption(self) -> None:
        generate_ydx_caption = GenerateYDXCaption(self.video_runner_obj)
        success = await generate_ydx_caption.generateYDXCaption(
            ydx_server=self.ydx_server,
            ydx_app_host=self.ydx_app_host,
            userId=self.userId,
            AI_USER_ID=self.AI_USER_ID,
        )
        if not success:
            raise Exception("Generate YDX caption failed")

    async def run_full_pipeline(self) -> None:
        self.logger.info(f"Starting pipeline for video: {self.video_id}")
        try:
            for task in self.tasks:
                await self.run_task(task)
            self.logger.info(f"Pipeline completed successfully for video: {self.video_id}")
        except Exception as e:
            self.logger.error(f"Pipeline failed for video {self.video_id}: {str(e)}", exc_info=True)
            raise

async def cleanup_failed_pipeline(self, error_message: str):
    """Clean up resources on pipeline failure."""
    try:
        video_folder = return_video_folder_name({"video_id": self.video_id})
        if os.path.exists(video_folder):
            shutil.rmtree(video_folder)
            self.logger.info(f"Cleaned up video folder: {video_folder}")

        update_status(self.video_id, self.AI_USER_ID, "failed")
        self.logger.info(f"Updated status to failed for video {self.video_id}")
    except Exception as e:
        self.logger.error(f"Cleanup error: {str(e)}")


def cleanup_resources(self):
    """Clean up temporary resources."""
    try:
        # Clean up service manager resources
        service_manager.cleanup()
        self.logger.info("Cleaned up service manager resources")
    except Exception as e:
        self.logger.error(f"Resource cleanup error: {str(e)}")

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
    """Main pipeline execution function."""
    try:
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

        if isinstance(pipeline_runner.progress, dict):
            if all(pipeline_runner.progress.get(task) == "completed" for task in pipeline_runner.tasks):
                pipeline_runner.logger.info(f"Pipeline already completed for video: {video_id}")
                return
        else:
            pipeline_runner.logger.error(f"Invalid progress data for video: {video_id}")
            raise ValueError("Progress data is not a valid dictionary.")

        await pipeline_runner.run_full_pipeline()

    except Exception as e:
        pipeline_runner.logger.error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    load_dotenv()
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--video_id", help="Video Id", type=str)
    parser.add_argument("--upload_to_server", help="Upload To YDX Server", action="store_true")
    parser.add_argument("--start_time", default=None, help="Start Time", type=str)
    parser.add_argument("--end_time", default=None, help="End Time", type=str)
    args = parser.parse_args()

    asyncio.run(run_pipeline(
        video_id=args.video_id,
        video_start_time=args.start_time,
        video_end_time=args.end_time,
        upload_to_server=args.upload_to_server
    ))