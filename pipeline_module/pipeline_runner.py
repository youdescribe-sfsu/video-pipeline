import logging
from typing import List, Dict, Any, Optional
import os
from dotenv import load_dotenv

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
from .utils_module.utils import (
    load_progress_from_file,
    save_progress_to_file,
    PipelineTask,
)

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
        self.progress = self.load_progress()

    def setup_logger(self) -> logging.Logger:
        logger = logging.getLogger(f"PipelineLogger-{self.video_id}")
        logger.setLevel(logging.INFO)
        handler = logging.FileHandler(f"pipeline_logs/{self.video_id}_{self.AI_USER_ID}pipeline.log")
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def load_progress(self) -> Dict[str, Any]:
        return load_progress_from_file({"video_id": self.video_id}) or {}

    def save_progress(self):
        save_progress_to_file({"video_id": self.video_id}, self.progress)

    def run_task(self, task: str, *args, **kwargs) -> Any:
        try:
            self.logger.info(f"Starting task: {task}")
            result = getattr(self, f"run_{task}")(*args, **kwargs)
            self.progress[task] = "completed"
            self.save_progress()
            self.logger.info(f"Completed task: {task}")
            return result
        except Exception as e:
            self.logger.error(f"Error in task {task}: {str(e)}", exc_info=True)
            self.progress[task] = "failed"
            self.save_progress()
            raise

    def run_import_video(self) -> None:
        import_video = ImportVideo({"video_id": self.video_id, "logger": self.logger})
        return import_video.download_video()

    def run_extract_audio(self) -> None:
        extract_audio = ExtractAudio({"video_id": self.video_id, "logger": self.logger})
        return extract_audio.extract_audio()

    def run_speech_to_text(self) -> None:
        speech_to_text = SpeechToText({"video_id": self.video_id, "logger": self.logger})
        return speech_to_text.get_speech_from_audio()

    def run_frame_extraction(self) -> None:
        frame_extraction = FrameExtraction(
            {"video_id": self.video_id, "logger": self.logger},
            int(os.environ.get("FRAME_EXTRACTION_RATE", 3))
        )
        return frame_extraction.extract_frames()

    def run_ocr_extraction(self) -> None:
        ocr_extraction = OcrExtraction({"video_id": self.video_id, "logger": self.logger})
        return ocr_extraction.run_ocr_detection()

    def run_object_detection(self) -> None:
        object_detection = ObjectDetection({"video_id": self.video_id, "logger": self.logger})
        return object_detection.run_object_detection()

    def run_keyframe_selection(self) -> None:
        keyframe_selection = KeyframeSelection({"video_id": self.video_id, "logger": self.logger})
        return keyframe_selection.run_keyframe_selection()

    def run_image_captioning(self) -> None:
        image_captioning = ImageCaptioning({"video_id": self.video_id, "logger": self.logger})
        image_captioning.run_image_captioning()
        return image_captioning.combine_image_caption()

    def run_caption_rating(self) -> None:
        caption_rating = CaptionRating({"video_id": self.video_id, "logger": self.logger})
        return caption_rating.perform_caption_rating()

    def run_scene_segmentation(self) -> None:
        scene_segmentation = SceneSegmentation({"video_id": self.video_id, "logger": self.logger})
        return scene_segmentation.run_scene_segmentation()

    def run_text_summarization(self) -> None:
        text_summarization = TextSummaryCoordinator({"video_id": self.video_id, "logger": self.logger})
        return text_summarization.generate_text_summary()

    def run_upload_to_ydx(self) -> None:
        upload_to_ydx = UploadToYDX(
            {"video_id": self.video_id, "logger": self.logger},
            upload_to_server=self.upload_to_server
        )
        return upload_to_ydx.upload_to_ydx(ydx_server=self.ydx_server, AI_USER_ID=self.AI_USER_ID)

    def run_generate_ydx_caption(self) -> None:
        generate_ydx_caption = GenerateYDXCaption({"video_id": self.video_id, "logger": self.logger})
        return generate_ydx_caption.generateYDXCaption(
            ydx_server=self.ydx_server,
            ydx_app_host=self.ydx_app_host,
            userId=self.userId,
            AI_USER_ID=self.AI_USER_ID,
        )

    def run_pipeline(self) -> None:
        self.logger.info(f"Starting pipeline for video: {self.video_id}")

        try:
            for task in self.tasks:
                self.run_task(task)

            self.logger.info(f"Pipeline completed successfully for video: {self.video_id}")
        except Exception as e:
            self.logger.error(f"Pipeline failed for video {self.video_id}: {str(e)}", exc_info=True)
            # Implement error handling and user notification here

def run_pipeline(
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
    pipeline_runner.run_pipeline()

if __name__ == "__main__":
    load_dotenv()
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--video_id", help="Video Id", type=str)
    parser.add_argument("--upload_to_server", help="Upload To YDX Server", action="store_true")
    parser.add_argument("--start_time", default=None, help="Start Time", type=str)
    parser.add_argument("--end_time", default=None, help="End Time", type=str)
    args = parser.parse_args()

    run_pipeline(
        video_id=args.video_id,
        video_start_time=args.start_time,
        video_end_time=args.end_time,
        upload_to_server=args.upload_to_server
    )