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
        print(f"Initializing PipelineRunner for video_id: {video_id}")
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
        print("PipelineRunner initialization complete")

    def setup_logger(self) -> logging.Logger:
        print("Setting up logger")
        logger = logging.getLogger(f"PipelineLogger-{self.video_id}")
        logger.setLevel(logging.INFO)
        handler = logging.FileHandler(f"pipeline_logs/{self.video_id}_{self.AI_USER_ID}pipeline.log")
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        print("Logger setup complete")
        return logger

    def load_progress(self) -> Dict[str, Any]:
        print("Loading progress")
        progress = load_progress_from_file({"video_id": self.video_id}) or {}
        print(f"Loaded progress: {progress}")
        return progress

    def save_progress(self):
        print("Saving progress")
        save_progress_to_file({"video_id": self.video_id}, self.progress)
        print("Progress saved")

    def run_task(self, task: str, *args, **kwargs) -> Any:
        print(f"Attempting to run task: {task}")
        try:
            self.logger.info(f"Starting task: {task}")
            result = getattr(self, f"run_{task}")(*args, **kwargs)
            self.progress[task] = "completed"
            self.save_progress()
            self.logger.info(f"Completed task: {task}")
            print(f"Task {task} completed successfully")
            return result
        except Exception as e:
            print(f"Error occurred in task {task}: {str(e)}")
            self.logger.error(f"Error in task {task}: {str(e)}", exc_info=True)
            self.progress[task] = "failed"
            self.save_progress()
            raise

    def run_import_video(self) -> None:
        print("Starting run_import_video method")
        try:
            import_video = ImportVideo({"video_id": self.video_id, "logger": self.logger})
            success = import_video.download_video()
            if not success:
                raise Exception("Video import failed")
            print("Import Video completed successfully")
        except Exception as e:
            print(f"Error occurred in video import: {str(e)}")
            self.logger.error(f"Error in video import: {str(e)}", exc_info=True)
            raise

    def run_extract_audio(self) -> None:
        print("Starting run_extract_audio method")
        try:
            extract_audio = ExtractAudio({"video_id": self.video_id, "logger": self.logger})
            success = extract_audio.extract_audio()
            if not success:
                raise Exception("Audio extraction failed")
            print("Audio extraction completed successfully")
        except Exception as e:
            print(f"Error occurred in audio extraction: {str(e)}")
            self.logger.error(f"Error in audio extraction: {str(e)}", exc_info=True)
            raise

    def run_speech_to_text(self) -> None:
        print("Starting run_speech_to_text method")
        try:
            speech_to_text = SpeechToText({"video_id": self.video_id, "logger": self.logger})
            success = speech_to_text.get_speech_from_audio()
            if not success:
                raise Exception("Speech to text conversion failed")
            print("Speech to text conversion completed successfully")
        except Exception as e:
            print(f"Error occurred in speech to text conversion: {str(e)}")
            self.logger.error(f"Error in speech to text conversion: {str(e)}", exc_info=True)
            raise

    def run_frame_extraction(self) -> None:
        print("Starting run_frame_extraction method")
        try:
            frame_extraction = FrameExtraction(
                {"video_id": self.video_id, "logger": self.logger},
                int(os.environ.get("FRAME_EXTRACTION_RATE", 3))
            )
            success = frame_extraction.extract_frames()
            if not success:
                raise Exception("Frame extraction failed")
            print("Frame extraction completed successfully")
        except Exception as e:
            print(f"Error occurred in frame extraction: {str(e)}")
            self.logger.error(f"Error in frame extraction: {str(e)}", exc_info=True)
            raise

    def run_ocr_extraction(self) -> None:
        print("Starting run_ocr_extraction method")
        try:
            ocr_extraction = OcrExtraction({"video_id": self.video_id, "logger": self.logger})
            success = ocr_extraction.run_ocr_detection()
            if not success:
                raise Exception("OCR extraction failed")
            print("OCR extraction completed successfully")
        except Exception as e:
            print(f"Error occurred in OCR extraction: {str(e)}")
            self.logger.error(f"Error in OCR extraction: {str(e)}", exc_info=True)
            raise

    def run_object_detection(self) -> None:
        print("Starting run_object_detection method")
        try:
            object_detection = ObjectDetection({"video_id": self.video_id, "logger": self.logger})
            success = object_detection.run_object_detection()
            if not success:
                raise Exception("Object detection failed")
            print("Object detection completed successfully")
        except Exception as e:
            print(f"Error occurred in object detection: {str(e)}")
            self.logger.error(f"Error in object detection: {str(e)}", exc_info=True)
            raise

    def run_keyframe_selection(self) -> None:
        print("Starting run_keyframe_selection method")
        try:
            keyframe_selection = KeyframeSelection({"video_id": self.video_id, "logger": self.logger})
            success = keyframe_selection.run_keyframe_selection()
            if not success:
                raise Exception("Keyframe selection failed")
            print("Keyframe selection completed successfully")
        except Exception as e:
            print(f"Error occurred in keyframe selection: {str(e)}")
            self.logger.error(f"Error in keyframe selection: {str(e)}", exc_info=True)
            raise

    def run_image_captioning(self) -> None:
        print("Starting run_image_captioning method")
        try:
            image_captioning = ImageCaptioning({"video_id": self.video_id, "logger": self.logger})
            success = image_captioning.run_image_captioning()
            if not success:
                raise Exception("Image captioning failed")
            combined_success = image_captioning.combine_image_caption()
            if not combined_success:
                raise Exception("Combining image captions failed")
            print("Image captioning completed successfully")
        except Exception as e:
            print(f"Error occurred in image captioning: {str(e)}")
            self.logger.error(f"Error in image captioning: {str(e)}", exc_info=True)
            raise

    def run_caption_rating(self) -> None:
        print("Starting run_caption_rating method")
        try:
            caption_rating = CaptionRating({"video_id": self.video_id, "logger": self.logger})
            success = caption_rating.perform_caption_rating()
            if not success:
                raise Exception("Caption rating failed")
            print("Caption rating completed successfully")
        except Exception as e:
            print(f"Error occurred in caption rating: {str(e)}")
            self.logger.error(f"Error in caption rating: {str(e)}", exc_info=True)
            raise

    def run_scene_segmentation(self) -> None:
        print("Starting run_scene_segmentation method")
        try:
            scene_segmentation = SceneSegmentation({"video_id": self.video_id, "logger": self.logger})
            success = scene_segmentation.run_scene_segmentation()
            if not success:
                raise Exception("Scene segmentation failed")
            print("Scene segmentation completed successfully")
        except Exception as e:
            print(f"Error occurred in scene segmentation: {str(e)}")
            self.logger.error(f"Error in scene segmentation: {str(e)}", exc_info=True)
            raise

    def run_text_summarization(self) -> None:
        print("Starting run_text_summarization method")
        try:
            text_summarization = TextSummaryCoordinator({"video_id": self.video_id, "logger": self.logger})
            success = text_summarization.generate_text_summary()
            if not success:
                raise Exception("Text summarization failed")
            print("Text summarization completed successfully")
        except Exception as e:
            print(f"Error occurred in text summarization: {str(e)}")
            self.logger.error(f"Error in text summarization: {str(e)}", exc_info=True)
            raise

    def run_upload_to_ydx(self) -> None:
        print("Starting run_upload_to_ydx method")
        try:
            upload_to_ydx = UploadToYDX(
                {"video_id": self.video_id, "logger": self.logger},
                upload_to_server=self.upload_to_server
            )
            success = upload_to_ydx.upload_to_ydx(ydx_server=self.ydx_server, AI_USER_ID=self.AI_USER_ID)
            if not success:
                raise Exception("Upload to YDX failed")
            print("Upload to YDX completed successfully")
        except Exception as e:
            print(f"Error occurred in upload to YDX: {str(e)}")
            self.logger.error(f"Error in upload to YDX: {str(e)}", exc_info=True)
            raise

    def run_generate_ydx_caption(self) -> None:
        print("Starting run_generate_ydx_caption method")
        try:
            generate_ydx_caption = GenerateYDXCaption({"video_id": self.video_id, "logger": self.logger})
            success = generate_ydx_caption.generateYDXCaption(
                ydx_server=self.ydx_server,
                ydx_app_host=self.ydx_app_host,
                userId=self.userId,
                AI_USER_ID=self.AI_USER_ID,
            )
            if not success:
                raise Exception("Generate YDX caption failed")
            print("Generate YDX caption completed successfully")
        except Exception as e:
            print(f"Error occurred in generate YDX caption: {str(e)}")
            self.logger.error(f"Error in generate YDX caption: {str(e)}", exc_info=True)
            raise

    def run_full_pipeline(self) -> None:
        print("STARTING RUN FULL PIPELINE")
        self.logger.info(f"Starting pipeline for video: {self.video_id}")

        try:
            for task in self.tasks:
                print(f"Preparing to run task: {task}")
                self.run_task(task)
                print(f"Finished running task: {task}")

            print("All tasks completed successfully")
            self.logger.info(f"Pipeline completed successfully for video: {self.video_id}")
        except Exception as e:
            print(f"Pipeline failed for video {self.video_id}: {str(e)}")
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
    print(f"Starting run_pipeline function for video_id: {video_id}")
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
    print("PipelineRunner instance created, starting full pipeline")
    pipeline_runner.run_full_pipeline()
    print("Full pipeline execution completed")

if __name__ == "__main__":
    print("Entering main block")
    load_dotenv()
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--video_id", help="Video Id", type=str)
    parser.add_argument("--upload_to_server", help="Upload To YDX Server", action="store_true")
    parser.add_argument("--start_time", default=None, help="Start Time", type=str)
    parser.add_argument("--end_time", default=None, help="End Time", type=str)
    args = parser.parse_args()

    print(
        f"Parsed arguments: video_id={args.video_id}, upload_to_server={args.upload_to_server}, start_time={args.start_time}, end_time={args.end_time}")

    run_pipeline(
        video_id=args.video_id,
        video_start_time=args.start_time,
        video_end_time=args.end_time,
        upload_to_server=args.upload_to_server
    )
    print("Pipeline execution completed, exiting main block")