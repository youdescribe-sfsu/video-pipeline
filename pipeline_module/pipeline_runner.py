# ! /usr/bin/env python
from dotenv import load_dotenv
import os
import argparse
from utils_module.timeit_decorator import timeit
from import_video_submodule.import_video import ImportVideo
from extract_audio_submodule.extract_audio import ExtractAudio
from speech_to_text_submodule.speech_to_text import SpeechToText
from frame_extraction_submodule.frame_extraction import FrameExtraction
from ocr_extraction_submodule.ocr_extraction import OcrExtraction
from object_detection_submodule.object_detection import ObjectDetection
from keyframe_selection_submodule.keyframe_selection import KeyframeSelection
from image_captioning_submodule.image_captioning import ImageCaptioning
from caption_rating_submodule.caption_rating import CaptionRating
from scene_segmentation_submodule.scene_segmentation import SceneSegmentation
from text_summarization_submodule.text_summary import TextSummarization
from upload_to_YDX_submodule.upload_to_YDX import UploadToYDX
from generate_YDX_caption_submodule.generate_ydx_caption import GenerateYDXCaption
from utils_module.utils import (
    DEFAULT_SAVE_PROGRESS,
    PipelineTask,
    load_progress_from_file,
    return_video_folder_name,
    save_progress_to_file,
)
import logging
from multi_thread_pipeline import run_pipeline_multi_thread
from utils_module.ydx_caption import run_generate_ydx_caption


class PipelineRunner:
    ALL_TASKS = [t.value for t in PipelineTask]

    def __init__(
        self,
        video_id,
        video_start_time,
        video_end_time,
        upload_to_server,
        tasks=None,
        ydx_server=None,
        ydx_app_host=None,
        userId=None,
        aiUserId=None,
    ):
        self.video_id = video_id
        self.video_start_time = video_start_time
        self.video_end_time = video_end_time
        self.upload_to_server = upload_to_server
        if tasks is None:
            self.tasks = self.ALL_TASKS
        else:
            self.tasks = tasks

        self.ydx_server = ydx_server
        self.ydx_app_host = ydx_app_host
        self.userId = userId
        self.aiUserId = aiUserId

    def setup_logger(self, video_runner_obj):
        os.makedirs(return_video_folder_name(video_runner_obj), exist_ok=True)
        log_file = f"{return_video_folder_name(video_runner_obj)}/pipeline.log"
        log_mode = "a" if os.path.exists(log_file) else "w"
        logger = logging.getLogger(f"PipelineLogger-{video_runner_obj['video_id']}")
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler = logging.FileHandler(log_file, mode=log_mode)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        return logger

    @timeit
    def run_full_pipeline(self):
        ## Download video from YouTube
        logger = self.setup_logger(
            video_runner_obj={
                "video_id": self.video_id,
                "video_start_time": self.video_start_time,
                "video_end_time": self.video_end_time,
            }
        )
        logger.info(f"Processing video: {self.video_id}")

        video_runner_obj = {
            "video_id": self.video_id,
            "video_start_time": self.video_start_time,
            "video_end_time": self.video_end_time,
            "logger": logger,
        }

        progress_file = load_progress_from_file(video_runner_obj=video_runner_obj)
        if progress_file is None:
            progress_file = DEFAULT_SAVE_PROGRESS
            progress_file["video_id"] = self.video_id
            save_progress_to_file(
                video_runner_obj=video_runner_obj, progress_data=progress_file
            )

        import_video = ImportVideo(video_runner_obj)
        import_video.download_video()
        # Extract audio from video
        extract_audio = ExtractAudio(video_runner_obj)
        extract_audio.extract_audio()
        # Speech to text
        speech_to_text = SpeechToText(video_runner_obj)
        speech_to_text.get_speech_from_audio()
        ## Frame extraction
        frame_extraction = FrameExtraction(
            video_runner_obj, int(os.environ.get("FRAME_EXTRACTION_RATE", 3))
        )
        frame_extraction.extract_frames()
        ## OCR extraction
        if PipelineTask.OCR_EXTRACTION.value in self.tasks:
            ocr_extraction = OcrExtraction(video_runner_obj)
            ocr_extraction.run_ocr_detection()
        ## Object detection
        if PipelineTask.OBJECT_DETECTION.value in self.tasks:
            object_detection = ObjectDetection(video_runner_obj)
            object_detection.run_object_detection()
            keyframe_selection = KeyframeSelection(video_runner_obj)
            keyframe_selection.run_keyframe_selection()
        ## Image captioning
        if PipelineTask.IMAGE_CAPTIONING.value in self.tasks:
            image_captioning = ImageCaptioning(video_runner_obj)
            image_captioning.run_image_captioning()
            image_captioning.combine_image_caption()
        ## Caption rating
        if PipelineTask.CAPTION_RATING.value in self.tasks:
            caption_rating = CaptionRating(video_runner_obj)
            caption_rating.perform_caption_rating()
        ## Scene segmentation
        if PipelineTask.SCENE_SEGMENTATION.value in self.tasks:
            scene_segmentation = SceneSegmentation(video_runner_obj)
            scene_segmentation.run_scene_segmentation()
        ## Text summarization
        ## Check for better summarization with GPT-3/3.5
        if PipelineTask.TEXT_SUMMARIZATION.value in self.tasks:
            text_summarization = TextSummarization(video_runner_obj)
            text_summarization.generate_text_summary()
        ## Upload to YDX
        upload_to_YDX = UploadToYDX(
            video_runner_obj, upload_to_server=self.upload_to_server
        )
        upload_to_YDX.upload_to_ydx(ydx_server=self.ydx_server)
        if self.upload_to_server:
            # generate_YDX_caption = GenerateYDXCaption(video_runner_obj)
            # generate_YDX_caption.generateYDXCaption(
            #     ydx_server=self.ydx_server,
            #     ydx_app_host=self.ydx_app_host,
            #     userId=self.userId,
            #     aiUserId=self.aiUserId,
            # )
            run_generate_ydx_caption(self.video_id, self.aiUserId)

    def run_multi_thread_pipeline(self):
        logger = self.setup_logger(
            {
                "video_id": self.video_id,
                "video_start_time": self.video_start_time,
                "video_end_time": self.video_end_time,
            }
        )
        run_pipeline_multi_thread(
            video_id=self.video_id,
            video_start_time=self.video_start_time,
            video_end_time=self.video_end_time,
            upload_to_server=self.upload_to_server,
            logger=logger,
            ydx_server=self.ydx_server,
            ydx_app_host=self.ydx_app_host,
            userId=self.userId,
            aiUserId=self.aiUserId,
        )
        return


def run_pipeline(
    video_id,
    video_start_time,
    video_end_time,
    upload_to_server=False,
    multi_thread=False,
    tasks=None,
    ydx_server=None,
    ydx_app_host=None,
    userId=None,
    aiUserId=None,
):
    pipeline_runner = PipelineRunner(
        video_id=video_id,
        video_start_time=video_start_time,
        video_end_time=video_end_time,
        upload_to_server=upload_to_server,
        tasks=tasks,
        ydx_server=ydx_server,
        ydx_app_host=ydx_app_host,
        userId=userId,
        aiUserId=aiUserId,
    )
    if multi_thread:
        pipeline_runner.run_multi_thread_pipeline()
    else:
        pipeline_runner.run_full_pipeline()
    return



if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--video_id", help="Video Id", type=str)
    parser.add_argument(
        "--upload_to_server", help="Upload To YDX Server", action="store_true"
    )
    parser.add_argument("--start_time", default=None, help="Start Time", type=str)
    parser.add_argument("--end_time", default=None, help="End Time", type=str)
    args = parser.parse_args()
    video_id = args.video_id
    video_start_time = args.start_time
    video_end_time = args.end_time
    upload_to_server = args.upload_to_server
    run_pipeline(video_id, video_start_time, video_end_time, upload_to_server)

    # python pipeline_runner.py --video_id c1ROUg6rOGs --start_time 6 --end_time 11
    # python pipeline_runner.py --video_id uqOtCbvFUZA > uqOtCbvFUZA.log
