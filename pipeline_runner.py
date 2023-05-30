# Runs all parts of the video processing pipeline except downloading the video
# ! /usr/bin/env python
from dotenv import load_dotenv
import os
import argparse
from timeit_decorator import timeit
from import_video_module.import_video import ImportVideo
from extract_audio_module.extract_audio import ExtractAudio
from speech_to_text_module.speech_to_text import SpeechToText
from frame_extraction_module.frame_extraction import FrameExtraction
from ocr_extraction_module.ocr_extraction import OcrExtraction
from object_detection_module.object_detection import ObjectDetection
from keyframe_selection_module.keyframe_selection import KeyframeSelection
from image_captioning_module.image_captioning import ImageCaptioning
from caption_rating_module.caption_rating import CaptionRating
from scene_segmentation_module.scene_segmentation import SceneSegmentation
from text_summarization_module.text_summary import TextSummarization
from upload_to_YDX_module.upload_to_YDX import UploadToYDX
from generate_YDX_caption_module.generate_ydx_caption import GenerateYDXCaption
from utils import PipelineTask


class PipelineRunner:
    ALL_TASKS = [t.value for t in PipelineTask]
    def __init__(self, video_id, video_start_time, video_end_time,upload_to_server, tasks=None):
        self.video_id = video_id
        self.video_start_time = video_start_time
        self.video_end_time = video_end_time
        self.upload_to_server = upload_to_server
        if(tasks is None):
            self.tasks = self.ALL_TASKS
        else:
            self.tasks = tasks


    @timeit
    def run_full_pipeline(self):
        ## Download video from YouTube
        video_runner_obj = {
            "video_id": self.video_id,
            "video_start_time": self.video_start_time,
            "video_end_time": self.video_end_time
        }
        import_video = ImportVideo(video_runner_obj)
        import_video.download_video()
        # Extract audio from video
        extract_audio = ExtractAudio(video_runner_obj)
        extract_audio.extract_audio()
        # Speech to text
        speech_to_text = SpeechToText(video_runner_obj)
        speech_to_text.get_speech_from_audio()
        ## Frame extraction
        if PipelineTask.FRAME_EXTRACTION.value in self.tasks:
            frame_extraction = FrameExtraction(video_runner_obj, int(os.environ.get("FRAME_EXTRACTION_RATE", 3)))
            frame_extraction.extract_frames()
        ## OCR extraction
        if PipelineTask.OCR_EXTRACTION.value in self.tasks:
            ocr_extraction = OcrExtraction(video_runner_obj)
            ocr_extraction.run_ocr_detection()
        ## Object detection
        if PipelineTask.OBJECT_DETECTION.value in self.tasks:
            object_detection = ObjectDetection(video_runner_obj)
            object_detection.run_object_detection()
        ## Keyframe selection
        if PipelineTask.KEYFRAME_SELECTION.value in self.tasks:
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
            caption_rating.get_all_caption_rating()
            caption_rating.filter_captions()
        ## Scene segmentation
        if PipelineTask.SCENE_SEGMENTATION.value in self.tasks:
            scene_segmentation = SceneSegmentation(video_runner_obj)
            scene_segmentation.run_scene_segmentation()
        ## Text summarization
        if PipelineTask.TEXT_SUMMARIZATION.value in self.tasks:
            text_summarization = TextSummarization(video_runner_obj)
            text_summarization.generate_text_summary()
        ## Upload to YDX
        upload_to_YDX = UploadToYDX(video_runner_obj,upload_to_server=self.upload_to_server)
        upload_to_YDX.upload_to_ydx()
        if(self.upload_to_server):
            generate_YDX_caption = GenerateYDXCaption(video_runner_obj)
            generate_YDX_caption.generateYDXCaption()


def run_pipeline(video_id, video_start_time, video_end_time,upload_to_server, tasks=None):
    pipeline_runner = PipelineRunner(
        video_id, video_start_time, video_end_time,upload_to_server, tasks
    )
    pipeline_runner.run_full_pipeline()
    return


if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--video_id", help="Video Id", type=str)
    parser.add_argument("--upload_to_server", help="Upload To YDX Server",action ='store_true')
    parser.add_argument("--start_time", default=None, help="Start Time", type=str)
    parser.add_argument("--end_time", default=None, help="End Time", type=str)
    args = parser.parse_args()
    video_id = args.video_id
    video_start_time = args.start_time
    video_end_time = args.end_time
    upload_to_server = args.upload_to_server
    run_pipeline(video_id, video_start_time, video_end_time,upload_to_server)
    
    
    #python pipeline_runner.py --video_id wzh0EuLhRhE --start_time 6 --end_time 11