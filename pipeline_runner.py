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


class PipelineRunner:
    def __init__(self, video_id, pagePort, video_start_time, video_end_time):
        self.video_id = video_id
        self.pagePort = pagePort
        self.video_start_time = video_start_time
        self.video_end_time = video_end_time

    @timeit
    def run_full_pipeline(self):
        ## Download video from YouTube
        import_video = ImportVideo(self.video_id,self.video_start_time,self.video_end_time)
        import_video.download_video()
        ## Extract audio from video
        extract_audio = ExtractAudio(self.video_id)
        extract_audio.extract_audio()
        ## Speech to text
        speech_to_text = SpeechToText(self.video_id)
        speech_to_text.get_speech_from_audio()
        ## Frame extraction
        frame_extraction = FrameExtraction(self.video_id)
        frame_extraction.extract_frames()
        ## OCR extraction
        ocr_extraction = OcrExtraction(self.video_id)
        ocr_extraction.run_ocr_detection()
        ## Object detection
        object_detection = ObjectDetection(self.video_id,self.pagePort)
        object_detection.run_object_detection()
        ## Keyframe selection
        keyframe_selection = KeyframeSelection(self.video_id)
        keyframe_selection.run_keyframe_selection()
        ## Image captioning
        image_captioning = ImageCaptioning(self.video_id)
        image_captioning.run_image_captioning()
        image_captioning.combine_captions_objects()
        ##TODO Caption rating
        caption_rating = CaptionRating(self.video_id)
        ## Scene segmentation
        scene_segmentation = SceneSegmentation(self.video_id)
        scene_segmentation.run_scene_segmentation()
        ## Text summarization
        text_summarization = TextSummarization(self.video_id)
        text_summarization.generate_text_summary()
        ## Upload to YDX
        upload_to_YDX = UploadToYDX(self.video_id)
        upload_to_YDX.upload_to_ydx()


if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--yolo", default=8081, help="Yolo Port", type=int)
    parser.add_argument("--video_id", help="Video Id", type=str)
    parser.add_argument("--start_time", default=None, help="Start Time", type=str)
    parser.add_argument("--end_time", default=None, help="End Time", type=str)
    args = parser.parse_args()
    video_id = args.videoid
    pagePort = args.yolo
    video_start_time = args.start_time
    video_end_time = args.end_time
    if video_start_time is not None and video_end_time is not None:
        os.environ["START_TIME"] = video_start_time
        os.environ["END_TIME"] = video_end_time
    pipeline_runner = PipelineRunner(
        video_id, pagePort, video_start_time, video_end_time
    )
    pipeline_runner.run_full_pipeline()
