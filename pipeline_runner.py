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
    def __init__(self, video_id, pagePort, video_start_time, video_end_time,upload_to_server):
        self.video_id = video_id
        self.pagePort = pagePort
        self.video_start_time = video_start_time
        self.video_end_time = video_end_time
        self.upload_to_server = upload_to_server


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
        frame_extraction = FrameExtraction(video_runner_obj,int(os.environ["FRAME_EXTRACTION_RATE"] or 3))
        frame_extraction.extract_frames()
        ## OCR extraction
        ocr_extraction = OcrExtraction(video_runner_obj)
        ocr_extraction.run_ocr_detection()
        ## Object detection
        object_detection = ObjectDetection(video_runner_obj,self.pagePort)
        object_detection.run_object_detection()
        ## Keyframe selection
        keyframe_selection = KeyframeSelection(video_runner_obj)
        keyframe_selection.run_keyframe_selection()
        ## Image captioning
        image_captioning = ImageCaptioning(video_runner_obj)
        image_captioning.run_image_captioning()
        image_captioning.combine_captions_objects()
        ##TODO Caption rating
        caption_rating = CaptionRating(video_runner_obj)
        caption_rating.get_caption_rating()
        ## Scene segmentation
        scene_segmentation = SceneSegmentation(video_runner_obj)
        scene_segmentation.run_scene_segmentation()
        ## Text summarization
        text_summarization = TextSummarization(video_runner_obj)
        text_summarization.generate_text_summary()
        ## Upload to YDX
        upload_to_YDX = UploadToYDX(video_runner_obj,upload_to_server=self.upload_to_server)
        upload_to_YDX.upload_to_ydx()


if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--yolo", default=8081, help="Yolo Port", type=int)
    parser.add_argument("--video_id", help="Video Id", type=str)
    parser.add_argument("--upload_to_server", help="Upload To YDX Server",action ='store_true')
    parser.add_argument("--start_time", default=None, help="Start Time", type=str)
    parser.add_argument("--end_time", default=None, help="End Time", type=str)
    args = parser.parse_args()
    video_id = args.video_id
    pagePort = args.yolo
    video_start_time = args.start_time
    video_end_time = args.end_time
    upload_to_server = args.upload_to_server
    pipeline_runner = PipelineRunner(
        video_id, pagePort, video_start_time, video_end_time,upload_to_server
    )
    pipeline_runner.run_full_pipeline()
    
    
    #python pipeline_runner.py --video_id wzh0EuLhRhE --start_time 30 --end_time 35