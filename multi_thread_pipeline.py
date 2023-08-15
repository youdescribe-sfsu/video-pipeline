# ! /usr/bin/env python
import os
import threading
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
from utils import DEFAULT_SAVE_PROGRESS, load_progress_from_file, save_progress_to_file


@timeit
def run_pipeline_multi_thread(video_id, video_start_time, video_end_time,upload_to_server,logger):
    ## Run the pipeline in parallel
    
    
    video_runner_obj = {
        "video_id": video_id,
        "video_start_time": video_start_time,
        "video_end_time": video_end_time,
        "logger": logger
    }
    
    
    progress_file = load_progress_from_file(video_runner_obj=video_runner_obj)
    if progress_file is None:
        progress_file = DEFAULT_SAVE_PROGRESS
        progress_file['video_id'] = video_id
        save_progress_to_file(video_runner_obj=video_runner_obj, progress_data=progress_file)
        
    #####################################    
    ## Main Thread
    #####################################
    import_video = ImportVideo(video_runner_obj)
    import_video.download_video()
    
    #####################################
    ## 2 Threads for audio and frames
    #####################################
    
    
    extract_audio = ExtractAudio(video_runner_obj)
    speech_to_text = SpeechToText(video_runner_obj)
    frame_extraction = FrameExtraction(video_runner_obj, int(os.environ.get("FRAME_EXTRACTION_RATE", 3)))

    def run_frame_extraction():
        # Extract frames using the FrameExtraction operation
        frame_extraction.extract_frames()

    def run_audio_and_speech():
        # Extract audio using the ExtractAudio operation
        extract_audio.extract_audio()
        
        # Get speech from audio using the SpeechToText operation
        speech_to_text.get_speech_from_audio()

    # Create threads for running operations
    frame_extraction_thread = threading.Thread(target=run_frame_extraction)
    audio_and_speech_thread = threading.Thread(target=run_audio_and_speech)

    # Start both threads
    frame_extraction_thread.start()
    audio_and_speech_thread.start()

    # Wait for both threads to finish
    frame_extraction_thread.join()
    audio_and_speech_thread.join()
    
    
    #####################################
    ## Finish
    #####################################
    
    
    #####################################
    ## 3 Threads for OCR, Object Detection and Image Captioning
    #####################################
    
    
    ## OCR extraction

    ocr_extraction = OcrExtraction(video_runner_obj)
    object_detection = ObjectDetection(video_runner_obj)
    ## Image captioning
    image_captioning = ImageCaptioning(video_runner_obj)
    
    def run_ocr():
        ocr_extraction.run_ocr_detection()
    
    
    def run_object_detection_and_keyframe_selection():
        object_detection.run_object_detection()
        keyframe_selection = KeyframeSelection(video_runner_obj)
        keyframe_selection.run_keyframe_selection()
    
    
    def run_image_captioning():
        image_captioning.run_image_captioning()
        
    
    ocr_thread = threading.Thread(target=run_ocr)
    object_detection_and_keyframe_selection_thread = threading.Thread(target=run_object_detection_and_keyframe_selection)
    image_captioning_thread = threading.Thread(target=run_image_captioning)
    
    ocr_thread.start()
    object_detection_and_keyframe_selection_thread.start()
    image_captioning_thread.start()
    
    ocr_thread.join()
    object_detection_and_keyframe_selection_thread.join()
    image_captioning_thread.join()
    
    
    
    #####################################
    ## Finish
    ##################################### 
    
    ## Rest of the code should be run sequentially
           
    image_captioning.filter_keyframes_from_caption()
    image_captioning.combine_image_caption()
    ## Caption rating
    caption_rating = CaptionRating(video_runner_obj)
    caption_rating.perform_caption_rating()    
    ## Scene segmentation
    scene_segmentation = SceneSegmentation(video_runner_obj)
    scene_segmentation.run_scene_segmentation()
    ## Text summarization
    ## Check for better summarization with GPT-3/3.5
    text_summarization = TextSummarization(video_runner_obj)
    text_summarization.generate_text_summary()
    ## Upload to YDX
    upload_to_YDX = UploadToYDX(video_runner_obj,upload_to_server=upload_to_server)
    upload_to_YDX.upload_to_ydx()
    if(upload_to_server):
        generate_YDX_caption = GenerateYDXCaption(video_runner_obj)
        generate_YDX_caption.generateYDXCaption()
    
    return True