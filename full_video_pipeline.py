# Runs all parts of the video processing pipeline except downloading the video
#! /usr/bin/env python
from extract_frames import extract_frames
from detect_objects import object_tracking_to_csv
from ocr_keyframes import print_all_ocr
from filter_ocr import filter_ocr, filter_ocr_agreement, filter_ocr_remove_similarity
from keyframe_timestamps import keyframes_from_object_tracking
from keyframe_captions import captions_to_csv
from combine_captions_objects import combine_captions_objects
from import_youtube import import_video
from keyframes_revised_script_with_scenes import insert_key_frames
from text_summarization import text_summarization, text_summarization_csv
from subprocess import call
import sys
from nodejs import node
from dotenv import load_dotenv
from utils import returnVideoFolderName,returnVideoFramesFolder,FRAMES,CAPTIONS_AND_OBJECTS_CSV,OUTPUT_AVG_CSV,SCENE_SEGMENTED_FILE_CSV
import os
import shutil
from speechToText import google_transcribe,getAudioFromVideo
from data_upload import upload_data
from generateYDXCaptions import generateYDXCaption
load_dotenv()


if __name__ == "__main__":

    video_id = sys.argv[1]
    video_name = sys.argv[1] + FRAMES
    path = returnVideoFolderName(video_id)
    os.makedirs(path, exist_ok=True)
    print("=== DOWNLOAD VIDEO ===")
    import_video(video_id)
    # # Frame extraction
    print("=== EXTRACT FRAMES ===")
    extract_frames(video_id, 10, True)

    # TODO automate restart on code break
    # OCR
    print("=== GET ALL OCR ===")
    print_all_ocr(video_id)
    print("=== FILTER OCR V1 ===")
    filter_ocr(video_id)
    print("=== FILTER OCR V2 ===")
    filter_ocr_agreement(video_id)
    print("=== REMOVE SIMILAR OCR ===")
    filter_ocr_remove_similarity(video_id)

    # # Keyframe selection
    print("=== TRACK OBJECTS ===")
    object_tracking_to_csv(video_id)
    print("=== FIND KEYFRAMES ===")
    keyframes_from_object_tracking(video_id)

    # Keyframe captioning
    print("=== GET CAPTIONS ===")
    captions_to_csv(video_id)
    print("=== COMBINE CAPTIONS AND OBJECTS ===")
    combine_captions_objects(video_id)

    # TODO VILBERT SCORING

    # TODO Convert to python
    node.call(['csv.js',path+'/'+CAPTIONS_AND_OBJECTS_CSV,path+'/'+OUTPUT_AVG_CSV])
    node.call(['sceneSegmentation.js',path+'/'+OUTPUT_AVG_CSV,path+'/'+SCENE_SEGMENTED_FILE_CSV])
    text_summarization_csv(video_id)
    getAudioFromVideo(video_id)
    google_transcribe(video_id)
    upload_data(video_id)
    generateYDXCaption(video_id)
    shutil.rmtree(returnVideoFramesFolder(video_id))
    print("=== DONE! ===")
