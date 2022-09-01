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
load_dotenv()


if __name__ == "__main__":

    video_id = sys.argv[1]
    video_name = sys.argv[1] + "_frames"
    print("=== DOWNLOAD VIDEO ===")
    import_video(video_id)
    # call("yt-dlp -vx --audio-format wav https://www.youtube.com/watch?v=" +
    #      video_id+" --ffmpeg-location \"C:\ffmpeg\bin -o "+video_id+".wav", shell=True)

    # # Frame extraction
    print("=== EXTRACT FRAMES ===")
    extract_frames(video_id, 10, True)

    # TODO automate restart on code break
    # OCR
    print("=== GET ALL OCR ===")
    print_all_ocr(video_name)
    print("=== FILTER OCR V1 ===")
    filter_ocr(video_name)
    print("=== FILTER OCR V2 ===")
    filter_ocr_agreement(video_name)
    print("=== REMOVE SIMILAR OCR ===")
    filter_ocr_remove_similarity(video_id)

    # # Keyframe selection
    print("=== TRACK OBJECTS ===")
    object_tracking_to_csv(video_name)
    print("=== FIND KEYFRAMES ===")
    keyframes_from_object_tracking(video_name)

    # Keyframe captioning
    print("=== GET CAPTIONS ===")
    captions_to_csv(video_name)
    print("=== COMBINE CAPTIONS AND OBJECTS ===")
    combine_captions_objects(video_name)

    # TODO VILBERT SCORING

    # TODO Convert to python
    # call(["node", "../csv.js"], shell=True)
    # call(["node", "../sceneSegmentation.js"], shell=True)
    node.call(['csv.js'])
    node.call(['sceneSegmentation.js'])
    text_summarization_csv(file=None)

    # speech to text HERE

    print("=== DONE! ===")
