#!/usr/bin/python
# -*- coding: utf-8 -*-

FRAMES = '_frames'
OCR_TEXT_ANNOTATIONS_FILE_NAME = 'ocr_text_annotations.csv'
OCR_TEXT_CSV_FILE_NAME = 'ocr_text.csv'
OCR_FILTER_CSV_FILE_NAME = 'ocr_filter.csv'
OCR_FILTER_CSV_2_FILE_NAME = 'ocr_filter_2.csv'
OCR_FILTER_REMOVE_SIMILAR = 'ocr_filter_remove_similar.csv'
OBJECTS_CSV = 'objects.csv'
KEYFRAMES_CSV = 'keyframes.csv'
CAPTIONS_CSV = 'captions.csv'
CAPTIONS_AND_OBJECTS_CSV = 'captions_and_objects.csv'
OUTPUT_AVG_CSV = 'outputavg.csv'
SCENE_SEGMENTED_FILE_CSV = 'scenesegmentedfile.csv'
SUMMARIZED_SCENES = 'summarized_scenes.json'
TRANSCRIPTS = 'transcripts.json'
DIALOGS = 'dialogs.json'
VICR_CSV = 'vicr.csv'
COUNT_VERTICE = 'count_vertice.json'

## OCR CSV HEADERS

FRAME_INDEX_SELECTOR = 'frame_index'
TIMESTAMP_SELECTOR = 'timestamp'
OCR_TEXT_SELECTOR = 'ocr_text'

OCR_HEADERS = {FRAME_INDEX_SELECTOR: 'Frame Index',
               TIMESTAMP_SELECTOR: 'Timestamp',
               OCR_TEXT_SELECTOR: 'OCR Text'}

## KEYFRAMES CSV HEADERS

IS_KEYFRAME_SELECTOR = 'is_keyframe'
KEYFRAME_CAPTION_SELECTOR = 'caption'

KEY_FRAME_HEADERS = {
    FRAME_INDEX_SELECTOR: 'Frame Index',
    TIMESTAMP_SELECTOR: 'Timestamp',
    IS_KEYFRAME_SELECTOR: 'Is Keyframe',
    KEYFRAME_CAPTION_SELECTOR: 'Caption',
    }

CAPTION_IMAGE_PAIR = 'caption_image_pair.csv'
CAPTION_SCORE = 'caption_score.csv'


from enum import Enum
import json


class PipelineTask(Enum):
    DOWNLOAD_VIDEO = "download_video"
    EXTRACT_AUDIO = "extract_audio"
    SPEECH_TO_TEXT = "speech_to_text"
    FRAME_EXTRACTION = "frame_extraction"
    OCR_EXTRACTION = "ocr_extraction"
    OBJECT_DETECTION = "object_detection"
    KEYFRAME_SELECTION = "keyframe_selection"
    IMAGE_CAPTIONING = "image_captioning"
    CAPTION_RATING = "caption_rating"
    SCENE_SEGMENTATION = "scene_segmentation"
    TEXT_SUMMARIZATION = "text_summarization"
    UPLOAD_TO_YDX = "upload_to_ydx"

import os
from typing import Dict,Union

def return_video_folder_name(video_runner_obj: Dict[str, int]) -> str:
    """
    Returns the folder name for a video.

    Parameters:
    video_runner_obj (Dict[str, int]): A dictionary that contains the information of the video.
        The keys are "video_id", "video_start_time", and "video_end_time", and their values are integers.

    Returns:
    str: The folder name for the video.
    """
    video_id = video_runner_obj.get("video_id")
    video_start_time = video_runner_obj.get("video_start_time",None)
    video_end_time = video_runner_obj.get("video_end_time",None)
    CURRENT_ENV = os.environ.get("CURRENT_ENV", "production")

    if video_start_time is not None and video_end_time is not None:
        if CURRENT_ENV == "development":
            return f"{video_id}_files/part_start_{video_start_time}_{video_end_time}"
        else:
            return f"/home/datasets/pipeline/{video_id}_files/part_start_{video_start_time}_end_{video_end_time}_files"

    if CURRENT_ENV == "development":
        return f"{video_id}_files"
    else:
        return f"/home/datasets/pipeline/{video_id}_files"

def return_video_progress_file(video_runner_obj: Dict[str, int]) -> str:
    """
    Returns the progress file for a video.

    Parameters:
    video_runner_obj (Dict[str, int]): A dictionary that contains the information of the video.
        The keys are "video_id", "video_start_time", and "video_end_time", and their values are integers.

    Returns:
    str: The progress file for the video.
    """
    video_folder_name = return_video_folder_name(video_runner_obj)
    return f"{video_folder_name}/progress.json"

def load_progress_from_file(video_runner_obj: Dict[str, int]) -> Dict | None:
    """
    Load progress from a JSON file or start with a default progress dictionary.

    Parameters:
        video_runner_obj (Dict[str, int]): A dictionary that contains the information of the video.
            The keys are "video_id", "video_start_time", and "video_end_time", and their values are integers.

    Returns:
        dict: The loaded progress dictionary or the default progress if the file doesn't exist.
    """
    progress_file = return_video_progress_file(video_runner_obj)
    loaded_progress = DEFAULT_SAVE_PROGRESS

    try:
        if os.path.exists(progress_file):
            with open(progress_file, 'r') as progress_file_obj:
                loaded_progress = json.load(progress_file_obj)
    except Exception as e:
        print(f"Error loading progress from file: {e}")
        return None

    return loaded_progress

def save_progress_to_file(video_runner_obj: Dict[str, int], progress_data: Dict[str, int]):
    """
    Save progress data to a JSON file.

    Parameters:
        video_runner_obj (Dict[str, int]): A dictionary that contains the information of the video.
            The keys are "video_id", "video_start_time", and "video_end_time", and their values are integers.
        progress_data (Dict[str, int]): The progress data to be saved to the file.

    Returns:
        None
    """
    progress_file = return_video_progress_file(video_runner_obj)

    try:
        with open(progress_file, 'w') as progress_file_obj:
            json.dump(progress_data, progress_file_obj)
    except Exception as e:
        print(f"Error saving progress to file: {e}")


def return_video_download_location(video_runner_obj: Dict[str, int]) -> str:
    """
    Returns the download location for a video.

    Parameters:
    video_runner_obj (Dict[str, int]): A dictionary that contains the information of the video.
        The keys are "video_id", "video_start_time", and "video_end_time", and their values are integers.

    Returns:
    str: The download location for the video.
    """
    video_folder_name = return_video_folder_name(video_runner_obj)
    video_id = video_runner_obj.get("video_id")
    return f"{video_folder_name}/{video_id}.mp4"


def return_video_frames_folder(video_runner_obj: Dict[str, int]) -> str:
    """
    Returns the frames folder for a video.

    Parameters:
    video_runner_obj (Dict[str, int]): A dictionary that contains the information of the video.
        The keys are "video_id", "start_time", and "end_time", and their values are integers.

    Returns:
    str: The frames folder for the video.
    """
    video_folder_name = return_video_folder_name(video_runner_obj)
    return f"{video_folder_name}/frames"


def return_audio_file_name(video_runner_obj: Dict[str, int]) -> str:
    """
    Returns the audio file name for a video.

    Parameters:
    video_runner_obj (Dict[str, int]): A dictionary that contains the information of the video.
        The keys are "video_id", "start_time", and "end_time", and their values are integers.

    Returns:
    str: The audio file name for the video.
    """
    video_id = video_runner_obj.get("video_id")
    return f"{video_id}.flac"


def return_int_if_possible(value: Union[int, float, str]) -> Union[int, float, str]:
    """
    Returns an integer if the input is a whole number, otherwise the original input.

    Parameters:
    value (Union[int, float, str]): The value to check.

    Returns:
    Union[int, float, str]: The integer representation of the input if it is a whole number, otherwise the original input.
    """
    try:
        decimal = value % 1
        if decimal == 0:
            return int(value)
        else:
            return value
    except (TypeError, ValueError):
        return value



DEFAULT_SAVE_PROGRESS = {
    'video_id': '',
    'ImportVideo': {
        'download_video': 0,
    },
    'ExtractAudio': {
        'extract_audio': 0,
    },
    'SpeechToText': {
        'upload_blob': 0,
        'getting_speech_from_audio': 0,
        'delete_blob': 0,
    },
    'FrameExtraction': {
        'started':False,
        'frame_extraction_rate': 0,
        'extract_frames': 0,
    }
}