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

