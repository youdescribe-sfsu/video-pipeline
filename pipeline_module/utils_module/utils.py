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
PROD_ARTIFACTS_ROOT_FOLDER = '/home/datasets/aiAudioDescriptionDataset-prod/'
DEV_ARTIFACTS_ROOT_FOLDER = '/home/datasets/aiAudioDescriptionDataset-dev/'

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
from typing import Dict, Any, Union
# Define a lock for thread safety
import threading
progress_lock = threading.Lock()

class PipelineTask(Enum):
    IMPORT_VIDEO = "import_video"
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
from typing import Dict, Union


def return_artifacts_root_folder(current_env):
    if current_env == "development":
        return DEV_ARTIFACTS_ROOT_FOLDER
    else:
        return PROD_ARTIFACTS_ROOT_FOLDER


def return_video_folder_name(video_runner_obj: Dict[str, Union[int, str]]) -> str:
    """
    Returns the folder name for a video.

    Parameters:
    video_runner_obj (Dict[str, int]): A dictionary that contains the information of the video.
        The keys are "video_id", "video_start_time", "video_end_time", and "AI_USER_ID and their values.

    Returns:
    str: The folder name for the video.
    """
    video_id = video_runner_obj.get("video_id")
    video_start_time = video_runner_obj.get("video_start_time", None)
    video_end_time = video_runner_obj.get("video_end_time", None)

    CURRENT_ENV = os.environ.get("CURRENT_ENV", "production")
    AI_USER_ID = video_runner_obj.get("AI_USER_ID", "650506db3ff1c2140ea10ece")
    return_string = ""

    if video_start_time is not None and video_end_time is not None:
        return_string = f"{return_artifacts_root_folder(CURRENT_ENV)}{video_id}_files/part_start_{video_start_time}_end_{video_end_time}"
    else:
        return_string = f"{return_artifacts_root_folder(CURRENT_ENV)}{video_id}_files"

    if AI_USER_ID is not None:
        return_string = f"{return_string}_{AI_USER_ID}"

    return return_string


def return_video_progress_file(video_runner_obj: Dict[str, Union[int, str]]) -> str:
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


def load_progress_from_file(video_runner_obj: Dict[str, Union[int, str]]) -> Dict:
    progress_file = return_video_progress_file(video_runner_obj)

    try:
        with progress_lock:
            if os.path.exists(progress_file):
                with open(progress_file, 'r') as progress_file_obj:
                    loaded_progress = json.load(progress_file_obj)
                    # Merge with default values
                    merged_progress = DEFAULT_SAVE_PROGRESS.copy()
                    merged_progress.update(loaded_progress)
                    return merged_progress
            else:
                new_progress = DEFAULT_SAVE_PROGRESS.copy()
                new_progress['video_id'] = video_runner_obj.get('video_id', '')
                return new_progress
    except Exception as e:
        print(f"Error loading progress from file: {e}")
        new_progress = DEFAULT_SAVE_PROGRESS.copy()
        new_progress['video_id'] = video_runner_obj.get('video_id', '')
        return new_progress


def read_value_from_file(video_runner_obj: Dict[str, Union[int, str]], key: str) -> Any:
    json_file = load_progress_from_file(video_runner_obj)
    keys = key.strip("[]").replace("']['", ".").split(".")

    current = json_file
    for k in keys:
        if isinstance(current, dict) and k in current:
            current = current[k]
        else:
            return None

    return current


def save_progress_to_file(video_runner_obj: Dict[str, Union[int, str]], progress_data: Dict[str, Union[int, str]]):
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
    directory = os.path.dirname(progress_file)
    if not os.path.exists(directory):
        os.makedirs(directory)

    try:
        with progress_lock:
            print(f"Saving progress to file: {progress_file}")
            with open(progress_file, 'w') as progress_file_obj:
                json.dump(progress_data, progress_file_obj)
    except Exception as e:
        print(f"Error saving progress to file: {e}")


def save_value_to_file(video_runner_obj: Dict[str, Union[int, str]], key: str, value: Any) -> None:
    """
    Save a new value associated with a specific key to the progress data stored in a JSON file for the given video runner object.

    This function handles nested keys in the format "['key1']['key2']['key3']".

    Parameters:
        video_runner_obj (Dict[str, Union[int, str]]): A dictionary containing the information of the video runner.
        key (str): The key under which the new value will be stored in the progress data.
        value (Any): The value to be associated with the provided key in the progress data.

    Returns:
        None
    """
    try:
        json_file = load_progress_from_file(video_runner_obj)  # Load existing progress data

        # Parse the key string
        keys = [k.strip("'[]") for k in key.split("][")]

        # Navigate through the nested dictionaries
        current = json_file
        for k in keys[:-1]:
            if k not in current:
                current[k] = {}
            current = current[k]

        # Set the value at the final key
        current[keys[-1]] = value

        save_progress_to_file(video_runner_obj, json_file)  # Save the modified progress data
        print(f"Successfully saved value for key: {key}")
    except Exception as e:
        print(f"Error saving value to file: {e}")
    return

def return_video_download_location(video_runner_obj: Dict[str, Union[int, str]]) -> str:
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


def return_video_frames_folder(video_runner_obj: Dict[str, Union[int, str]]) -> str:
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


def return_audio_file_name(video_runner_obj: Dict[str, Union[int, str]]) -> str:
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
    'video_common_values': {
        'step': None,
        'num_frames': None,
        'frames_per_second': None,
    },
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
        'started': False,
        'frame_extraction_rate': 0,
        'extract_frames': 0,
        'num_frames': 0,
    },
    'OCR': {
        'started': False,
        'detect_watermark': 0,
        'get_all_ocr': 0,
        'filter_ocr': 0,
        'filter_ocr_agreement': 0,
        'filter_ocr_remove_similarity': 0,
    },
    'ObjectDetection': {
        'started': False,
        'step': 0,
        'num_frames': 0,
    },
    'KeyframeSelection': {
        'started': False,
    },
    'ImageCaptioning': {
        'started': False,
        'run_image_captioning': {
            'started': False,
            'last_processed_frame': 0,
        },
        'combine_image_caption': 0,
    },
    'CaptionRating': {
        'started': False,
        'last_processed_frame': 0,
        'get_all_caption_rating': 0,
        'filter_captions': 0,
    },
    'SceneSegmentation': {
        'started': False,
        'generate_average_output': 0,
        'run_scene_segmentation': 0,
    },
    'TextSummarization': {
        'started': False,
    },
    'UploadToYDX': {
        'started': False,
        'generateYDXCaption': 0,
    },
}