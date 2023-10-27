

## Youtube_ID
## user_id
## AI_USER_ID

##############################################################################################################
############################################## SAMPLE DATA ##################################################
##############################################################################################################
{
    "youtube_id": {
        "data": {
            "ai_user_123": [
                {
                    "user_id": "asdasdasd",
                    "AI_USER_ID": "asdasd",
                    "ydx_server": "some_server",
                    "ydx_app_host": "some_host",
                    "status": "in_progress",
                }
            ]
        },
        "status": "in_progress",
    }
}

##############################################################################################################
 
import json
import os
from typing import Dict, List, Union
import threading

progress_lock = threading.Lock()
YTUserMapping = Dict[str, List[Dict[str, str]]]
PIPELINE_SAVE_DATA_TYPE = Dict[str, YTUserMapping]

def return_pipeline_save_file_folder() -> str:
    """
    Returns the folder name for saving pipeline progress.

    Returns:
    str: The folder path for the pipeline progress files.
    """
    CURRENT_ENV = os.environ.get("CURRENT_ENV", "production")

    if CURRENT_ENV == "development":
        return "pipeline_data.json"  # Placeholder for development folder
    else:
        return "/home/datasets/pipeline/pipeline_data.json"


def load_pipeline_progress_from_file() -> Union[PIPELINE_SAVE_DATA_TYPE, None]:
    """
    Load pipeline progress from a JSON file or start with a default progress dictionary.

    Returns:
        Union[Dict[str, List[Dict[str, str]]], None]: The loaded progress dictionary or None if the file doesn't exist.
    """
    progress_file = return_pipeline_save_file_folder()
    loaded_progress = {}

    try:
        with progress_lock:
            if os.path.exists(progress_file):
                with open(progress_file, 'r') as progress_file_obj:
                    loaded_progress = json.load(progress_file_obj)
            else:
                return loaded_progress
    except Exception as e:
        print(f"Error loading progress from file: {e}")
        return loaded_progress

    return loaded_progress


def save_pipeline_progress_to_file(progress_data: PIPELINE_SAVE_DATA_TYPE):
    """
    Save pipeline progress data to a JSON file.

    Parameters:
        progress_data (Dict[str, List[Dict[str, str]]]): The pipeline progress data to be saved to the file.

    Returns:
        None
    """
    progress_file = return_pipeline_save_file_folder()
    directory = os.path.dirname(progress_file)
    if not os.path.exists(directory):
        os.makedirs(directory)

    try:
        with progress_lock:
            with open(progress_file, 'w') as progress_file_obj:
                json.dump(progress_data, progress_file_obj)
                return
    except Exception as e:
        print(f"Error saving progress to file: {e}")
        return
