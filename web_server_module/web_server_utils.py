import os
from typing import Dict, List
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
    CURRENT_ENV = os.environ.get("CURRENT_ENV", "development")

    if CURRENT_ENV == "development":
        return "pipeline_data.db"  # Placeholder for development folder
    else:
        return "/home/datasets/pipeline/pipeline_data.db"
