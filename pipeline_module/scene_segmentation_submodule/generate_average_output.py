import csv
import warnings
import numpy as np
import os
from ..utils_module.utils import CAPTIONS_AND_OBJECTS_CSV, OUTPUT_AVG_CSV, return_video_folder_name, \
    return_int_if_possible
from web_server_module.web_server_database import update_status, get_status_for_youtube_id, update_module_output
from typing import Dict, Union, Optional, Any

warnings.filterwarnings("error")


def cosine_similarity(v1, v2):
    """Compute cosine similarity of v1 to v2: (v1 dot v2)/{||v1||*||v2||}"""
    try:
        return np.dot(v1, v2) / (np.linalg.norm(v1, ord=2) * np.linalg.norm(v2, ord=2))
    except RuntimeWarning:
        return "NaN"


def generate_average_output(video_runner_obj: Dict[str, Any]) -> bool:
    '''Generate output average CSV file for a video'''

    video_id = video_runner_obj["video_id"]
    logger = video_runner_obj.get("logger")

    # Check if average output generation is already completed
    if get_status_for_youtube_id(video_runner_obj["video_id"], video_runner_obj["AI_USER_ID"]) == "done":
        logger.info("Average output already generated, skipping step.")
        return True

    captions_and_objects_csv = return_video_folder_name(video_runner_obj) + '/' + CAPTIONS_AND_OBJECTS_CSV
    output_avg_csv = return_video_folder_name(video_runner_obj) + '/' + OUTPUT_AVG_CSV

    if not os.path.exists(captions_and_objects_csv):
        logger.error(f"Required input file not found: {captions_and_objects_csv}")
        return False

    jsonArray = []

    with open(captions_and_objects_csv, 'r') as csvFile:
        csvReader = csv.DictReader(csvFile)
        for row in csvReader:
            jsonArray.append(row)

    isKeyFrame, description, frame_index, timestamp, list = [], [], [], [], []

    for row in jsonArray:
        keys = list(row.keys())
        temp = []
        isKeyFrame.append(row[keys[2]])
        description.append(row[keys[3]])
        frame_index.append(return_int_if_possible(float(row[keys[0]])))
        timestamp.append(return_int_if_possible(float(row[keys[1]])))
        for idx in range(4, len(keys)):
            temp.append(float(row[keys[idx]]) if row[keys[idx]] != '' else 0.0)
        list.append(temp)

    data = []
    for idx in range(2, len(list) - 1):
        s = return_int_if_possible(cosine_similarity(list[idx], list[idx + 1]))
        a1 = return_int_if_possible(cosine_similarity(list[idx - 1], list[idx + 2])) if idx < len(list) - 3 else 0.0
        a2 = return_int_if_possible(cosine_similarity(list[idx - 2], list[idx + 3])) if idx < len(list) - 3 else 0.0
        s = "SKIP" if s == "NaN" else s
        data.append([frame_index[idx], timestamp[idx], idx, idx + 1, s, a1, a2, isKeyFrame[idx], description[idx]])

    with open(output_avg_csv, 'w') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerow(
            ['frame', 'timestamp', 'Line1', 'Line2', 'Similarity', 'avgone', 'avgtwo', 'isKeyFrame', 'description'])
        writer.writerows(data)

    if not os.path.exists(output_avg_csv):
        logger.error(f"Failed to create output file: {output_avg_csv}")
        return False

    logger.info(f"Output avg CSV file generated for video: {video_id}")

    # Save generated average output in the database
    update_module_output(video_runner_obj["video_id"], video_runner_obj["AI_USER_ID"], 'generate_average_output',
                         {"average_output": data})

    # Mark task as done in the database
    update_status(video_runner_obj["video_id"], video_runner_obj["AI_USER_ID"], "done")

    return True