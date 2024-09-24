import requests
import os
import csv
from typing import Dict, Any
from web_server_module.web_server_database import update_status, get_status_for_youtube_id, update_module_output
from ..utils_module.utils import return_video_frames_folder, return_video_folder_name, OBJECTS_CSV
from ..utils_module.timeit_decorator import timeit

DEFAULT_OBJECT_DETECTION_BATCH_SIZE = 100
IMAGE_THRESHOLD = 0.25


def get_object_from_YOLO_batch(files_path, threshold, logger=None):
    """
    Sends batch request to YOLO API for object detection.

    Parameters:
    files_path (str): The path to the folder containing video frames.
    threshold (float): The confidence threshold for object detection.
    logger (Logger): Optional logger for recording actions.

    Returns:
    List[Dict]: The results of the object detection for each frame.
    """
    token = os.getenv('ANDREW_YOLO_TOKEN')
    yolo_port = os.getenv('YOLO_PORT') or '8087'

    payload = {
        "files_path": files_path,
        "threshold": threshold
    }

    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    url = f'http://localhost:{yolo_port}/detect_multiple_files'
    print("YOLO API URL: ", url)

    if logger:
        logger.info(f"Running object detection on URL {url}")

    try:
        response = requests.post(url, headers=headers, json=payload)

        if response.status_code != 200:
            print(f"Server returned status {response.status_code}.")
            if logger:
                logger.info(f"Server returned status {response.status_code}")
            return []

        response_data = response.json()
        return response_data['results']

    except requests.RequestException as e:
        print(f"Request error: {e}")
        raise Exception(f"Request error: {e}")


@timeit
def object_detection_to_csv(video_runner_obj: Dict[str, Any]) -> bool:
    """
    Performs object detection on video frames, writes detection results to a CSV file, and saves progress and output to the database.

    Parameters:
    video_runner_obj (Dict): A dictionary containing video processing details.

    Returns:
    bool: True if object detection and saving results were successful, False otherwise.
    """
    print(f"Starting object_detection_to_csv for video: {video_runner_obj['video_id']}")
    video_frames_path = return_video_frames_folder(video_runner_obj)
    video_runner_obj["logger"].info(f"Running object detection for {video_runner_obj['video_id']}")

    outcsvpath = return_video_folder_name(video_runner_obj) + "/" + OBJECTS_CSV

    if os.path.exists(outcsvpath):
        print(f"Object detection CSV already exists: {outcsvpath}")
        return True

    try:
        print("Detecting objects and writing to CSV")
        objects = detect_objects_batch(video_frames_path, IMAGE_THRESHOLD, video_runner_obj=video_runner_obj,
                                       logging=True, logger=video_runner_obj["logger"])

        print(f"Object detection completed. Writing results to {outcsvpath}")
        video_runner_obj["logger"].info(f"Writing object detection results to {outcsvpath}")

        with open(outcsvpath, 'w', newline='') as outcsvfile:
            writer = csv.writer(outcsvfile)
            header = ['frame_index'] + list(objects.keys())
            writer.writerow(header)

            for frame_index in range(len(objects)):
                row = [frame_index]
                for obj, data in objects.items():
                    row.extend([data.get('confidence', ''), data.get('count', '')])
                writer.writerow(row)
                outcsvfile.flush()

        print(f"Object detection results written to {outcsvpath}")

        # Save object detection results in the database for future use
        update_module_output(video_runner_obj["video_id"], video_runner_obj["AI_USER_ID"], 'object_detection_helper',
                             {"objects": objects})

        # Update progress to "done" in the database
        update_status(video_runner_obj["video_id"], video_runner_obj["AI_USER_ID"], "done")

        return True
    except Exception as e:
        print(f"Error in object_detection_to_csv: {str(e)}")
        video_runner_obj["logger"].error(f"Error in object_detection_to_csv: {str(e)}")
        return False


def detect_objects_batch(video_frames_path: str, threshold: float, video_runner_obj: Dict[str, Any],
                         logging: bool = False, logger=None) -> Dict:
    """
    Detect objects in a batch of video frames using the YOLO model.

    Parameters:
    video_frames_path (str): The folder path where the video frames are stored.
    threshold (float): The confidence threshold for object detection.
    video_runner_obj (Dict): A dictionary containing video processing details.
    logging (bool): Whether to log progress information.
    logger (Logger): Optional logger for recording actions.

    Returns:
    Dict: A dictionary where keys are object names and values are detection results (confidence and count).
    """
    objects_detected = {}

    frame_files = [os.path.join(video_frames_path, f) for f in os.listdir(video_frames_path) if f.endswith('.jpg')]

    for i in range(0, len(frame_files), DEFAULT_OBJECT_DETECTION_BATCH_SIZE):
        batch = frame_files[i:i + DEFAULT_OBJECT_DETECTION_BATCH_SIZE]

        logger.info(f"Processing object detection batch {i // DEFAULT_OBJECT_DETECTION_BATCH_SIZE + 1}")
        detection_results = get_object_from_YOLO_batch(video_frames_path, threshold, logger)

        # Combine detection results from batches
        for result in detection_results:
            frame_index = result['frame_index']
            for obj in result['objects']:
                if obj['name'] not in objects_detected:
                    objects_detected[obj['name']] = {'confidence': obj['confidence'], 'count': 1}
                else:
                    objects_detected[obj['name']]['confidence'] = max(objects_detected[obj['name']]['confidence'],
                                                                      obj['confidence'])
                    objects_detected[obj['name']]['count'] += 1

    return objects_detected