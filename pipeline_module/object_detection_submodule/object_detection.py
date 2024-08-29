import requests
import os
import csv
import json
import traceback
from typing import Dict, List, Any
from ..utils_module.utils import read_value_from_file, return_video_frames_folder, return_video_folder_name, \
    OBJECTS_CSV, save_value_to_file
from ..utils_module.timeit_decorator import timeit

DEFAULT_OBJECT_DETECTION_BATCH_SIZE = 100
IMAGE_THRESHOLD = 0.25


class ObjectDetection:
    def __init__(self, video_runner_obj: Dict[str, Any]):
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")

    def get_object_from_YOLO_batch(self, files_path: List[str], threshold: float) -> List[Dict[str, Any]]:
        token = os.getenv('ANDREW_YOLO_TOKEN')
        yolo_port = os.getenv('YOLO_PORT') or '8087'

        payload = json.dumps({
            "files_path": files_path,
            "threshold": threshold
        })

        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        url = f'http://localhost:{yolo_port}/detect_multiple_files'

        self.logger.info(f"Running object detection for {len(files_path)} files")
        self.logger.info(f"YOLO API URL: {url}")

        try:
            response = requests.post(url, headers=headers, data=payload, timeout=300)
            response.raise_for_status()
            response_data = response.json()
            return response_data['results']
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error in YOLO API request: {str(e)}")
            raise

    @timeit
    def detect_objects_batch(self, video_files_path: str, threshold: float) -> Dict[str, List[List[Any]]]:
        objects = {}
        last_processed_frame = 0

        if not read_value_from_file(video_runner_obj=self.video_runner_obj, key="['ObjectDetection']['started']"):
            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['ObjectDetection']['started']", value=True)
            step = read_value_from_file(video_runner_obj=self.video_runner_obj, key="['video_common_values']['step']")
            num_frames = read_value_from_file(video_runner_obj=self.video_runner_obj,
                                              key="['video_common_values']['num_frames']")
        else:
            last_processed_frame = read_value_from_file(video_runner_obj=self.video_runner_obj,
                                                        key="['ObjectDetection']['last_processed_frame']")
            num_frames = read_value_from_file(video_runner_obj=self.video_runner_obj,
                                              key="['video_common_values']['num_frames']")
            step = read_value_from_file(video_runner_obj=self.video_runner_obj, key="['video_common_values']['step']")

        batch_size = DEFAULT_OBJECT_DETECTION_BATCH_SIZE
        batched_frame_filenames = []

        for frame_index in range(last_processed_frame, num_frames, step):
            frame_filename = f'{video_files_path}/frame_{frame_index}.jpg'
            batched_frame_filenames.append(frame_filename)

            if len(batched_frame_filenames) == batch_size or frame_index == num_frames - 1:
                batch_response = self.get_object_from_YOLO_batch(batched_frame_filenames, threshold)
                objects = self.process_batch_response(batch_response=batch_response, objects=objects)

                self.logger.info(
                    f"Processed frames up to {frame_index}/{num_frames} ({frame_index * 100 // num_frames}% complete)")

                last_processed_frame = frame_index
                save_value_to_file(video_runner_obj=self.video_runner_obj,
                                   key="['ObjectDetection']['last_processed_frame']", value=last_processed_frame)

                batched_frame_filenames = []

        return objects

    def process_batch_response(self, batch_response: List[Dict[str, Any]], objects: Dict[str, List[List[Any]]]) -> Dict[
        str, List[List[Any]]]:
        for response in batch_response:
            frame_index = response['frame_number']
            obj_list = response['confidences']
            for entry in obj_list:
                name = entry['name']
                prob = entry['confidence']
                if name not in objects:
                    objects[name] = []
                objects[name].append([frame_index, prob, 1])
        return objects

    @timeit
    def object_detection_to_csv(self) -> bool:
        video_frames_path = return_video_frames_folder(self.video_runner_obj)
        self.logger.info(f"Running object detection for {self.video_runner_obj['video_id']}")

        outcsvpath = return_video_folder_name(self.video_runner_obj) + "/" + OBJECTS_CSV
        num_frames = read_value_from_file(video_runner_obj=self.video_runner_obj,
                                          key="['video_common_values']['num_frames']")
        step = read_value_from_file(video_runner_obj=self.video_runner_obj, key="['video_common_values']['step']")

        if not os.path.exists(outcsvpath):
            try:
                objects = self.detect_objects_batch(video_frames_path, IMAGE_THRESHOLD)
                self.logger.info(f"Writing object detection results to {outcsvpath}")

                with open(outcsvpath, 'w', newline='') as outcsvfile:
                    writer = csv.writer(outcsvfile)
                    header = ['frame_index']
                    for name in objects.keys():
                        header.extend([name, ''])
                    writer.writerow(header)

                    for frame_index in range(0, num_frames, step):
                        row = [frame_index]
                        for name, data in objects.items():
                            found = False
                            for entry in data:
                                if entry[0] == frame_index:
                                    found = True
                                    row.extend([entry[1], entry[2]])
                                    break
                            if not found:
                                row.extend(['', ''])
                        writer.writerow(row)

                return True
            except Exception as e:
                self.logger.error(f"Error in object detection: {str(e)}")
                self.logger.error(traceback.format_exc())
                return False
        else:
            self.logger.info(f"Object detection results already exist at {outcsvpath}")
            return True

    def run_object_detection(self) -> bool:
        try:
            self.logger.info(f"Running object detection on {self.video_runner_obj['video_id']}")
            if read_value_from_file(video_runner_obj=self.video_runner_obj,
                                    key="['ObjectDetection']['started']") == 'done':
                self.logger.info("Object detection already completed, skipping step.")
                return True

            if self.object_detection_to_csv():
                save_value_to_file(video_runner_obj=self.video_runner_obj, key="['ObjectDetection']['started']",
                                   value='done')
                return True
            return False
        except Exception as e:
            self.logger.error(f"Error in object detection: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False