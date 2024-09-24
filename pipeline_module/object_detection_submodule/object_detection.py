import os
import csv
import json
import traceback
from typing import Dict, List, Any, Optional
import requests
from web_server_module.web_server_database import get_status_for_youtube_id, update_status, update_module_output
from ..utils_module.utils import return_video_frames_folder, return_video_folder_name, OBJECTS_CSV
from ..utils_module.timeit_decorator import timeit
from concurrent.futures import ThreadPoolExecutor, as_completed


class ObjectDetection:
    def __init__(self, video_runner_obj: Dict[str, Any]):
        print(f"Initializing ObjectDetection for video: {video_runner_obj['video_id']}")
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        self.yolo_version = "v8"
        self.confidence_threshold = 0.25
        self.yolo_endpoint = os.getenv('YOLO_ENDPOINT', 'http://localhost:8080/detect_batch_folder')
        self.batch_size = 16
        print(f"ObjectDetection initialized with YOLO endpoint: {self.yolo_endpoint}")
        self.logger.info(f"ObjectDetection initialized with YOLO endpoint: {self.yolo_endpoint}")

    @timeit
    def run_object_detection(self) -> bool:
        print("Starting run_object_detection method")
        try:
            self.logger.info(f"Running object detection on {self.video_runner_obj['video_id']}")

            # Use the database to check if object detection has already been completed
            if get_status_for_youtube_id(self.video_runner_obj["video_id"],
                                         self.video_runner_obj["AI_USER_ID"]) == "done":
                self.logger.info("Object detection already completed, skipping step.")
                return True

            frame_files = self.get_frame_files()
            results = self.process_frames_in_batches(frame_files)
            self.save_detection_results(results)

            # Update progress in the database
            update_status(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"], "done")

            # Save object detection results to the database for future use
            update_module_output(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"],
                                 'object_detection', {"detection_results": results})

            self.logger.info("Object detection completed successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error in object detection: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False

    def get_frame_files(self) -> List[str]:
        frames_folder = return_video_frames_folder(self.video_runner_obj)
        return [os.path.join(frames_folder, f) for f in os.listdir(frames_folder) if f.endswith('.jpg')]

    def process_frames_in_batches(self, frame_files: List[str]) -> List[Dict[str, Any]]:
        print(f"Processing {len(frame_files)} frames in batches")
        results = []
        for i in range(0, len(frame_files), self.batch_size):
            batch = frame_files[i:i + self.batch_size]
            batch_results = self.process_batch(batch)
            results.extend(batch_results)
            self.logger.info(f"Processed batch {i // self.batch_size + 1}/{len(frame_files) // self.batch_size + 1}")
        print(f"Processed {len(results)} frames")
        return results

    def process_batch(self, batch: List[str]) -> List[Dict[str, Any]]:
        print(f"Processing batch of {len(batch)} frames")
        self.logger.info(f"Processing batch of {len(batch)} frames")
        payload = {
            "folder_path": os.path.dirname(batch[0]),  # Assuming all files are in the same directory
            "threshold": self.confidence_threshold
        }
        print(f"Sending payload to YOLO endpoint: {json.dumps(payload, indent=2)}")
        self.logger.info(f"Sending payload to YOLO endpoint: {json.dumps(payload, indent=2)}")
        try:
            response = requests.post(self.yolo_endpoint, json=payload)
            print(f"YOLO API Response status code: {response.status_code}")
            self.logger.info(f"YOLO API Response status code: {response.status_code}")
            response.raise_for_status()
            results = response.json()['results']
            print(f"Received results for {len(results)} frames")
            self.logger.info(f"Received results for {len(results)} frames")

            valid_results = []
            for result in results:
                try:
                    if 'frame_number' not in result or 'confidences' not in result:
                        raise ValueError(f"Invalid result structure for frame: {result.get('file_path', 'unknown')}")
                    valid_results.append({
                        'frame_number': result['frame_number'],
                        'timestamp': result.get('timestamp', 0.0),  # Default to 0.0 if not present
                        'objects': result['confidences']
                    })
                except Exception as e:
                    print(f"Error processing frame {result.get('file_path', 'unknown')}: {str(e)}")
                    self.logger.error(f"Error processing frame {result.get('file_path', 'unknown')}: {str(e)}")

            return valid_results
        except requests.RequestException as e:
            print(f"Error in YOLO API request: {str(e)}")
            self.logger.error(f"Error in YOLO API request: {str(e)}")
            raise

    def save_detection_results(self, results: List[Dict[str, Any]]) -> None:
        print(f"Saving detection results for {len(results)} frames")
        output_file = os.path.join(return_video_folder_name(self.video_runner_obj), OBJECTS_CSV)
        self.logger.info(f"Saving object detection results to {output_file}")

        all_classes = set()
        for result in results:
            all_classes.update(obj['name'] for obj in result['objects'])

        with open(output_file, 'w', newline='') as csvfile:
            fieldnames = ['frame_index', 'timestamp'] + list(all_classes)
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for result in results:
                row = {
                    'frame_index': result['frame_number'],
                    'timestamp': result['timestamp']
                }
                for obj in result['objects']:
                    row[obj['name']] = obj['confidence']
                writer.writerow(row)
        print(f"Detection results saved to {output_file}")