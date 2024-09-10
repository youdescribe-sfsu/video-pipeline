import os
import csv
import json
import traceback
from typing import Dict, List, Any, Optional
import requests
from ..utils_module.utils import read_value_from_file, return_video_frames_folder, return_video_folder_name, OBJECTS_CSV, save_value_to_file
from ..utils_module.timeit_decorator import timeit
from concurrent.futures import ThreadPoolExecutor, as_completed

class ObjectDetection:
    def __init__(self, video_runner_obj: Dict[str, Any]):
        print(f"Initializing ObjectDetection for video: {video_runner_obj['video_id']}")
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        self.yolo_version = "v8"
        self.confidence_threshold = 0.25
        self.yolo_endpoint = os.getenv('YOLO_ENDPOINT', 'http://localhost:8080/detect_multiple_files')
        self.batch_size = 16
        print(f"ObjectDetection initialized with YOLO endpoint: {self.yolo_endpoint}")

    @timeit
    def run_object_detection(self) -> bool:
        print("Starting run_object_detection method")
        try:
            self.logger.info(f"Running object detection on {self.video_runner_obj['video_id']}")
            if read_value_from_file(video_runner_obj=self.video_runner_obj, key="['ObjectDetection']['started']") == 'done':
                self.logger.info("Object detection already completed, skipping step.")
                return True

            frame_files = self.get_frame_files()
            results = self.process_frames_in_batches(frame_files)
            self.save_detection_results(results)

            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['ObjectDetection']['started']", value='done')
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
            batch = frame_files[i:i+self.batch_size]
            batch_results = self.process_batch(batch)
            results.extend(batch_results)
            self.logger.info(f"Processed batch {i//self.batch_size + 1}/{len(frame_files)//self.batch_size + 1}")
        print(f"Processed {len(results)} frames")
        return results

    def process_batch(self, batch: List[str]) -> List[Dict[str, Any]]:
        print(f"Processing batch of {len(batch)} frames")
        payload = {
            "files": batch,
            "confidence": self.confidence_threshold,
            "yolo_version": self.yolo_version
        }
        try:
            response = requests.post(self.yolo_endpoint, json=payload)
            response.raise_for_status()
            print(f"Batch processing result: {len(response.json()['results'])} frames processed")
            return response.json()['results']
        except requests.RequestException as e:
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


    def get_object_counts(self, results: List[Dict[str, Any]]) -> Dict[str, int]:
        object_counts = {}
        for result in results:
            for obj in result['objects']:
                object_counts[obj['name']] = object_counts.get(obj['name'], 0) + 1
        return object_counts

    def get_top_objects(self, results: List[Dict[str, Any]], top_n: int = 5) -> List[str]:
        object_counts = self.get_object_counts(results)
        return sorted(object_counts, key=object_counts.get, reverse=True)[:top_n]

    @timeit
    def analyze_object_distribution(self) -> Dict[str, Any]:
        results_file = os.path.join(return_video_folder_name(self.video_runner_obj), OBJECTS_CSV)
        if not os.path.exists(results_file):
            self.logger.error("Object detection results file not found")
            return {}

        with open(results_file, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            results = list(reader)

        object_counts = {}
        frame_count = len(results)

        for row in results:
            for obj, confidence in row.items():
                if obj not in ['frame_index', 'timestamp'] and confidence:
                    object_counts[obj] = object_counts.get(obj, 0) + 1

        object_distribution = {obj: count / frame_count for obj, count in object_counts.items()}
        top_objects = sorted(object_distribution, key=object_distribution.get, reverse=True)[:5]

        return {
            "total_frames": frame_count,
            "unique_objects": len(object_counts),
            "top_objects": top_objects,
            "object_distribution": object_distribution
        }

if __name__ == "__main__":
    # For testing purposes
    video_runner_obj = {
        "video_id": "test_video",
        "logger": print  # Use print as a simple logger for testing
    }
    object_detector = ObjectDetection(video_runner_obj)
    success = object_detector.run_object_detection()
    print(f"Object detection {'succeeded' if success else 'failed'}")

    if success:
        analysis = object_detector.analyze_object_distribution()
        print("Object Distribution Analysis:")
        print(json.dumps(analysis, indent=2))