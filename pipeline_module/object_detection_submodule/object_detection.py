import os
import csv
import json
import traceback
from typing import Dict, List, Any, Optional
import requests
from web_server_module.web_server_database import get_status_for_youtube_id, update_status, update_module_output
from ..utils_module.utils import return_video_frames_folder, return_video_folder_name, OBJECTS_CSV
from ..utils_module.timeit_decorator import timeit


class ObjectDetection:
    def __init__(self, video_runner_obj: Dict[str, Any], service_url: Optional[str] = None):
        """Initialize ObjectDetection with video info and service URL."""
        print(f"Initializing ObjectDetection for video: {video_runner_obj['video_id']}")
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")

        # Use service URL from video_runner_obj if available, fallback to parameter
        self.yolo_endpoint = service_url or video_runner_obj.get("yolo_url")
        if not self.yolo_endpoint:
            self.yolo_endpoint = "http://localhost:8087/detect_batch_folder"  # Default to GPU 2 service

        self.confidence_threshold = 0.25
        self.batch_size = 16
        self.max_retries = 2

        print(f"ObjectDetection initialized with YOLO endpoint: {self.yolo_endpoint}")
        self.logger.info(f"ObjectDetection initialized with YOLO endpoint: {self.yolo_endpoint}")

    @timeit
    async def run_object_detection(self) -> bool:
        """Main entry point for object detection process."""
        print("Starting run_object_detection method")
        try:
            self.logger.info(f"Running object detection on {self.video_runner_obj['video_id']}")

            # Check if already processed
            if get_status_for_youtube_id(
                    self.video_runner_obj["video_id"],
                    self.video_runner_obj["AI_USER_ID"]
            ) == "done":
                self.logger.info("Object detection already completed, skipping step.")
                return True

            frame_files = self.get_frame_files()
            results = self.process_frames_in_batches(frame_files)
            self.save_detection_results(results)

            # Update database
            update_status(
                self.video_runner_obj["video_id"],
                self.video_runner_obj["AI_USER_ID"],
                "done"
            )
            update_module_output(
                self.video_runner_obj["video_id"],
                self.video_runner_obj["AI_USER_ID"],
                'object_detection',
                {"detection_results": results}
            )

            self.logger.info("Object detection completed successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error in object detection: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False

    def get_frame_files(self) -> List[str]:
        """Get list of frame files to process."""
        frames_folder = return_video_frames_folder(self.video_runner_obj)
        return sorted([
            os.path.join(frames_folder, f)
            for f in os.listdir(frames_folder)
            if f.endswith('.jpg')
        ], key=lambda x: int(os.path.basename(x).split('_')[1].split('.')[0]))

    def process_frames_in_batches(self, frame_files: List[str]) -> List[Dict[str, Any]]:
        """Process frames in batches with progress logging."""
        print(f"Processing {len(frame_files)} frames in batches")
        results = []
        total_batches = (len(frame_files) + self.batch_size - 1) // self.batch_size

        for i in range(0, len(frame_files), self.batch_size):
            batch = frame_files[i:i + self.batch_size]
            current_batch = i // self.batch_size + 1

            try:
                batch_results = self.process_batch(batch)
                results.extend(batch_results)
                self.logger.info(f"Processed batch {current_batch}/{total_batches}")
            except Exception as e:
                self.logger.error(f"Error in batch {current_batch}: {str(e)}")
                # Continue with next batch even if one fails
                continue

        print(f"Processed {len(results)} frames")
        return results

    def process_batch(self, batch: List[str], attempt: int = 1) -> List[Dict[str, Any]]:
        """Process a single batch with retry logic."""
        print(f"Processing batch of {len(batch)} frames")
        self.logger.info(f"Processing batch of {len(batch)} frames")

        payload = {
            "folder_path": os.path.dirname(batch[0]),
            "threshold": self.confidence_threshold
        }

        try:
            response = requests.post(self.yolo_endpoint, json=payload, timeout=60)
            print(f"YOLO API Response status code: {response.status_code}")
            self.logger.info(f"YOLO API Response status code: {response.status_code}")

            response.raise_for_status()
            results = response.json()['results']

            valid_results = []
            for result in results:
                try:
                    if 'frame_number' not in result or 'confidences' not in result:
                        raise ValueError(f"Invalid result structure: {result}")
                    valid_results.append({
                        'frame_number': result['frame_number'],
                        'timestamp': result.get('timestamp', 0.0),
                        'objects': result['confidences']
                    })
                except Exception as e:
                    self.logger.error(f"Error processing result: {str(e)}")

            return valid_results

        except requests.RequestException as e:
            if attempt < self.max_retries:
                self.logger.warning(f"Retry attempt {attempt} for batch")
                return self.process_batch(batch, attempt + 1)
            self.logger.error(f"Error in YOLO API request: {str(e)}")
            raise

    def save_detection_results(self, results: List[Dict[str, Any]]) -> None:
        """Save detection results in the expected CSV format."""
        print(f"Saving detection results for {len(results)} frames")
        output_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            OBJECTS_CSV
        )
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