import os
import csv
import re
import time
import traceback
from typing import Dict, List, Any
import requests
from web_server_module.web_server_database import get_status_for_youtube_id, update_status, update_module_output
from ..utils_module.utils import return_video_frames_folder, return_video_folder_name, OBJECTS_CSV
from ..utils_module.timeit_decorator import timeit


class ObjectDetection:
    def __init__(self, video_runner_obj: Dict[str, Any], service_url: str):
        """Initialize ObjectDetection with video info and service URL."""
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        self.yolo_endpoint = service_url  # Direct service URL injection
        self.confidence_threshold = 0.25
        self.max_retries = 2
        self.batch_size = 16
        self.request_timeout = 120

        print(f"ObjectDetection initialized with YOLO endpoint: {self.yolo_endpoint}")
        self.logger.info(f"ObjectDetection initialized with YOLO endpoint: {self.yolo_endpoint}")

    @timeit
    def run_object_detection(self) -> bool:
        """Execute object detection on video frames."""
        try:
            # Check if already processed
            if get_status_for_youtube_id(
                    self.video_runner_obj["video_id"],
                    self.video_runner_obj["AI_USER_ID"]
            ) == "done":
                self.logger.info("Object detection already completed")
                return True

            # Get frame files
            frame_files = self.get_frame_files()
            total_frames = len(frame_files)
            self.logger.info(f"Found {total_frames} frames to process")

            if total_frames == 0:
                self.logger.error("No frames found for processing")
                return False

            # Calculate optimal batch size
            self.batch_size = self.calculate_optimal_batch_size(total_frames)
            self.logger.info(f"Using batch size of {self.batch_size} for {total_frames} frames")

            # Process frames in batches
            all_results = []
            processed_frames = 0

            for i in range(0, total_frames, self.batch_size):
                batch = frame_files[i:i + self.batch_size]
                batch_num = (i // self.batch_size) + 1
                total_batches = (total_frames + self.batch_size - 1) // self.batch_size

                self.logger.info(f"Processing batch {batch_num}/{total_batches}")

                try:
                    batch_results = self.process_batch(batch)
                    all_results.extend(batch_results)

                    # Update processed count for progress tracking
                    processed_frames += len(batch_results)
                    progress = (processed_frames / total_frames) * 100
                    self.logger.info(f"Progress: {progress:.1f}% ({processed_frames}/{total_frames})")

                except Exception as e:
                    self.logger.error(f"Error in batch {batch_num}: {str(e)}")
                    # Continue with next batch even on failure

            # Verify we have sufficient results
            coverage = len(all_results) / total_frames if total_frames > 0 else 0
            self.logger.info(f"Completed with {coverage:.1%} frame coverage ({len(all_results)}/{total_frames})")

            if coverage < 0.75:  # Require at least 75% coverage
                self.logger.error(f"Insufficient frame coverage: {coverage:.1%}")
                if coverage == 0:
                    return False

            # Save detection results and update database
            self.save_detection_results(all_results)
            update_status(
                self.video_runner_obj["video_id"],
                self.video_runner_obj["AI_USER_ID"],
                "done"
            )

            self.logger.info("Object detection completed successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error in object detection: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False

    def verify_detection_results(self, results: List[Dict[str, Any]], expected_frames: int) -> bool:
        """
        Verify that we have detection results for at least 90% of expected frames.
        """
        if not results:
            return False

        # Count unique frame numbers in results
        processed_frames = set(result.get('frame_number') for result in results if 'frame_number' in result)

        # Calculate percentage of frames processed
        coverage = len(processed_frames) / expected_frames if expected_frames > 0 else 0

        self.logger.info(
            f"Object detection coverage: {coverage:.2f} ({len(processed_frames)}/{expected_frames} frames)")

        # Accept if we have at least 90% coverage
        return coverage >= 0.9

    def calculate_optimal_batch_size(self, total_frames: int) -> int:
        """
        Dynamically calculate optimal batch size based on frame count.
        Smaller batches for larger videos to prevent timeouts.
        """
        if total_frames < 500:
            return 32
        elif total_frames < 1000:
            return 16
        elif total_frames < 2000:
            return 8
        else:
            return 4

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
        """Process a specific batch of frame files."""
        num_frames = len(batch)
        self.logger.info(f"Processing batch of {num_frames} frames")

        # Calculate adaptive timeout based on batch size
        # Allow ~5 seconds per frame with minimum 60 seconds
        adaptive_timeout = max(60, num_frames * 5)

        # Create payload with specific files rather than folder
        payload = {
            "files_path": batch,  # Send exact file paths
            "threshold": self.confidence_threshold
        }

        try:
            self.logger.info(f"Sending request with {adaptive_timeout}s timeout")
            response = requests.post(
                self.yolo_endpoint,
                json=payload,
                timeout=adaptive_timeout
            )

            self.logger.info(f"YOLO API Response status code: {response.status_code}")
            response.raise_for_status()

            results = response.json().get('results', [])

            # Validate results
            if len(results) != num_frames:
                self.logger.warning(
                    f"Expected {num_frames} results but got {len(results)}. "
                    f"Results may be incomplete."
                )

            # Process and return valid results
            valid_results = []
            for result in results:
                try:
                    file_path = result.get('file_path')
                    if not file_path or 'confidences' not in result:
                        continue

                    # Extract frame number from filename
                    frame_match = re.search(r'frame_(\d+)\.jpg', file_path)
                    frame_number = int(frame_match.group(1)) if frame_match else 0

                    valid_results.append({
                        'frame_number': frame_number,
                        'timestamp': result.get('timestamp', 0.0),
                        'objects': result.get('confidences', [])
                    })
                except Exception as e:
                    self.logger.error(f"Error processing result: {str(e)}")

            return valid_results

        except requests.RequestException as e:
            if attempt < self.max_retries:
                # Exponential backoff between retries
                wait_time = 2 ** attempt
                self.logger.warning(f"Request failed. Retry {attempt} after {wait_time}s")
                time.sleep(wait_time)
                return self.process_batch(batch, attempt + 1)

            self.logger.error(f"Error in YOLO API request after {attempt} attempts: {str(e)}")
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