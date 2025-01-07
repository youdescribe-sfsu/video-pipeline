# object_detection.py
import csv
import aiohttp
import aiofiles
import os
import json
import traceback
from typing import Dict, List, Any, Optional
import requests
import asyncio
from web_server_module.web_server_database import (
    get_status_for_youtube_id,
    update_status,
    update_module_output
)
from ..utils_module.utils import return_video_frames_folder, return_video_folder_name, OBJECTS_CSV
from ..utils_module.timeit_decorator import timeit
from datetime import datetime


class ObjectDetection:
    """Enhanced object detection with service URL injection"""

    def __init__(self, video_runner_obj: Dict[str, Any], service_url: Optional[str] = None):
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        # Use service URL from video_runner_obj if available, fallback to parameter
        self.service_url = service_url or video_runner_obj.get("yolo_url")
        if not self.service_url:
            raise ValueError("YOLO service URL must be provided")

        self.confidence_threshold = 0.25
        self.batch_size = 16

        self.logger.info(f"ObjectDetection initialized with YOLO endpoint: {self.service_url}")

    @timeit
    async def run_object_detection(self) -> bool:
        try:
            self.logger.info(f"Running object detection on {self.video_runner_obj['video_id']}")

            if get_status_for_youtube_id(
                    self.video_runner_obj["video_id"],
                    self.video_runner_obj["AI_USER_ID"]
            ) == "done":
                self.logger.info("Object detection already completed")
                return True

            frame_files = self.get_frame_files()
            results = await self.process_frames_in_batches(frame_files)
            await self.save_detection_results(results)

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
        """Get list of frame files to process"""
        frames_folder = return_video_frames_folder(self.video_runner_obj)
        return [
            os.path.join(frames_folder, f)
            for f in os.listdir(frames_folder)
            if f.endswith('.jpg')
        ]

    async def process_frames_in_batches(self, frame_files: List[str]) -> List[Dict[str, Any]]:
        """Process frames in batches with async handling"""
        self.logger.info(f"Processing {len(frame_files)} frames in batches")
        results = []

        # Process batches with asyncio.gather
        tasks = []
        for i in range(0, len(frame_files), self.batch_size):
            batch = frame_files[i:i + self.batch_size]
            tasks.append(self.process_batch(batch))

        batch_results = await asyncio.gather(*tasks)
        for result in batch_results:
            results.extend(result)

        self.logger.info(f"Processed {len(results)} frames")
        return results

    async def process_batch(self, batch: List[str]) -> List[Dict[str, Any]]:
        """Process a single batch of frames"""
        self.logger.info(f"Processing batch of {len(batch)} frames")

        payload = {
            "folder_path": os.path.dirname(batch[0]),
            "threshold": self.confidence_threshold
        }

        start_time = datetime.now()
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.service_url, json=payload) as response:
                    if response.status != 200:
                        raise requests.RequestException(
                            f"YOLO API returned status {response.status}"
                        )

                    response_data = await response.json()
                    results = response_data['results']

                    # Calculate response time for monitoring
                    elapsed = (datetime.now() - start_time).total_seconds()
                    self.logger.info(
                        f"Batch processed in {elapsed:.2f}s - "
                        f"Received {len(results)} results"
                    )

                    return self.validate_results(results)

        except Exception as e:
            self.logger.error(f"Error in YOLO API request: {str(e)}")
            raise

    def validate_results(self, results: List[Dict]) -> List[Dict]:
        """Validate and clean detection results"""
        valid_results = []
        for result in results:
            try:
                if 'frame_number' not in result or 'confidences' not in result:
                    self.logger.warning(
                        f"Invalid result structure for frame: "
                        f"{result.get('file_path', 'unknown')}"
                    )
                    continue

                valid_results.append({
                    'frame_number': result['frame_number'],
                    'timestamp': result.get('timestamp', 0.0),
                    'objects': result['confidences']
                })

            except Exception as e:
                self.logger.error(
                    f"Error processing frame {result.get('file_path', 'unknown')}: "
                    f"{str(e)}"
                )

        return valid_results

    async def save_detection_results(self, results: List[Dict[str, Any]]) -> None:
        """Save detection results to CSV with async file handling"""
        self.logger.info(f"Saving detection results for {len(results)} frames")

        output_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            OBJECTS_CSV
        )

        # Get unique object classes
        all_classes = set()
        for result in results:
            all_classes.update(obj['name'] for obj in result['objects'])

        # Write results
        async with aiofiles.open(output_file, 'w', newline='') as csvfile:
            fieldnames = ['frame_index', 'timestamp'] + list(all_classes)
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            await writer.writeheader()

            for result in results:
                row = {
                    'frame_index': result['frame_number'],
                    'timestamp': result['timestamp']
                }
                for obj in result['objects']:
                    row[obj['name']] = obj['confidence']
                await writer.writerow(row)

        self.logger.info(f"Detection results saved to {output_file}")