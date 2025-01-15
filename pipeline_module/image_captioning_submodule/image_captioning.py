import csv
import os
import json
import requests
import traceback
from typing import Dict, Any, Optional, List, Tuple
from ..utils_module.timeit_decorator import timeit
from web_server_module.web_server_database import (
    update_status,
    update_module_output,
    get_module_output
)
from ..utils_module.utils import (
    CAPTIONS_CSV,
    FRAME_INDEX_SELECTOR,
    IS_KEYFRAME_SELECTOR,
    KEY_FRAME_HEADERS,
    KEYFRAME_CAPTION_SELECTOR,
    TIMESTAMP_SELECTOR,
    return_video_folder_name,
    return_video_frames_folder,
    CAPTION_IMAGE_PAIR,
    KEYFRAMES_CSV
)


class ImageCaptioning:
    """
    Handles image captioning process with synchronous processing as per professor's requirements.
    Each service instance handles one request at a time.
    """

    def __init__(self, video_runner_obj: Dict[str, Any], service_url: Optional[str] = None):
        """Initialize the image captioning handler with necessary configurations."""
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        # Use service URL from service manager or video runner object
        self.service_url = service_url or video_runner_obj.get("caption_url")
        if not self.service_url:
            raise ValueError("Captioning service URL must be provided")

        # Service configuration
        self.token = 'VVcVcuNLTwBAaxsb2FRYTYsTnfgLdxKmdDDxMQLvh7rac959eb96BCmmCrAY7Hc3'
        self.min_length = 25
        self.max_length = 50
        self.max_retries = 2
        self.request_timeout = 10

    def get_caption(self, filename: str) -> str:
        """Get caption for single image using synchronous request with retry mechanism."""
        for attempt in range(self.max_retries):
            try:
                # Ensure file exists before attempting to process
                if not os.path.exists(filename):
                    self.logger.error(f"Image file not found: {filename}")
                    return ""

                with open(filename, 'rb') as fileBuffer:
                    multipart_form_data = {
                        'token': ('', self.token),
                        'image': (os.path.basename(filename), fileBuffer),
                        'min_length': ('', str(self.min_length)),
                        'max_length': ('', str(self.max_length))
                    }

                    self.logger.info(f"Sending request to {self.service_url}")
                    response = requests.post(
                        self.service_url,
                        files=multipart_form_data,
                        timeout=self.request_timeout
                    )

                    if response.status_code == 200:
                        caption = response.json()['caption']
                        self.logger.info(f"Got caption: {caption}")
                        return caption.strip()

                    self.logger.error(f"Caption service returned status {response.status_code}")
                    if attempt < self.max_retries - 1:
                        continue

            except requests.Timeout:
                self.logger.error(f"Request timed out for file {filename}")
                if attempt < self.max_retries - 1:
                    continue
            except Exception as e:
                self.logger.error(f"Error getting caption: {str(e)}")
                if attempt < self.max_retries - 1:
                    continue

        return ""

    def get_frame_files(self) -> List[str]:
        """Get sorted list of frame files to process."""
        frames_folder = return_video_frames_folder(self.video_runner_obj)
        frame_files = []

        try:
            # Get all jpg files and sort them by frame number
            frame_files = sorted(
                [f for f in os.listdir(frames_folder) if f.endswith('.jpg')],
                key=lambda x: int(x.split('_')[1].split('.')[0])
            )
        except Exception as e:
            self.logger.error(f"Error getting frame files: {str(e)}")
            return []

        return frame_files

    def get_frame_rate(self) -> float:
        """Get frame rate from previous module output."""
        try:
            module_data = get_module_output(
                self.video_runner_obj["video_id"],
                self.video_runner_obj["AI_USER_ID"],
                'frame_extraction'
            )
            if module_data and 'adaptive_fps' in module_data:
                return float(module_data['adaptive_fps'])
        except Exception as e:
            self.logger.error(f"Error getting frame rate: {str(e)}")

        return 30.0  # Default to 30fps if not found

    def load_keyframes(self) -> List[int]:
        """Load keyframe information from keyframes.csv."""
        keyframes = []
        try:
            keyframes_file = os.path.join(
                return_video_folder_name(self.video_runner_obj),
                KEYFRAMES_CSV
            )
            if os.path.exists(keyframes_file):
                with open(keyframes_file, 'r', newline='') as f:
                    reader = csv.reader(f)
                    next(reader)  # Skip header
                    keyframes = [int(row[0]) for row in reader]
        except Exception as e:
            self.logger.error(f"Error loading keyframes: {str(e)}")

        return keyframes

    def process_frames(self) -> List[Dict[str, Any]]:
        """Process frames sequentially to generate captions."""
        try:
            frames_folder = return_video_frames_folder(self.video_runner_obj)
            frame_files = self.get_frame_files()
            keyframes = self.load_keyframes()
            fps = self.get_frame_rate()

            results = []
            for frame_file in frame_files:
                frame_index = int(frame_file.split('_')[1].split('.')[0])

                # Only process keyframes if available
                if keyframes and frame_index not in keyframes:
                    continue

                filename = os.path.join(frames_folder, frame_file)
                caption = self.get_caption(filename)

                if caption:
                    results.append({
                        'frame_index': frame_index,
                        'timestamp': frame_index / fps,
                        'caption': caption,
                        'frame_url': filename
                    })

                    # Update progress in database
                    update_module_output(
                        self.video_runner_obj["video_id"],
                        self.video_runner_obj["AI_USER_ID"],
                        'image_captioning',
                        {"last_processed_frame": frame_index}
                    )

            return results

        except Exception as e:
            self.logger.error(f"Error processing frames: {str(e)}")
            return []

    def save_captions_csv(self, results: List[Dict[str, Any]]) -> bool:
        """Save caption results to captions.csv."""
        try:
            output_file = os.path.join(
                return_video_folder_name(self.video_runner_obj),
                CAPTIONS_CSV
            )

            with open(output_file, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=[
                    KEY_FRAME_HEADERS[FRAME_INDEX_SELECTOR],
                    KEY_FRAME_HEADERS[TIMESTAMP_SELECTOR],
                    KEY_FRAME_HEADERS[IS_KEYFRAME_SELECTOR],
                    KEY_FRAME_HEADERS[KEYFRAME_CAPTION_SELECTOR]
                ])
                writer.writeheader()

                for result in results:
                    writer.writerow({
                        KEY_FRAME_HEADERS[FRAME_INDEX_SELECTOR]: result['frame_index'],
                        KEY_FRAME_HEADERS[TIMESTAMP_SELECTOR]: result['timestamp'],
                        KEY_FRAME_HEADERS[IS_KEYFRAME_SELECTOR]: 'True',
                        KEY_FRAME_HEADERS[KEYFRAME_CAPTION_SELECTOR]: result['caption']
                    })

            return True

        except Exception as e:
            self.logger.error(f"Error saving captions CSV: {str(e)}")
            return False

    def combine_image_caption(self) -> bool:
        """Generate caption_image_pair.csv for the rating process."""
        try:
            video_frames_path = return_video_frames_folder(self.video_runner_obj)
            captions_path = os.path.join(
                return_video_folder_name(self.video_runner_obj),
                CAPTIONS_CSV
            )

            if not os.path.exists(captions_path):
                self.logger.error(f"Captions file not found: {captions_path}")
                return False

            pairs = []
            with open(captions_path, 'r', newline='') as captcsvfile:
                reader = csv.DictReader(captcsvfile)
                for row in reader:
                    frame_index = row[KEY_FRAME_HEADERS[FRAME_INDEX_SELECTOR]]
                    # Only add pairs where both caption and frame exist
                    frame_path = f'{video_frames_path}/frame_{frame_index}.jpg'
                    if os.path.exists(frame_path):
                        pairs.append({
                            "frame_index": frame_index,
                            "frame_url": frame_path,
                            "caption": row[KEY_FRAME_HEADERS[KEYFRAME_CAPTION_SELECTOR]]
                        })

            # Write the pairs to output file
            output_file = os.path.join(
                return_video_folder_name(self.video_runner_obj),
                CAPTION_IMAGE_PAIR
            )

            with open(output_file, 'w', newline='') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=["frame_index", "frame_url", "caption"])
                writer.writeheader()
                writer.writerows(pairs)

            return True

        except Exception as e:
            self.logger.error(f"Error combining image captions: {str(e)}")
            return False

    @timeit
    def run_image_captioning(self) -> bool:
        """Main entry point for image captioning process."""
        try:
            # Check if already processed
            module_status = get_module_output(
                self.video_runner_obj["video_id"],
                self.video_runner_obj["AI_USER_ID"],
                'image_captioning'
            )
            if module_status and module_status.get("status") == "completed":
                self.logger.info("Image captioning already processed")
                return True

            # Process frames and generate captions
            results = self.process_frames()
            if not results:
                raise Exception("Frame processing failed")

            # Save results and generate required files
            success = self.save_captions_csv(results)
            if not success:
                raise Exception("Failed to save captions")

            # Generate caption-image pairs file
            success = self.combine_image_caption()
            if not success:
                raise Exception("Failed to combine captions with images")

            # Mark process as complete
            update_module_output(
                self.video_runner_obj["video_id"],
                self.video_runner_obj["AI_USER_ID"],
                'image_captioning',
                {"status": "completed", "captions": results}
            )

            self.logger.info("Image captioning completed successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error in image captioning: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False

    def filter_keyframes_from_caption(self) -> bool:
        """Optional method to filter captions based on keyframe selection."""
        try:
            keyframes = self.load_keyframes()
            if not keyframes:
                self.logger.info("No keyframes found, skipping filtering")
                return True

            captions_file = os.path.join(
                return_video_folder_name(self.video_runner_obj),
                CAPTIONS_CSV
            )

            if not os.path.exists(captions_file):
                self.logger.error("Captions file not found")
                return False

            # Read existing captions
            with open(captions_file, 'r', newline='') as infile:
                reader = csv.DictReader(infile)
                rows = [row for row in reader if int(row[KEY_FRAME_HEADERS[FRAME_INDEX_SELECTOR]]) in keyframes]

            # Write filtered captions
            with open(captions_file, 'w', newline='') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
                writer.writeheader()
                writer.writerows(rows)

            return True

        except Exception as e:
            self.logger.error(f"Error filtering keyframes: {str(e)}")
            return False