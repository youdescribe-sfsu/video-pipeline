import csv
import os
import json
import requests
import traceback
from typing import Dict, Any, Optional, List
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
    KEYFRAMES_CSV,
    TIMESTAMP_SELECTOR,
    return_video_folder_name,
    return_video_frames_folder,
    CAPTION_IMAGE_PAIR
)


class ImageCaptioning:
    """Enhanced image captioning with database integration and proper error handling."""

    def __init__(self, video_runner_obj: Dict[str, Any], service_url: Optional[str] = None):
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        # Use service URL from service manager
        self.service_url = service_url or video_runner_obj.get("caption_url")
        if not self.service_url:
            raise ValueError("Captioning service URL must be provided")

        self.token = 'VVcVcuNLTwBAaxsb2FRYTYsTnfgLdxKmdDDxMQLvh7rac959eb96BCmmCrAY7Hc3'
        self.min_length = 25
        self.max_length = 50

    def get_caption(self, filename: str) -> str:
        """Get caption for single image using synchronous request with retry."""
        for attempt in range(2):  # Simple retry mechanism
            try:
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
                        timeout=10
                    )

                    if response.status_code == 200:
                        caption = response.json()['caption']
                        self.logger.info(f"Got caption: {caption}")
                        return caption.strip()
                    else:
                        self.logger.error(f"Caption service returned status {response.status_code}")
                        if attempt < 1:  # Only retry on first failure
                            continue
                        return ""

            except requests.Timeout:
                self.logger.error(f"Request timed out for file {filename}")
                if attempt < 1:
                    continue
                return ""
            except Exception as e:
                self.logger.error(f"Error getting caption: {str(e)}")
                if attempt < 1:
                    continue
                return ""

        return ""  # Return empty string if all attempts fail

    @timeit
    def run_image_captioning(self) -> bool:
        """Main entry point for image captioning process."""
        try:
            module_status = get_module_output(
                self.video_runner_obj["video_id"],
                self.video_runner_obj["AI_USER_ID"],
                'image_captioning'
            )
            if module_status and module_status.get("status") == "completed":
                self.logger.info("Image captioning already processed")
                return True

            # Get previous frame data if any
            last_processed = module_status.get("last_processed_frame", 0) if module_status else 0

            results = self.process_frames(last_processed)
            if not results:
                raise Exception("Frame processing failed")

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

    def process_frames(self, last_processed: int = 0) -> List[Dict[str, Any]]:
        """Process frames and generate captions."""
        try:
            video_frames_path = return_video_frames_folder(self.video_runner_obj)
            video_folder_path = return_video_folder_name(self.video_runner_obj)

            # Load keyframes information
            keyframes = self.load_keyframes()

            results = []
            frames_to_process = self.get_frames_to_process(last_processed)

            outcsvpath = os.path.join(video_folder_path, CAPTIONS_CSV)
            mode = 'w' if last_processed == 0 else 'a'

            with open(outcsvpath, mode, newline='') as outcsvfile:
                writer = csv.writer(outcsvfile)
                if last_processed == 0:
                    writer.writerow([
                        KEY_FRAME_HEADERS[FRAME_INDEX_SELECTOR],
                        KEY_FRAME_HEADERS[TIMESTAMP_SELECTOR],
                        KEY_FRAME_HEADERS[IS_KEYFRAME_SELECTOR],
                        KEY_FRAME_HEADERS[KEYFRAME_CAPTION_SELECTOR]
                    ])

                for frame in frames_to_process:
                    frame_data = self.process_single_frame(frame, keyframes)
                    if frame_data:
                        results.append(frame_data)
                        writer.writerow([
                            frame_data['frame_index'],
                            frame_data['timestamp'],
                            frame_data['is_keyframe'],
                            frame_data['caption']
                        ])

                        # Update progress in database
                        update_module_output(
                            self.video_runner_obj["video_id"],
                            self.video_runner_obj["AI_USER_ID"],
                            'image_captioning',
                            {"last_processed_frame": frame_data['frame_index']}
                        )

            return results

        except Exception as e:
            self.logger.error(f"Error processing frames: {str(e)}")
            self.logger.error(traceback.format_exc())
            return []

    def process_single_frame(self, frame_index: int, keyframes: List[int]) -> Optional[Dict[str, Any]]:
        """Process a single frame and return its data."""
        frame_filename = os.path.join(
            return_video_frames_folder(self.video_runner_obj),
            f'frame_{frame_index}.jpg'
        )

        if not os.path.exists(frame_filename):
            return None

        caption = self.get_caption(frame_filename)
        if not caption:
            return None

        return {
            'frame_index': frame_index,
            'timestamp': self.calculate_timestamp(frame_index),
            'is_keyframe': frame_index in keyframes,
            'caption': caption
        }

    def load_keyframes(self) -> List[int]:
        """Load keyframe information from file."""
        keyframes_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            KEYFRAMES_CSV
        )
        with open(keyframes_file, newline='') as incsvfile:
            reader = csv.reader(incsvfile)
            next(reader)  # Skip header
            return [int(row[0]) for row in reader]

    def get_frames_to_process(self, last_processed: int) -> List[int]:
        """Get list of frames that need processing."""
        module_data = get_module_output(
            self.video_runner_obj["video_id"],
            self.video_runner_obj["AI_USER_ID"],
            'frame_extraction'
        )

        if not module_data:
            raise ValueError("No frame extraction data found")

        step = int(module_data['steps'])
        num_frames = int(module_data['frames_extracted'])

        return list(range(last_processed + step, num_frames, step))

    def calculate_timestamp(self, frame_index: int) -> float:
        """Calculate timestamp for a frame."""
        module_data = get_module_output(
            self.video_runner_obj["video_id"],
            self.video_runner_obj["AI_USER_ID"],
            'frame_extraction'
        )

        step = int(module_data['steps'])
        fps = float(module_data['adaptive_fps'])
        return frame_index / (step * fps)

    def combine_image_caption(self) -> bool:
        """Combine frame data with captions."""
        try:
            video_frames_path = return_video_frames_folder(self.video_runner_obj)
            captions_path = os.path.join(
                return_video_folder_name(self.video_runner_obj),
                CAPTIONS_CSV
            )

            with open(captions_path, 'r', newline='') as captcsvfile:
                data = list(csv.DictReader(captcsvfile))

            pairs = []
            for row in data:
                frame_index = row[KEY_FRAME_HEADERS[FRAME_INDEX_SELECTOR]]
                pairs.append({
                    "frame_index": frame_index,
                    "frame_url": f'{video_frames_path}/frame_{frame_index}.jpg',
                    "caption": row[KEY_FRAME_HEADERS[KEYFRAME_CAPTION_SELECTOR]]
                })

            output_file = os.path.join(
                return_video_folder_name(self.video_runner_obj),
                CAPTION_IMAGE_PAIR
            )

            with open(output_file, 'w', newline='') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=["frame_index", "frame_url", "caption"])
                writer.writeheader()
                writer.writerows(pairs)

            update_module_output(
                self.video_runner_obj["video_id"],
                self.video_runner_obj["AI_USER_ID"],
                'image_captioning',
                {"caption_pairs_generated": True}
            )

            return True

        except Exception as e:
            self.logger.error(f"Error combining image captions: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False