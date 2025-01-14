import csv
import os
import traceback
import requests
from typing import Dict, Any, Optional, List
from web_server_module.web_server_database import (
    update_status,
    update_module_output,
    get_module_output
)
from ..utils_module.utils import (
    CAPTION_SCORE,
    OBJECTS_CSV,
    CAPTIONS_CSV,
    CAPTIONS_AND_OBJECTS_CSV,
    CAPTION_IMAGE_PAIR,
    return_video_folder_name
)


class CaptionRating:
    """Enhanced caption rating service with database integration and robust error handling."""

    def __init__(self, video_runner_obj: Dict[str, Any], service_url: Optional[str] = None):
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        # Use service URL from service manager
        self.service_url = service_url or video_runner_obj.get("rating_url")
        if not self.service_url:
            raise ValueError("Rating service URL must be provided")

        self.token = 'VVcVcuNLTwBAaxsb2FRYTYsTnfgLdxKmdDDxMQLvh7rac959eb96BCmmCrAY7Hc3'
        self.rating_threshold = float(os.getenv('CAPTION_RATING_THRESHOLD', '0.5'))

    def get_caption_rating(self, image_data: Dict[str, str]) -> str:
        """Get rating for a single caption using synchronous request with retry."""
        payload = {
            'token': self.token,
            'img_url': image_data['frame_url'],
            'caption': image_data['caption']
        }

        for attempt in range(2):  # Simple retry mechanism
            try:
                self.logger.info(f"Sending rating request for frame {image_data['frame_index']}")
                response = requests.post(
                    self.service_url,
                    data=payload,
                    timeout=10
                )

                if response.status_code == 200:
                    rating = response.text.lstrip("['").rstrip("']")
                    self.logger.info(f"Got rating {rating} for frame {image_data['frame_index']}")
                    return rating

                self.logger.error(f"Rating service returned status {response.status_code}")
                if attempt < 1:  # Only retry on first failure
                    continue

            except (requests.Timeout, requests.RequestException) as e:
                self.logger.error(f"Request failed ({str(e)}), attempt {attempt + 1}")
                if attempt < 1:
                    continue

        return "0.0"  # Default value if all attempts fail

    async def perform_caption_rating(self) -> bool:
        """Main entry point for caption rating process."""
        try:
            # Check if already processed
            module_status = get_module_output(
                self.video_runner_obj["video_id"],
                self.video_runner_obj["AI_USER_ID"],
                'caption_rating'
            )

            if module_status and module_status.get("status") == "completed":
                self.logger.info("Caption rating already processed")
                return True

            # Process all captions
            if not await self.process_all_captions():
                raise Exception("Caption rating failed")

            # Filter captions based on ratings
            if not await self.filter_captions():
                raise Exception("Caption filtering failed")

            # Mark process as complete
            update_module_output(
                self.video_runner_obj["video_id"],
                self.video_runner_obj["AI_USER_ID"],
                'caption_rating',
                {"status": "completed"}
            )

            return True

        except Exception as e:
            self.logger.error(f"Error in caption rating: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False

    async def process_all_captions(self) -> bool:
        """Process and rate all captions."""
        try:
            # Get module status
            module_status = get_module_output(
                self.video_runner_obj["video_id"],
                self.video_runner_obj["AI_USER_ID"],
                'caption_rating'
            )

            processed_frames = module_status.get("processed_frames", []) if module_status else []

            # Read caption pairs
            video_folder = return_video_folder_name(self.video_runner_obj)
            caption_pair_file = os.path.join(video_folder, CAPTION_IMAGE_PAIR)
            output_file = os.path.join(video_folder, CAPTION_SCORE)

            # Process captions
            with open(caption_pair_file, 'r', newline='') as infile:
                data = list(csv.DictReader(infile))

                # Open output file in appropriate mode
                mode = 'a' if os.path.exists(output_file) and processed_frames else 'w'
                with open(output_file, mode, newline='') as outfile:
                    writer = csv.writer(outfile)
                    if mode == 'w':
                        writer.writerow(['frame_index', 'frame_url', 'caption', 'rating'])

                    for image_data in data:
                        frame_index = int(image_data['frame_index'])
                        if frame_index in processed_frames:
                            continue

                        rating = self.get_caption_rating(image_data)
                        writer.writerow([
                            frame_index,
                            image_data['frame_url'],
                            image_data['caption'],
                            rating
                        ])

                        # Update progress
                        processed_frames.append(frame_index)
                        update_module_output(
                            self.video_runner_obj["video_id"],
                            self.video_runner_obj["AI_USER_ID"],
                            'caption_rating',
                            {"processed_frames": processed_frames}
                        )

            return True

        except Exception as e:
            self.logger.error(f"Error processing captions: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False

    async def filter_captions(self) -> bool:
        """Filter captions based on ratings and combine with object data."""
        try:
            video_folder = return_video_folder_name(self.video_runner_obj)

            # Load rated captions
            rated_captions = self.load_rated_captions()
            filtered_indices = self.filter_by_rating(rated_captions)

            # Process object detection data
            obj_data = self.load_object_data()
            caption_data = self.load_caption_data(filtered_indices)

            # Combine and save results
            await self.save_filtered_results(obj_data, caption_data)

            return True

        except Exception as e:
            self.logger.error(f"Error filtering captions: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False

    def load_rated_captions(self) -> List[Dict]:
        """Load captions with their ratings."""
        caption_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            CAPTION_SCORE
        )
        with open(caption_file, newline='') as f:
            return list(csv.DictReader(f))

    def filter_by_rating(self, captions: List[Dict]) -> List[str]:
        """Filter frame indices based on rating threshold."""
        return [
            str(caption['frame_index'])
            for caption in captions
            if self.is_valid_rating(caption.get('rating', '0.0'))
        ]

    def is_valid_rating(self, rating: str) -> bool:
        """Check if rating is valid and above threshold."""
        try:
            return float(rating) > self.rating_threshold
        except ValueError:
            return False

    def load_object_data(self) -> tuple:
        """Load object detection data."""
        objects_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            OBJECTS_CSV
        )
        with open(objects_file, newline='') as f:
            reader = csv.reader(f)
            return next(reader), list(reader)  # header, rows

    def load_caption_data(self, filtered_indices: List[str]) -> tuple:
        """Load filtered caption data."""
        captions_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            CAPTIONS_CSV
        )
        with open(captions_file, newline='') as f:
            reader = csv.reader(f)
            header = next(reader)
            rows = [row for row in reader if row[0] in filtered_indices]
            return header, rows

    async def save_filtered_results(
            self,
            obj_data: tuple,
            caption_data: tuple
    ) -> None:
        """Save filtered and combined results."""
        obj_header, obj_rows = obj_data
        capt_header, capt_rows = caption_data

        output_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            CAPTIONS_AND_OBJECTS_CSV
        )

        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f)
            header = capt_header + obj_header[1:]
            writer.writerow(header)

            for idx in range(len(obj_rows)):
                try:
                    new_row = capt_rows[idx] + obj_rows[idx][1:]
                    writer.writerow(new_row)
                except IndexError:
                    continue

        # Update progress
        update_module_output(
            self.video_runner_obj["video_id"],
            self.video_runner_obj["AI_USER_ID"],
            'caption_rating',
            {"filtering_completed": True}
        )