import csv
import os
import json
import time
import requests
import traceback
from typing import Dict, Any, Optional, List, Tuple
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
    """
    Handles caption rating process with synchronous processing as per professor's requirements.
    Each service instance handles one request at a time.
    """

    def __init__(self, video_runner_obj: Dict[str, Any]):
        """Initialize caption rating handler with necessary configurations."""
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")

        # Service configuration
        self.token = 'VVcVcuNLTwBAaxsb2FRYTYsTnfgLdxKmdDDxMQLvh7rac959eb96BCmmCrAY7Hc3'
        self.rating_threshold = float(os.getenv('CAPTION_RATING_THRESHOLD', '0.5'))
        self.max_retries = 2
        self.request_timeout = 10

    def validate_input_files(self) -> bool:
        """Validate existence of required input files."""
        required_files = {
            'caption_pairs': os.path.join(
                return_video_folder_name(self.video_runner_obj),
                CAPTION_IMAGE_PAIR
            ),
            'objects': os.path.join(
                return_video_folder_name(self.video_runner_obj),
                OBJECTS_CSV
            )
        }

        for file_type, filepath in required_files.items():
            if not os.path.exists(filepath):
                self.logger.error(f"Required {file_type} file not found: {filepath}")
                return False

            # Check if file is not empty
            if os.path.getsize(filepath) == 0:
                self.logger.error(f"Required {file_type} file is empty: {filepath}")
                return False

        return True

    def get_caption_rating(self, image_data: Dict[str, str]) -> str:
        service = None
        try:
            # Get fresh service for each request
            service = self.video_runner_obj["service_manager"].rating_balancer.get_next_service()
            service_url = service.get_url(endpoint="/api")

            payload = {
                'token': self.token,
                'img_url': image_data['frame_url'],
                'caption': image_data['caption']
            }

            for attempt in range(self.max_retries):
                try:
                    self.logger.info(f"Sending request to {service_url}")

                    response = requests.post(
                        service_url,
                        data=payload,
                        timeout=self.request_timeout
                    )
                    if response.status_code == 200:
                        rating = response.text.lstrip("['").rstrip("']")
                        self.logger.info(f"Got rating {rating} for frame {image_data['frame_index']}")
                        return rating
                except (requests.Timeout, requests.RequestException) as e:
                    self.logger.warning(f"Request attempt {attempt + 1} failed: {str(e)}")
                    time.sleep(1)  # Small delay between retries

            # All attempts failed, return default rating
            self.logger.warning(f"All rating attempts failed for frame {image_data['frame_index']}")
            return "0.5"  # Default non-zero rating to keep captions in the pipeline
        finally:
            if service:
                self.video_runner_obj["service_manager"].rating_balancer.release_service(service)

    def process_captions(self, caption_pair_file: str) -> bool:
        """Process all captions and save ratings to file."""
        try:
            # Get module status to check for previous progress
            module_status = get_module_output(
                self.video_runner_obj["video_id"],
                self.video_runner_obj["AI_USER_ID"],
                'caption_rating'
            )
            processed_frames = set(module_status.get("processed_frames", [])) if module_status else set()

            output_file = os.path.join(
                return_video_folder_name(self.video_runner_obj),
                CAPTION_SCORE
            )

            # Read existing ratings if any
            existing_ratings = {}
            if os.path.exists(output_file):
                with open(output_file, 'r', newline='') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        existing_ratings[row['frame_index']] = row

            # Process new ratings
            with open(caption_pair_file, 'r', newline='') as infile:
                reader = csv.DictReader(infile)
                data = list(reader)

            # Write all ratings (existing and new)
            with open(output_file, 'w', newline='') as outfile:
                writer = csv.writer(outfile)
                writer.writerow(['frame_index', 'frame_url', 'caption', 'rating'])

                for row in data:
                    frame_index = row['frame_index']

                    # Skip if already processed
                    if frame_index in processed_frames:
                        if frame_index in existing_ratings:
                            existing_row = existing_ratings[frame_index]
                            writer.writerow([
                                existing_row['frame_index'],
                                existing_row['frame_url'],
                                existing_row['caption'],
                                existing_row['rating']
                            ])
                        continue

                    # Get new rating
                    rating = self.get_caption_rating(row)
                    writer.writerow([
                        frame_index,
                        row['frame_url'],
                        row['caption'],
                        rating
                    ])

                    # Update progress
                    processed_frames.add(frame_index)
                    update_module_output(
                        self.video_runner_obj["video_id"],
                        self.video_runner_obj["AI_USER_ID"],
                        'caption_rating',
                        {"processed_frames": list(processed_frames)}
                    )

            return True

        except Exception as e:
            self.logger.error(f"Error processing captions: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False

    def filter_rated_captions(self) -> List[Dict[str, Any]]:
        """Filter captions based on rating threshold."""
        try:
            caption_file = os.path.join(
                return_video_folder_name(self.video_runner_obj),
                CAPTION_SCORE
            )

            filtered_captions = []
            with open(caption_file, 'r', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        rating = float(row['rating'])
                        if rating >= self.rating_threshold:
                            filtered_captions.append(row)
                    except ValueError:
                        self.logger.warning(f"Invalid rating value for frame {row['frame_index']}")
                        continue

            return filtered_captions

        except Exception as e:
            self.logger.error(f"Error filtering rated captions: {str(e)}")
            return []

    def load_object_detection_data(self) -> Tuple[List[str], List[List[str]]]:
        """Load object detection data from CSV."""
        try:
            objects_file = os.path.join(
                return_video_folder_name(self.video_runner_obj),
                OBJECTS_CSV
            )

            with open(objects_file, 'r', newline='') as f:
                reader = csv.reader(f)
                header = next(reader)
                rows = list(reader)

            return header, rows

        except Exception as e:
            self.logger.error(f"Error loading object detection data: {str(e)}")
            return [], []

    def generate_captions_and_objects(self) -> bool:
        """Generate final combined CSV with captions and object data."""
        try:
            # Get filtered captions
            filtered_captions = self.filter_rated_captions()

            # If no captions passed the threshold, use all captions with a warning
            if not filtered_captions:
                self.logger.warning("No captions passed rating threshold. Using all captions with original ratings.")

                # Load all captions instead
                caption_file = os.path.join(
                    return_video_folder_name(self.video_runner_obj),
                    CAPTION_SCORE
                )

                all_captions = []
                with open(caption_file, 'r', newline='') as f:
                    reader = csv.DictReader(f)
                    all_captions = list(reader)

                if not all_captions:
                    self.logger.error("No caption data available at all")
                    return False

                filtered_captions = all_captions

            # Get object detection data
            obj_header, obj_rows = self.load_object_detection_data()

            # Prepare output file
            output_file = os.path.join(
                return_video_folder_name(self.video_runner_obj),
                CAPTIONS_AND_OBJECTS_CSV
            )

            # If we have proper object data, use it
            if obj_header and obj_rows:
                with open(output_file, 'w', newline='') as f:
                    writer = csv.writer(f)

                    # Combine headers (skip frame_index from objects as it's in captions)
                    combined_header = ['frame_index', 'frame_url', 'caption', 'rating'] + obj_header[1:]
                    writer.writerow(combined_header)

                    # Match and combine data
                    for caption in filtered_captions:
                        frame_idx = caption['frame_index']
                        # Find matching object data
                        obj_row = next((row for row in obj_rows if row[0] == frame_idx), None)
                        if obj_row:
                            combined_row = [
                                               frame_idx,
                                               caption['frame_url'],
                                               caption['caption'],
                                               caption['rating']
                                           ] + obj_row[1:]
                            writer.writerow(combined_row)
                        else:
                            # No matching object data, use zeros for object values
                            combined_row = [
                                               frame_idx,
                                               caption['frame_url'],
                                               caption['caption'],
                                               caption['rating']
                                           ] + ['0'] * (len(obj_header) - 1)
                            writer.writerow(combined_row)
            else:
                # No object data at all, create file with just the captions
                # but match structure needed for next module
                self.logger.warning("No object detection data available. Creating backup file.")

                # Create the output file with just captions
                with open(output_file, 'w', newline='') as f:
                    writer = csv.writer(f)
                    # Add timestamp column that's required for scene segmentation
                    writer.writerow(['frame_index', 'frame_url', 'caption', 'rating', 'timestamp'])

                    for caption in filtered_captions:
                        # Estimate timestamp from frame_index (assuming 30fps)
                        estimated_timestamp = float(caption['frame_index']) / 30.0
                        writer.writerow([
                            caption['frame_index'],
                            caption['frame_url'],
                            caption['caption'],
                            caption['rating'],
                            str(estimated_timestamp)
                        ])

            self.logger.info(f"Generated captions and objects file: {output_file}")
            return True

        except Exception as e:
            self.logger.error(f"Error generating captions and objects file: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False

    def perform_caption_rating(self) -> bool:
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

            # Validate required input files
            if not self.validate_input_files():
                self.logger.error("Required input files not found or empty")
                return False

            # Get caption-image pairs file path
            caption_pair_file = os.path.join(
                return_video_folder_name(self.video_runner_obj),
                CAPTION_IMAGE_PAIR
            )

            # Try to process captions, but continue if it fails
            caption_success = True
            try:
                caption_success = self.process_captions(caption_pair_file)
            except Exception as e:
                self.logger.error(f"Error processing captions: {str(e)}")
                self.logger.error(traceback.format_exc())
                caption_success = False

            # Even if caption rating failed, try to generate combined file
            # Generate final combined file
            if not self.generate_captions_and_objects():
                self.logger.error("Failed to generate captions and objects file")
                return False

            # Mark process as complete
            update_module_output(
                self.video_runner_obj["video_id"],
                self.video_runner_obj["AI_USER_ID"],
                'caption_rating',
                {"status": "completed", "success": caption_success}
            )

            self.logger.info("Caption rating completed successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error in caption rating: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False