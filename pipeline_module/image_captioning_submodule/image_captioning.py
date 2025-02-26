import csv
import os
import re
import time
import json
import requests
import traceback
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
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
    KEYFRAMES_CSV,
    OBJECTS_CSV
)


class CaptionVerifier:
    """Helper class for verifying and enhancing captions based on detected objects."""

    def __init__(self, video_runner_obj):
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        self.object_cache = {}
        self.video_title = self._get_video_title()
        self.brand_patterns = {
            r'pringles': 'Pringles',
            r'coca[\s-]?cola': 'Coca-Cola',
            r'pepsi': 'Pepsi',
            r'disney': 'Disney',
            r'pixar': 'Pixar',
            r'inside out': 'Inside Out',
            r'mario': 'Mario',
            r'iphone': 'iPhone',
            r'samsung': 'Samsung',
            r'netflix': 'Netflix',
            r'amazon': 'Amazon',
            r'youtube': 'YouTube'
        }

        # Preload objects data
        self._preload_objects()

    def _get_video_title(self):
        """Get the video title from metadata."""
        try:
            metadata_file = os.path.join(return_video_folder_name(self.video_runner_obj), "metadata.json")
            if os.path.exists(metadata_file):
                import json
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                    return metadata.get("title", "")
            return ""
        except Exception as e:
            self.logger.error(f"Error getting video title: {e}")
            return ""

    def _preload_objects(self):
        """Preload objects data for efficient access."""
        try:
            objects_file = os.path.join(return_video_folder_name(self.video_runner_obj), OBJECTS_CSV)
            if not os.path.exists(objects_file):
                return

            with open(objects_file, 'r', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        frame_idx = int(row['frame_index'])
                        # Extract detected objects (columns with values > 0.4)
                        detected = {}
                        for key, value in row.items():
                            if key not in ['frame_index', 'timestamp'] and value and value.strip():
                                try:
                                    confidence = float(value)
                                    if confidence > 0.4:  # Only include objects with reasonable confidence
                                        detected[key] = confidence
                                except (ValueError, TypeError):
                                    pass

                        self.object_cache[frame_idx] = detected
                    except Exception as e:
                        self.logger.error(f"Error processing object row: {e}")
        except Exception as e:
            self.logger.error(f"Error preloading objects: {e}")

    def get_objects_for_frame(self, frame_index):
        """Get detected objects for a specific frame."""
        # Use cached data if available
        if frame_index in self.object_cache:
            return self.object_cache[frame_index]

        # Otherwise load from file
        try:
            objects_file = os.path.join(return_video_folder_name(self.video_runner_obj), OBJECTS_CSV)
            if not os.path.exists(objects_file):
                return {}

            with open(objects_file, 'r', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if int(row['frame_index']) == frame_index:
                        # Extract detected objects (columns with values > 0.4)
                        detected = {}
                        for key, value in row.items():
                            if key not in ['frame_index', 'timestamp'] and value and value.strip():
                                try:
                                    confidence = float(value)
                                    if confidence > 0.4:  # Only include objects with reasonable confidence
                                        detected[key] = confidence
                                except (ValueError, TypeError):
                                    pass

                        # Cache for future use
                        self.object_cache[frame_index] = detected
                        return detected

            return {}
        except Exception as e:
            self.logger.error(f"Error getting objects for frame {frame_index}: {e}")
            return {}

    def get_brands_from_title(self):
        """Extract brand names from video title."""
        detected_brands = []
        title_lower = self.video_title.lower()

        for pattern, brand in self.brand_patterns.items():
            if re.search(r'\b' + pattern + r'\b', title_lower):
                detected_brands.append(brand)

        return detected_brands

    def verify_caption(self, caption, frame_index):
        """
        Verify caption against detected objects and video context.

        Returns:
            tuple: (is_valid, reason, improved_caption)
        """
        if not caption or len(caption) < 10:
            return False, "Caption too short", self.generate_better_caption(frame_index)

        # Get detected objects for this frame
        objects = self.get_objects_for_frame(frame_index)
        detected_object_names = list(objects.keys())

        # Check for common caption errors
        caption_lower = caption.lower()

        # Common misleading phrases that often appear in captions
        misleading_phrases = {
            "man and woman stand": ["person"],
            "couple are trying to sell": [],
            "local charity": [],
            "tv tablet": [],
            "called a tv tablet": []
        }

        # Check for inconsistencies
        for phrase, required_objects in misleading_phrases.items():
            if phrase in caption_lower:
                # If the phrase requires specific objects
                if required_objects:
                    missing_objects = [obj for obj in required_objects if obj not in detected_object_names]
                    if missing_objects:
                        return False, f"Caption mentions '{phrase}' but objects {missing_objects} not detected", \
                            self.generate_better_caption(frame_index)
                else:
                    # If the phrase is likely incorrect based on context
                    return False, f"Caption contains potentially incorrect phrase '{phrase}'", \
                        self.generate_better_caption(frame_index)

        # Check for brand mentions in title but not in caption
        brands_in_title = self.get_brands_from_title()
        if brands_in_title:
            brand_mentioned = any(brand.lower() in caption_lower for brand in brands_in_title)
            if not brand_mentioned:
                # Caption should probably mention the brand
                improved = self.generate_better_caption(frame_index)
                return False, f"Caption missing brand {brands_in_title} mentioned in title", improved

        # If no issues found, caption is valid
        return True, "Caption is valid", caption

    def generate_better_caption(self, frame_index):
        """
        Generate an improved caption based on objects and context.
        """
        objects = self.get_objects_for_frame(frame_index)
        brands_in_title = self.get_brands_from_title()

        # If no objects detected, return a generic caption
        if not objects:
            if brands_in_title:
                return f"A scene from the video showing {' and '.join(brands_in_title)} content."
            else:
                return "A scene from the video."

        # Initialize caption components
        subjects = []
        locations = []
        objects_described = []

        # Map commonly detected objects to more descriptive phrases
        object_descriptions = {
            'person': 'a person',
            'cup': 'a cup',
            'bottle': 'a bottle',
            'chair': 'a chair',
            'dining table': 'a table',
            'tvmonitor': 'a screen',
            'laptop': 'a laptop',
            'cell phone': 'a mobile phone',
            'book': 'a book',
            'toothbrush': 'a toothbrush',
            'remote': 'a remote control'
        }

        # Locations where objects might be
        location_objects = {
            'dining table': 'on a table',
            'bed': 'on a bed',
            'chair': 'on a chair',
            'couch': 'on a couch',
            'floor': 'on the floor'
        }

        # Identify subjects and locations
        for obj, confidence in sorted(objects.items(), key=lambda x: x[1], reverse=True):
            if obj in object_descriptions and obj not in objects_described:
                subjects.append(object_descriptions[obj])
                objects_described.append(obj)

            if obj in location_objects and obj not in objects_described:
                locations.append(location_objects[obj])
                objects_described.append(obj)

        # Start building the caption
        caption = ""

        # Add subjects
        if subjects:
            if len(subjects) == 1:
                caption += f"{subjects[0].capitalize()} is shown"
            else:
                caption += f"{', '.join(subjects[:-1])} and {subjects[-1]} are shown"
        else:
            caption += "The video shows"

        # Add brands if present in title
        if brands_in_title:
            caption += f" featuring {' and '.join(brands_in_title)}"

        # Add locations if any
        if locations:
            caption += f" {locations[0]}"

        # Close the caption
        caption += "."

        # Add hashtags if they were in the original title
        if '#' in self.video_title:
            hashtags = re.findall(r'#\w+', self.video_title)
            if hashtags:
                caption += f" {' '.join(hashtags[:2])}"  # Include up to 2 hashtags

        return caption


class ImageCaptioning:
    """
    Handles image captioning process with synchronous processing as per professor's requirements.
    Each service instance handles one request at a time.
    """

    def __init__(self, video_runner_obj: Dict[str, Any]):
        """Initialize the image captioning handler with necessary configurations."""
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")

        # Service configuration
        self.token = 'VVcVcuNLTwBAaxsb2FRYTYsTnfgLdxKmdDDxMQLvh7rac959eb96BCmmCrAY7Hc3'
        self.min_length = 25
        self.max_length = 50
        self.max_retries = 2
        self.request_timeout = 10

    def get_caption(self, filename: str) -> str:
        service = None
        try:
            service = self.video_runner_obj["service_manager"].caption_balancer.get_next_service()
            service_url = service.get_url(endpoint="/upload")

            for attempt in range(self.max_retries):
                try:
                    with open(filename, 'rb') as fileBuffer:
                        multipart_form_data = {
                            'token': ('', self.token),
                            'image': (os.path.basename(filename), fileBuffer),
                            'min_length': ('', str(self.min_length)),
                            'max_length': ('', str(self.max_length))
                        }
                        self.logger.info(f"Sending request to {service_url}")
                        response = requests.post(
                            service_url,
                            files=multipart_form_data,
                            timeout=self.request_timeout
                        )
                        if response.status_code == 200:
                            caption = response.json()['caption']
                            self.logger.info(f"Got caption: {caption}")
                            time.sleep(4)
                            return caption.strip()
                except (requests.Timeout, requests.RequestException) as e:
                    if attempt == self.max_retries - 1:
                        self.video_runner_obj["service_manager"].caption_balancer.mark_service_failed(service)
                        service = None
        finally:
            if service:
                self.video_runner_obj["service_manager"].caption_balancer.release_service(service)
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

    def _original_run_image_captioning(self) -> bool:
        """Original implementation preserved for fallback."""
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

    @timeit
    def run_image_captioning(self) -> bool:
        """
        Enhanced implementation of run_image_captioning with caption verification.
        """
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

            # Initialize caption verifier
            caption_verifier = CaptionVerifier(self.video_runner_obj)

            # Process frames and generate captions
            results = self.process_frames()
            if not results:
                raise Exception("Frame processing failed")

            # Verify and improve captions
            improved_results = []
            for result in results:
                frame_index = result['frame_index']

                # Verify the caption
                is_valid, reason, improved_caption = caption_verifier.verify_caption(
                    result['caption'], frame_index
                )

                if not is_valid:
                    self.logger.info(f"Improving caption for frame {frame_index}: {reason}")
                    result['caption'] = improved_caption
                    result['auto_improved'] = True
                    result['improvement_reason'] = reason

                improved_results.append(result)

            # Save results and generate required files
            success = self.save_captions_csv(improved_results)
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
                {"status": "completed", "captions": improved_results}
            )

            self.logger.info("Image captioning with verification completed successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error in enhanced image captioning: {str(e)}")
            self.logger.error(traceback.format_exc())

            # Try to run original implementation as fallback
            self.logger.info("Falling back to original implementation")
            return self._original_run_image_captioning()

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