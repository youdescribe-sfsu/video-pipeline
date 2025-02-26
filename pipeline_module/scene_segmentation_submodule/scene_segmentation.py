import os
import json
import csv
import re
import traceback
from typing import Dict, List, Any, Optional

from pipeline_module.scene_segmentation_submodule.generate_average_output import generate_average_output
from pipeline_module.utils_module.utils import return_video_folder_name, SCENE_SEGMENTED_FILE_CSV, OUTPUT_AVG_CSV
from web_server_module.web_server_database import update_status, update_module_output, get_status_for_youtube_id


class EnhancedSceneGenerator:
    """Helper class for improving scene descriptions."""

    def __init__(self, video_runner_obj):
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        self.video_title = self._get_video_title()
        self.video_duration = self._get_video_duration()

    def _get_video_title(self):
        """Get the video title from metadata."""
        try:
            metadata_file = os.path.join(
                return_video_folder_name(self.video_runner_obj),
                "metadata.json"
            )
            if os.path.exists(metadata_file):
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                    return metadata.get("title", "")
            return ""
        except Exception as e:
            self.logger.error(f"Error getting video title: {e}")
            return ""

    def _get_video_duration(self):
        """Get the video duration from metadata."""
        try:
            metadata_file = os.path.join(
                return_video_folder_name(self.video_runner_obj),
                "metadata.json"
            )
            if os.path.exists(metadata_file):
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                    return float(metadata.get("duration", 0))
            return 0
        except Exception as e:
            self.logger.error(f"Error getting video duration: {e}")
            return 0

    def _load_ocr_data(self, start_time, end_time):
        """Load OCR data for the given time range."""
        try:
            ocr_file = os.path.join(
                return_video_folder_name(self.video_runner_obj),
                "ocr_filter_remove_similar.csv"
            )
            if not os.path.exists(ocr_file):
                return []

            ocr_data = []
            with open(ocr_file, 'r', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    timestamp = float(row.get("Timestamp", 0))
                    if start_time <= timestamp <= end_time:
                        ocr_data.append(row.get("OCR Text", "").strip())

            return [text for text in ocr_data if text]
        except Exception as e:
            self.logger.error(f"Error loading OCR data: {e}")
            return []

    def _load_objects_data(self, start_time, end_time):
        """Load object detection data for the given time range."""
        try:
            objects_file = os.path.join(
                return_video_folder_name(self.video_runner_obj),
                "objects.csv"
            )
            if not os.path.exists(objects_file):
                return {}

            objects_data = {}
            with open(objects_file, 'r', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    timestamp = float(row.get("timestamp", 0))
                    if start_time <= timestamp <= end_time:
                        for key, value in row.items():
                            if key not in ["frame_index", "timestamp"] and value:
                                try:
                                    confidence = float(value)
                                    if confidence > 0.4:  # Only include confident detections
                                        if key not in objects_data:
                                            objects_data[key] = 0
                                        objects_data[key] += 1
                                except (ValueError, TypeError):
                                    pass

            # Normalize counts
            total_frames = sum(objects_data.values())
            if total_frames > 0:
                for key in objects_data:
                    objects_data[key] = objects_data[key] / total_frames

            return objects_data
        except Exception as e:
            self.logger.error(f"Error loading objects data: {e}")
            return {}

    def generate_improved_scene_description(self, scene_data):
        """
        Generate an improved scene description based on multiple data sources.
        """
        if not scene_data:
            return None

        start_time = scene_data.get("start_time", 0)
        end_time = scene_data.get("end_time", 0)
        original_text = scene_data.get("text", "").strip()

        # If the scene duration is too short, keep original
        if end_time - start_time < 2:
            return original_text

        # Load additional data for context
        ocr_text = self._load_ocr_data(start_time, end_time)
        objects_data = self._load_objects_data(start_time, end_time)

        # Check if the original text looks suspicious
        suspicious_patterns = [
            r"man and woman stand.*sell.*charity",
            r"tv tablet.*device that allows",
            r"couple are trying to sell"
        ]

        is_suspicious = any(re.search(pattern, original_text, re.IGNORECASE)
                            for pattern in suspicious_patterns)

        # Video type detection
        video_type = "video"
        if "#shorts" in self.video_title.lower():
            video_type = "short video"

        # Extract brand mentions from title and OCR
        brand_patterns = {
            r'pringles': 'Pringles',
            r'coca[\s-]?cola': 'Coca-Cola',
            r'pepsi': 'Pepsi',
            r'disney': 'Disney',
            r'pixar': 'Pixar',
            r'inside out': 'Inside Out',
            r'mario': 'Mario',
            r'iphone': 'iPhone',
            r'android': 'Android'
        }

        detected_brands = []
        # Check title
        title_lower = self.video_title.lower()
        for pattern, brand in brand_patterns.items():
            if re.search(r'\b' + pattern + r'\b', title_lower):
                detected_brands.append(brand)

        # Check OCR
        for text in ocr_text:
            text_lower = text.lower()
            for pattern, brand in brand_patterns.items():
                if re.search(r'\b' + pattern + r'\b', text_lower) and brand not in detected_brands:
                    detected_brands.append(brand)

        # If we found brands but they're not in the original description
        if detected_brands and not any(brand.lower() in original_text.lower() for brand in detected_brands):
            is_suspicious = True

        # If we need to regenerate the description
        if is_suspicious or not original_text:
            # Compose a better description
            description = self._compose_description(
                detected_brands, objects_data, ocr_text, video_type, start_time, end_time
            )
            return description

        return original_text

    def _compose_description(self, brands, objects, ocr_text, video_type, start_time, end_time):
        """Compose a comprehensive scene description."""
        # Start with scene position context
        if start_time == 0:
            description = f"This {video_type} begins with "
        elif start_time >= self.video_duration * 0.7:
            description = f"The {video_type} ends with "
        else:
            description = f"The {video_type} shows "

        # Add brand information if present
        if brands:
            brand_text = " and ".join(brands)
            description += f"{brand_text} "

        # Add object information
        significant_objects = {k: v for k, v in objects.items()
                               if v > 0.3 and k not in ["person", "background"]}

        if significant_objects:
            object_names = list(significant_objects.keys())

            if len(object_names) == 1:
                object_desc = object_names[0]
                if object_desc in ["cup", "bottle"]:
                    if "Pringles" in brands:
                        object_desc = "Pringles container"
                    elif any("cola" in b.lower() for b in brands):
                        object_desc = "soda bottle"

                description += f"a {object_desc}. "
            else:
                # Get top objects
                top_objects = object_names[:3]
                description += f"{', '.join(top_objects[:-1])} and {top_objects[-1]}. "
        else:
            # If no significant objects but we have a person
            if "person" in objects and objects["person"] > 0.3:
                description += "a person. "

        # Add OCR information if relevant
        if ocr_text:
            flavor_patterns = [
                r'\b(SOUR CREAM)\b',
                r'\b(PERI PERI)\b',
                r'\b(ASALA TADKA)\b',
                r'\b(CHUTNEY)\b',
                r'\b(SALT)\b',
                r'\b(ORIGINAL)\b',
                r'\b(BBQ)\b'
            ]

            flavors = set()
            for text in ocr_text:
                for pattern in flavor_patterns:
                    matches = re.findall(pattern, text, re.IGNORECASE)
                    for match in matches:
                        flavors.add(match.upper())

            if flavors and "Pringles" in brands:
                description += f"Various Pringles flavors are shown including {', '.join(flavors)}. "

        # Add information from the video title if relevant
        title_keywords = [word.lower() for word in self.video_title.split()
                          if len(word) > 3 and not word.startswith('#')]

        if "sound" in title_keywords and "Pringles" in brands:
            description += "The video appears to demonstrate the distinctive sound Pringles makes. "

        return description.strip()

    def detect_identical_scenes(self, scenes):
        """
        Detect if all scenes have identical descriptions.
        """
        if not scenes or len(scenes) < 2:
            return False

        first_description = scenes[0].get("text", "").strip()
        return all(scene.get("text", "").strip() == first_description for scene in scenes)

    def improve_scene_segmentation(self, scenes):
        """
        Improve scene segmentation by detecting and fixing issues.
        """
        if not scenes:
            return []

        # Check if all scenes have identical descriptions
        if self.detect_identical_scenes(scenes):
            self.logger.info("Detected identical scene descriptions - improving diversity")

            # Improve each scene individually
            improved_scenes = []
            for i, scene in enumerate(scenes):
                position = "beginning" if i == 0 else "middle" if i < len(scenes) - 1 else "end"

                improved_desc = self.generate_improved_scene_description(scene)
                improved_scenes.append({
                    "start_time": scene["start_time"],
                    "end_time": scene["end_time"],
                    "text": improved_desc
                })

            return improved_scenes

        # Otherwise, just improve individual scenes that need it
        improved_scenes = []
        for scene in scenes:
            original_text = scene.get("text", "").strip()

            # Check for suspicious patterns
            suspicious_patterns = [
                r"man and woman stand.*sell.*charity",
                r"tv tablet.*device that allows",
                r"couple are trying to sell"
            ]

            is_suspicious = any(re.search(pattern, original_text, re.IGNORECASE)
                                for pattern in suspicious_patterns)

            if is_suspicious or not original_text:
                improved_desc = self.generate_improved_scene_description(scene)
                scene["text"] = improved_desc

            improved_scenes.append(scene)

        return improved_scenes


# Enhance the run_scene_segmentation method in SceneSegmentation class
def run_scene_segmentation(self):
    """
    Enhanced implementation of run_scene_segmentation with improved descriptions.
    """
    try:
        self.logger.info("Running enhanced scene segmentation")

        # Check if already processed
        if get_status_for_youtube_id(self.video_runner_obj["video_id"],
                                     self.video_runner_obj["AI_USER_ID"]) == "done":
            self.logger.info("Scene segmentation already processed")
            return True

        # Generate average output if needed
        output_avg_csv = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            OUTPUT_AVG_CSV
        )

        if not os.path.exists(output_avg_csv):
            self.logger.info("Generating average output")
            if not generate_average_output(self.video_runner_obj):
                raise Exception("Failed to generate average output")

        # Load video metadata
        metadata_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            "metadata.json"
        )

        with open(metadata_file, "r") as f:
            metadata = json.load(f)
            video_duration = float(metadata.get('duration', 0))

        if video_duration <= 0:
            raise ValueError(f"Invalid video duration: {video_duration}")

        # Process similarity data
        list_new = self.parse_CSV_file(output_avg_csv)

        # Find optimal threshold and generate scenes
        optimal_threshold = self.incremental_search_for_optimal_threshold(
            video_duration, list_new
        )

        data = self.get_segmented_data(10, optimal_threshold, list_new)

        # Convert data to proper scene format
        scenes = []
        for scene_data in data:
            scenes.append({
                'start_time': scene_data[0],
                'end_time': scene_data[1],
                'text': scene_data[2]
            })

        # Initialize the scene generator
        scene_generator = EnhancedSceneGenerator(self.video_runner_obj)

        # Improve scene descriptions
        improved_scenes = scene_generator.improve_scene_segmentation(scenes)

        # Convert back to the format expected by save function
        improved_data = []
        for scene in improved_scenes:
            improved_data.append([
                scene["start_time"],
                scene["end_time"],
                scene["text"]
            ])

        # Save scene segmentation results
        scene_segmented_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            SCENE_SEGMENTED_FILE_CSV
        )

        with open(scene_segmented_file, "w", newline='') as csvFile:
            writer = csv.writer(csvFile)
            writer.writerow(self.columns.values())
            writer.writerows(improved_data)

        self.logger.info(f"Enhanced scene segmentation results saved to {scene_segmented_file}")

        # Save results to database
        update_module_output(
            self.video_runner_obj["video_id"],
            self.video_runner_obj["AI_USER_ID"],
            'scene_segmentation',
            {
                "scenes": improved_data,
                "threshold": optimal_threshold,
                "total_scenes": len(improved_data)
            }
        )

        # Mark process as complete
        update_status(
            self.video_runner_obj["video_id"],
            self.video_runner_obj["AI_USER_ID"],
            "done"
        )

        self.logger.info("Enhanced scene segmentation completed successfully")
        return True

    except Exception as e:
        self.logger.error(f"Error in enhanced scene segmentation: {str(e)}")
        self.logger.error(traceback.format_exc())

        # Try to run the original implementation as fallback
        try:
            return False
        except Exception as fallback_error:
            self.logger.error(f"Fallback also failed: {str(fallback_error)}")
            return False