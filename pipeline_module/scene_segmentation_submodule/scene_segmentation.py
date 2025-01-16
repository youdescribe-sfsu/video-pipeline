import csv
import json
import os
from typing import Dict, Any, List, Tuple, Optional
import warnings
from web_server_module.web_server_database import (
    update_status,
    get_status_for_youtube_id,
    update_module_output
)
from ..utils_module.utils import (
    OUTPUT_AVG_CSV,
    SCENE_SEGMENTED_FILE_CSV,
    return_video_folder_name
)
from .generate_average_output import generate_average_output


class SceneSegmentation:
    """
    Handles video scene segmentation using similarity analysis and dynamic thresholding.
    Added robust error handling and scene validation to prevent empty outputs.
    """

    def __init__(self, video_runner_obj: Dict[str, Any]):
        """Initialize SceneSegmentation with video information and logging."""
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        self.columns = {
            "start_time": "start_time",
            "end_time": "end_time",
            "description": "description",
        }
        # Minimum scene duration in seconds
        self.MIN_SCENE_DURATION = 5.0
        # Default scene time limit
        self.DEFAULT_SCENE_TIME_LIMIT = 10.0

    def validate_scene_data(self, data: List[List[Any]]) -> bool:
        """
        Validate scene data before saving.
        Ensures scenes are properly formatted and cover a reasonable duration.
        """
        if not data:
            self.logger.error("Scene data is empty")
            return False

        try:
            total_duration = 0
            for scene in data:
                if len(scene) != 3:  # start_time, end_time, description
                    self.logger.error(f"Invalid scene format: {scene}")
                    return False

                start_time = float(scene[0])
                end_time = float(scene[1])

                if end_time <= start_time:
                    self.logger.error(f"Invalid scene timing: end {end_time} <= start {start_time}")
                    return False

                if end_time - start_time < self.MIN_SCENE_DURATION:
                    self.logger.warning(f"Scene duration too short: {end_time - start_time}s")
                    # Don't fail for short scenes, just warn

                total_duration += (end_time - start_time)

            return total_duration > 0

        except Exception as e:
            self.logger.error(f"Error validating scene data: {str(e)}")
            return False

    def create_fallback_scene(self, video_duration: float) -> List[List[Any]]:
        """
        Create a fallback single scene when no valid scenes are detected.
        """
        return [[0, video_duration, "Complete video segment"]]

    def average_check(self, averageone: float, averagetwo: float, threshold: float) -> bool:
        """Check if both averages are below the threshold."""
        return averageone < threshold and averagetwo < threshold

    def parse_CSV_file(self, csv_path: str) -> List[List[Any]]:
        """
        Parse the CSV file with careful type handling and validation.
        """
        list_new = []
        try:
            with open(csv_path, "r") as csvFile:
                reader = csv.reader(csvFile)
                headers = next(reader)  # Skip header row

                for row in reader:
                    temp = []
                    for idx, value in enumerate(row):
                        if value == "":
                            temp.append(0.0)
                        elif idx == 4 and value == "SKIP":
                            temp.append(value)
                        elif idx == 7 or idx == 8:  # isKeyFrame and description columns
                            temp.append(value)
                        else:
                            try:
                                temp.append(float(value))
                            except ValueError:
                                temp.append(0.0)  # Fallback for invalid numbers
                    list_new.append(temp)

            if not list_new:
                self.logger.warning("No data found in CSV file")
            return list_new

        except Exception as e:
            self.logger.error(f"Error parsing CSV file: {str(e)}")
            raise

    def get_segmented_data(self, scene_time_limit: float, threshold: float,
                           list_new: List[List[Any]]) -> List[List[Any]]:
        """
        Generate scene segments based on similarity analysis and timing.
        Added validation and minimum scene requirements.
        """
        if not list_new:
            self.logger.error("No input data for scene segmentation")
            return []

        scenesegments = []
        current_scene_timestamp = 0
        first_skip = False
        skip_timestamp = None
        description = ""
        data = []

        for i in range(len(list_new)):
            # Add description for keyframes
            if list_new[i][7] == "True":
                description = description + "\n" + list_new[i][8] if description else list_new[i][8]

            # Process non-skip frames
            if list_new[i][4] != "SKIP" and float(list_new[i][4]) < threshold:
                if (self.average_check(float(list_new[i][5]), float(list_new[i][6]), threshold) and
                        list_new[i][1] - current_scene_timestamp > scene_time_limit):
                    scenesegments.append(list_new[i][1])
                    data.append([current_scene_timestamp, list_new[i][1], description.strip()])
                    description = ""
                    current_scene_timestamp = list_new[i][1]

            # Handle transition from skip
            if list_new[i][4] != "SKIP" and first_skip:
                if list_new[i][1] - skip_timestamp >= scene_time_limit:
                    scenesegments.append(list_new[i][1])
                    data.append([current_scene_timestamp, list_new[i][1], description.strip()])
                    description = ""
                    current_scene_timestamp = list_new[i][1]
                first_skip = False

            # Handle skip frames
            if list_new[i][4] == "SKIP" and not first_skip:
                skip_timestamp = list_new[i][1]
                first_skip = True

        # Handle last scene if necessary
        if list_new and current_scene_timestamp < list_new[-1][1]:
            data.append([current_scene_timestamp, list_new[-1][1], description.strip()])

        return data

    def incremental_search_for_optimal_threshold(self, low: float, high: float,
                                                 video_duration: float,
                                                 list_new: List[List[Any]]) -> float:
        """
        Search for optimal similarity threshold based on video duration.
        Added minimum scene requirements and better error handling.
        """
        if video_duration <= 0:
            self.logger.error("Invalid video duration")
            return high

        increment = 0.05
        # Ensure we have at least 1 scene, even for very short videos
        optimal_number_of_scenes = max(1, int(video_duration // 25))

        self.logger.info(f"Searching for threshold targeting {optimal_number_of_scenes} scenes")

        best_threshold = high
        best_scene_count = 0

        # Search through thresholds
        for threshold in range(int(low * 100), int(high * 100) + 1, int(increment * 100)):
            threshold /= 100
            data = self.get_segmented_data(self.DEFAULT_SCENE_TIME_LIMIT, threshold, list_new)

            scene_count = len(data)

            # Update best if this threshold gives us more scenes (but not too many)
            if optimal_number_of_scenes <= scene_count <= optimal_number_of_scenes * 2:
                best_threshold = threshold
                best_scene_count = scene_count
                self.logger.info(f"Found good threshold {threshold} with {scene_count} scenes")
                break
            elif scene_count > 0 and (best_scene_count == 0 or
                                      abs(scene_count - optimal_number_of_scenes) <
                                      abs(best_scene_count - optimal_number_of_scenes)):
                best_threshold = threshold
                best_scene_count = scene_count

        self.logger.info(f"Selected threshold: {best_threshold} producing {best_scene_count} scenes")
        return best_threshold

    def run_scene_segmentation(self) -> bool:
        """
        Main entry point for scene segmentation process.
        Added comprehensive error handling and scene validation.
        """
        try:
            self.logger.info("Running scene segmentation")

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

            # Find optimal threshold
            optimal_threshold = self.incremental_search_for_optimal_threshold(
                0.75, 1.0, video_duration, list_new
            )

            # Generate scene segments
            data = self.get_segmented_data(self.DEFAULT_SCENE_TIME_LIMIT, optimal_threshold, list_new)

            # Validate scene data or create fallback
            if not self.validate_scene_data(data):
                self.logger.warning("Invalid scene data detected, creating fallback scene")
                data = self.create_fallback_scene(video_duration)

            # Save scene segmentation results
            scene_segmented_file = os.path.join(
                return_video_folder_name(self.video_runner_obj),
                SCENE_SEGMENTED_FILE_CSV
            )

            with open(scene_segmented_file, "w", newline='') as csvFile:
                writer = csv.writer(csvFile)
                writer.writerow(self.columns.values())
                writer.writerows(data)

            self.logger.info(f"Scene segmentation results saved to {scene_segmented_file}")

            # Save results to database
            update_module_output(
                self.video_runner_obj["video_id"],
                self.video_runner_obj["AI_USER_ID"],
                'scene_segmentation',
                {
                    "scenes": data,
                    "threshold": optimal_threshold,
                    "total_scenes": len(data)
                }
            )

            # Mark process as complete
            update_status(
                self.video_runner_obj["video_id"],
                self.video_runner_obj["AI_USER_ID"],
                "done"
            )

            self.logger.info("Scene segmentation completed successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error in scene segmentation: {str(e)}")
            return False