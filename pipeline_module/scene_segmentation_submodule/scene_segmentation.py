import csv
import json
import os
from typing import Dict, Any, List, Tuple
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

            return list_new

        except Exception as e:
            self.logger.error(f"Error parsing CSV file: {str(e)}")
            raise

    def get_segmented_data(self, scene_time_limit: float, threshold: float,
                           list_new: List[List[Any]]) -> List[List[Any]]:
        """
        Generate scene segments based on similarity analysis and timing.
        """
        scenesegments = []
        current_scene_timestamp = 0
        first_skip = False
        skip_timestamp = None
        description = ""
        data = []

        for i in range(len(list_new)):
            # Add description for keyframes
            if list_new[i][7] == "True":
                description = description + "\n" + list_new[i][8]

            # Process non-skip frames
            if list_new[i][4] != "SKIP" and float(list_new[i][4]) < threshold:
                if (self.average_check(float(list_new[i][5]), float(list_new[i][6]), threshold) and
                        list_new[i][1] - current_scene_timestamp > scene_time_limit):
                    scenesegments.append(list_new[i][1])
                    data.append([current_scene_timestamp, list_new[i][1], description])
                    description = ""
                    current_scene_timestamp = list_new[i][1]

            # Handle transition from skip
            if list_new[i][4] != "SKIP" and first_skip:
                if list_new[i][1] - skip_timestamp >= scene_time_limit:
                    scenesegments.append(list_new[i][1])
                    data.append([current_scene_timestamp, list_new[i][1], description])
                    description = ""
                    current_scene_timestamp = list_new[i][1]
                first_skip = False

            # Handle skip frames
            if list_new[i][4] == "SKIP" and not first_skip:
                skip_timestamp = list_new[i][1]
                first_skip = True

        return data

    def incremental_search_for_optimal_threshold(self, low: float, high: float,
                                                 video_duration: float,
                                                 list_new: List[List[Any]]) -> float:
        """
        Search for optimal similarity threshold based on video duration.
        """
        increment = 0.05
        # Target a scene every 25 seconds on average
        optimal_number_of_scenes = video_duration // 25

        self.logger.info(f"Searching for threshold targeting {optimal_number_of_scenes} scenes")

        # Search through thresholds
        for threshold in range(int(low * 100), int(high * 100) + 1, int(increment * 100)):
            threshold /= 100  # Convert back to float
            data = self.get_segmented_data(10, threshold, list_new)

            if len(data) >= optimal_number_of_scenes:
                self.logger.info(f"Found optimal threshold: {threshold}")
                return threshold

        self.logger.info(f"Using maximum threshold: {high}")
        return high

    def run_scene_segmentation(self) -> bool:
        """
        Main entry point for scene segmentation process.
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
                video_duration = float(metadata['duration'])

            # Process similarity data
            list_new = self.parse_CSV_file(output_avg_csv)

            # Find optimal threshold
            optimal_threshold = self.incremental_search_for_optimal_threshold(
                0.75, 1.0, video_duration, list_new
            )

            # Generate scene segments
            data = self.get_segmented_data(10, optimal_threshold, list_new)

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

    def validate_scene_boundaries(self, data: List[List[Any]],
                                  min_scene_duration: float = 5.0) -> bool:
        """
        Validate scene boundaries to ensure they make sense.
        """
        try:
            for scene in data:
                start_time, end_time = float(scene[0]), float(scene[1])

                # Check for negative durations
                if end_time <= start_time:
                    self.logger.error(f"Invalid scene boundaries: end time {end_time} <= start time {start_time}")
                    return False

                # Check for too short scenes
                if end_time - start_time < min_scene_duration:
                    self.logger.error(f"Scene duration too short: {end_time - start_time}s")
                    return False

            return True

        except Exception as e:
            self.logger.error(f"Error validating scene boundaries: {str(e)}")
            return False