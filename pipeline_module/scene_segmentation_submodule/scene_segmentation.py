import csv
import json
import numpy as np
from typing import Dict, Any, List, Tuple
from ..utils_module.utils import OUTPUT_AVG_CSV, SCENE_SEGMENTED_FILE_CSV, read_value_from_file, \
    return_video_folder_name, save_value_to_file
from ..utils_module.timeit_decorator import timeit


class SceneSegmentation:
    def __init__(self, video_runner_obj: Dict[str, Any]):
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        self.columns = {
            "start_time": "start_time",
            "end_time": "end_time",
            "description": "description",
        }

    def average_check(self, averageone: float, averagetwo: float, threshold: float) -> bool:
        return averageone < threshold and averagetwo < threshold

    def parse_CSV_file(self, csvPath: str) -> List[List[Any]]:
        with open(csvPath, "r") as csvFile:
            reader = csv.reader(csvFile)
            headers = next(reader)
            list_new = []
            for row in reader:
                temp = []
                for idx, value in enumerate(row):
                    if value == "":
                        temp.append(0.0)
                    elif idx == 4:
                        temp.append(float(value) if value != "SKIP" else value)
                    elif idx in [7, 8]:
                        temp.append(value)
                    else:
                        temp.append(float(value))
                list_new.append(temp)
        return list_new

    def get_segmented_data(self, scene_time_limit: float, threshold: float, list_new: List[List[Any]]) -> List[
        List[Any]]:
        scenesegments = []
        currentSceneTimeStamp = 0
        firstSkip = False
        skiptimestamp = None
        description = ""
        data = []

        for i, row in enumerate(list_new):
            if row[7] == "True":
                description = description + "\n" + row[8]

            if row[4] != "SKIP" and float(row[4]) < threshold:
                if self.average_check(row[5], row[6], threshold) and row[1] - currentSceneTimeStamp > scene_time_limit:
                    scenesegments.append(row[1])
                    data.append([currentSceneTimeStamp, row[1], description])
                    description = ""
                    currentSceneTimeStamp = row[1]

            if row[4] != "SKIP" and firstSkip:
                if row[1] - skiptimestamp >= scene_time_limit:
                    scenesegments.append(row[1])
                    data.append([currentSceneTimeStamp, row[1], description])
                    description = " "
                    currentSceneTimeStamp = row[1]
                firstSkip = False
            if row[4] == "SKIP":
                if not firstSkip:
                    skiptimestamp = row[1]
                    firstSkip = True

        return data

    @timeit
    def run_scene_segmentation(self) -> None:
        self.logger.info("Running scene segmentation")

        if read_value_from_file(video_runner_obj=self.video_runner_obj,
                                key="['SceneSegmentation']['run_scene_segmentation']") == 1:
            self.logger.info("Scene segmentation already processed")
            return

        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['SceneSegmentation']['started']", value=True)

        if read_value_from_file(video_runner_obj=self.video_runner_obj,
                                key="['SceneSegmentation']['generate_average_output']") != 1:
            self.generate_average_output()
            save_value_to_file(video_runner_obj=self.video_runner_obj,
                               key="['SceneSegmentation']['generate_average_output']", value=1)

        outputavgFile = return_video_folder_name(self.video_runner_obj) + "/" + OUTPUT_AVG_CSV
        sceneSegmentedFile = return_video_folder_name(self.video_runner_obj) + "/" + SCENE_SEGMENTED_FILE_CSV

        list_new = self.parse_CSV_file(outputavgFile)
        data = self.get_segmented_data(10, 0.75, list_new)

        with open(sceneSegmentedFile, "w", newline='') as csvFile:
            writer = csv.writer(csvFile)
            writer.writerow(self.columns.values())
            writer.writerows(data)

        self.logger.info(f"Writing scene segmentation results to {sceneSegmentedFile}")
        save_value_to_file(video_runner_obj=self.video_runner_obj,
                           key="['SceneSegmentation']['run_scene_segmentation']", value=1)
        self.logger.info("Scene segmentation completed")

    def generate_average_output(self) -> None:
        captions_and_objects_csv = return_video_folder_name(self.video_runner_obj) + '/' + 'captions_and_objects.csv'
        output_avg_csv = return_video_folder_name(self.video_runner_obj) + '/' + OUTPUT_AVG_CSV

        with open(captions_and_objects_csv, 'r') as csvFile:
            csvReader = csv.DictReader(csvFile)
            jsonArray = list(csvReader)

        isKeyFrame = [row['Is Keyframe'] for row in jsonArray]
        description = [row['Caption'] for row in jsonArray]
        frame_index = [int(float(row['Frame Index'])) for row in jsonArray]
        timestamp = [float(row['Timestamp']) for row in jsonArray]

        list_data = []
        for row in jsonArray:
            temp = [float(row.get(key, 0)) if row.get(key) != '' else 0.0 for key in row.keys() if
                    key not in ['Frame Index', 'Timestamp', 'Is Keyframe', 'Caption']]
            list_data.append(temp)

        data = []
        for idx in range(2, len(list_data) - 1):
            s = np.dot(list_data[idx], list_data[idx + 1]) / (
                        np.linalg.norm(list_data[idx]) * np.linalg.norm(list_data[idx + 1]))
            a1 = np.dot(list_data[idx - 1], list_data[idx + 2]) / (
                        np.linalg.norm(list_data[idx - 1]) * np.linalg.norm(list_data[idx + 2])) if idx < len(
                list_data) - 3 else 0.0
            a2 = np.dot(list_data[idx - 2], list_data[idx + 3]) / (
                        np.linalg.norm(list_data[idx - 2]) * np.linalg.norm(list_data[idx + 3])) if idx < len(
                list_data) - 3 else 0.0
            s = "SKIP" if np.isnan(s) else s
            data.append([frame_index[idx], timestamp[idx], idx, idx + 1, s, a1, a2, isKeyFrame[idx], description[idx]])

        with open(output_avg_csv, 'w', newline='') as csvFile:
            writer = csv.writer(csvFile)
            writer.writerow(
                ['frame', 'timestamp', 'Line1', 'Line2', 'Similarity', 'avgone', 'avgtwo', 'iskeyFrame', 'description'])
            writer.writerows(data)

        self.logger.info(f"Generated average output CSV: {output_avg_csv}")


if __name__ == "__main__":
    # For testing purposes
    video_runner_obj = {
        "video_id": "test_video",
        "logger": print  # Use print as a simple logger for testing
    }
    scene_segmentation = SceneSegmentation(video_runner_obj)
    scene_segmentation.run_scene_segmentation()