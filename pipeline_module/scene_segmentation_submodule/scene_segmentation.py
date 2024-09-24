import csv
import json
import numpy as np
from typing import Dict, Any, List
from ..utils_module.utils import OUTPUT_AVG_CSV, SCENE_SEGMENTED_FILE_CSV, return_video_folder_name
from web_server_module.web_server_database import update_status, get_status_for_youtube_id, update_module_output
from ..utils_module.timeit_decorator import timeit
from sklearn.cluster import KMeans
from scipy.signal import find_peaks

class SceneSegmentation:
    def __init__(self, video_runner_obj: Dict[str, Any]):
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        self.columns = {
            "start_time": "start_time",
            "end_time": "end_time",
            "description": "description",
        }
        self.min_scene_duration = 3  # minimum scene duration in seconds
        self.max_scenes = 50  # maximum number of scenes to detect

    @timeit
    def run_scene_segmentation(self) -> bool:
        self.logger.info("Running scene segmentation")

        # Check if scene segmentation has already been completed
        if get_status_for_youtube_id(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"]) == "done":
            self.logger.info("Scene segmentation already processed")
            return True

        try:
            frame_data = self.load_frame_data()
            scene_boundaries = self.detect_scene_boundaries(frame_data)
            scenes = self.generate_scenes(frame_data, scene_boundaries)
            self.save_scenes(scenes)

            # Mark task as done in the database
            update_status(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"], "done")

            # Save the segmented scenes to the database for future use
            update_module_output(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"], 'scene_segmentation', {"scenes": scenes})

            self.logger.info("Scene segmentation completed")
            return True
        except Exception as e:
            self.logger.error(f"Error in scene segmentation: {str(e)}")
            return False

    def load_frame_data(self) -> List[Dict[str, Any]]:
        output_avg_csv = return_video_folder_name(self.video_runner_obj) + '/' + OUTPUT_AVG_CSV
        frame_data = []

        with open(output_avg_csv, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                frame_data.append({
                    'frame': int(row['frame']),
                    'timestamp': float(row['timestamp']),
                    'similarity': float(row['Similarity']) if row['Similarity'] != 'SKIP' else np.nan,
                    'description': row['description']
                })

        return frame_data

    def detect_scene_boundaries(self, frame_data: List[Dict[str, Any]]) -> List[int]:
        similarities = [frame['similarity'] for frame in frame_data]
        similarities = np.array(similarities)
        similarities[np.isnan(similarities)] = np.nanmean(similarities)

        # Use both threshold-based and peak detection methods
        threshold_boundaries = self.threshold_based_detection(similarities)
        peak_boundaries = self.peak_based_detection(similarities)

        all_boundaries = sorted(set(threshold_boundaries + peak_boundaries))

        filtered_boundaries = self.filter_boundaries(all_boundaries, frame_data)

        return filtered_boundaries

    def threshold_based_detection(self, similarities: np.ndarray) -> List[int]:
        threshold = np.mean(similarities) - np.std(similarities)
        return [i for i in range(1, len(similarities)) if similarities[i] < threshold]

    def peak_based_detection(self, similarities: np.ndarray) -> List[int]:
        inverted_similarities = np.max(similarities) - similarities
        peaks, _ = find_peaks(inverted_similarities, distance=self.min_scene_duration * 30)
        return list(peaks)

    def filter_boundaries(self, boundaries: List[int], frame_data: List[Dict[str, Any]]) -> List[int]:
        filtered = [0]  # Always include the start of the video
        for b in boundaries:
            if (frame_data[b]['timestamp'] - frame_data[filtered[-1]]['timestamp']) >= self.min_scene_duration:
                filtered.append(b)
            if len(filtered) >= self.max_scenes:
                break
        return filtered

    def generate_scenes(self, frame_data: List[Dict[str, Any]], scene_boundaries: List[int]) -> List[Dict[str, Any]]:
        scenes = []
        for i in range(len(scene_boundaries) - 1):
            start = scene_boundaries[i]
            end = scene_boundaries[i + 1]
            scene = {
                'start_time': frame_data[start]['timestamp'],
                'end_time': frame_data[end]['timestamp'],
                'description': self.summarize_scene_description(frame_data[start:end])
            }
            scenes.append(scene)

        if scene_boundaries:
            last_start = scene_boundaries[-1]
            scenes.append({
                'start_time': frame_data[last_start]['timestamp'],
                'end_time': frame_data[-1]['timestamp'],
                'description': self.summarize_scene_description(frame_data[last_start:])
            })

        return scenes

    def summarize_scene_description(self, scene_frames: List[Dict[str, Any]]) -> str:
        descriptions = [frame['description'] for frame in scene_frames if frame['description']]
        if not descriptions:
            return "No description available"

        vectorizer = self.get_vectorizer()
        vectors = vectorizer.fit_transform(descriptions)

        n_clusters = min(3, len(descriptions))
        kmeans = KMeans(n_clusters=n_clusters, random_state=42)
        kmeans.fit(vectors)

        cluster_centers = kmeans.cluster_centers_
        closest_descriptions = []

        for center in cluster_centers:
            distances = np.linalg.norm(vectors - center, axis=1)
            closest_idx = np.argmin(distances)
            closest_descriptions.append(descriptions[closest_idx])

        return " ".join(closest_descriptions)

    def get_vectorizer(self):
        from sklearn.feature_extraction.text import TfidfVectorizer
        return TfidfVectorizer(stop_words='english')

    def save_scenes(self, scenes: List[Dict[str, Any]]) -> None:
        scene_segmented_file = return_video_folder_name(self.video_runner_obj) + "/" + SCENE_SEGMENTED_FILE_CSV
        with open(scene_segmented_file, "w", newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.columns.values())
            writer.writeheader()
            writer.writerows(scenes)

        self.logger.info(f"Scene segmentation results saved to {scene_segmented_file}")