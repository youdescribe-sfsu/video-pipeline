import cv2
import numpy as np
import os
import csv
from typing import List, Dict, Any
from web_server_module.web_server_database import get_status_for_youtube_id, update_status, update_module_output, \
    get_module_output
from ..utils_module.utils import return_video_frames_folder, return_video_folder_name, KEYFRAMES_CSV
from ..utils_module.timeit_decorator import timeit
from scipy.spatial.distance import cosine


class KeyframeSelection:
    def __init__(self, video_runner_obj: Dict[str, Any], config: Dict[str, Any] = None):
        self.video_runner_obj = video_runner_obj
        self.config = config or {}
        self.logger = video_runner_obj.get("logger")
        self.frames_folder = return_video_frames_folder(video_runner_obj)
        self.logger.info(f"Initialization complete. Frames folder: {self.frames_folder}")

    @timeit
    def run_keyframe_selection(self):
        try:
            self.logger.info(f"Running keyframe selection for {self.video_runner_obj['video_id']}")

            if self._is_selection_complete():
                return True

            video_info = self._load_video_info()
            if video_info is None:
                return False

            step, num_frames, frames_per_second, scene_changes = video_info

            self.logger.info(
                f"Running keyframe selection with step={step}, num_frames={num_frames}, fps={frames_per_second}")

            keyframes_data = self._select_keyframes(step, num_frames, frames_per_second, scene_changes)
            self._save_keyframes_to_csv(keyframes_data)
            self._save_results_to_database(keyframes_data)

            self.logger.info("Keyframe selection completed successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error in keyframe selection: {str(e)}")
            return False

    def _is_selection_complete(self) -> bool:
        if get_status_for_youtube_id(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"]) == "done":
            self.logger.info("Keyframe selection already done, skipping step.")
            return True
        return False

    def _load_video_info(self):
        try:
            previous_outputs = get_module_output(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"],
                                                 'frame_extraction')
            if not previous_outputs:
                raise ValueError("No video info found from frame extraction")

            step = int(float(previous_outputs['steps']))
            num_frames = int(float(previous_outputs['frames_extracted']))
            frames_per_second = float(previous_outputs['adaptive_fps'])
            scene_changes = previous_outputs.get('scene_changes', [])

            return step, num_frames, frames_per_second, scene_changes
        except Exception as e:
            self.logger.error(f"Error retrieving video info: {str(e)}")
            return None

    def _select_keyframes(self, step: int, num_frames: int, frames_per_second: float, scene_changes: List[int]) -> List[
        Dict[str, Any]]:
        keyframes_data = []
        previous_features = None

        for i in range(0, num_frames, step):
            frame_path = os.path.join(self.frames_folder, f"frame_{i}.jpg")
            if not os.path.exists(frame_path):
                continue

            frame = cv2.imread(frame_path)
            features = self._extract_features(frame)

            is_keyframe = False
            if previous_features is not None:
                diff = self._compute_difference(previous_features, features)
                is_keyframe = self._is_keyframe(diff, i, num_frames) or (i in scene_changes)
            elif i == 0:  # Always include the first frame
                is_keyframe = True

            if is_keyframe:
                keyframes_data.append({
                    'frame_index': i,
                    'timestamp': i / frames_per_second,
                    'is_keyframe': True
                })

            previous_features = features

        return keyframes_data

    def _extract_features(self, frame: np.ndarray) -> np.ndarray:
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        hist = cv2.calcHist([gray], [0], None, [256], [0, 256])
        cv2.normalize(hist, hist)
        return hist.flatten()

    def _compute_difference(self, feat1: np.ndarray, feat2: np.ndarray) -> float:
        return cosine(feat1, feat2)

    def _is_keyframe(self, diff: float, frame_index: int, total_frames: int) -> bool:
        base_threshold = self.config.get('base_threshold', 0.5)
        adaptive_factor = self.config.get('adaptive_factor', 0.1)

        if frame_index < total_frames * 0.1 or frame_index > total_frames * 0.9:
            threshold = base_threshold * (1 - adaptive_factor)
        else:
            threshold = base_threshold

        return diff > threshold

    def _save_keyframes_to_csv(self, keyframes_data: List[Dict[str, Any]]) -> None:
        output_file = os.path.join(return_video_folder_name(self.video_runner_obj), KEYFRAMES_CSV)
        self.logger.info(f"Saving keyframe results to {output_file}")

        with open(output_file, 'w', newline='') as csvfile:
            fieldnames = ['frame_index', 'timestamp', 'is_keyframe']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for keyframe in keyframes_data:
                writer.writerow(keyframe)
        self.logger.info(f"Keyframe selection results saved to {output_file}")

    def _save_results_to_database(self, keyframes_data: List[Dict[str, Any]]) -> None:
        update_module_output(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"],
                             'keyframe_selection', {"keyframes": keyframes_data})
        update_status(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"], "done")
        self.logger.info("Keyframe selection results saved to database")

    @staticmethod
    def detect_scene_change(frame1: np.ndarray, frame2: np.ndarray, threshold: float = 30.0) -> bool:
        diff = cv2.absdiff(cv2.cvtColor(frame1, cv2.COLOR_BGR2GRAY),
                           cv2.cvtColor(frame2, cv2.COLOR_BGR2GRAY))
        non_zero_count = np.count_nonzero(diff)
        return non_zero_count > threshold * frame1.size / 100