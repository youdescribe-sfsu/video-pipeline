import cv2
import os
import numpy as np
from typing import Dict, List, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from web_server_module.web_server_database import get_status_for_youtube_id, update_status, update_module_output
from ..utils_module.utils import return_video_download_location, return_video_frames_folder
from ..utils_module.timeit_decorator import timeit

class FrameExtraction:
    def __init__(self, video_runner_obj: Dict[str, Any], config: Dict[str, Any] = None):
        self.video_runner_obj = video_runner_obj
        self.config = config or {}
        self.logger = video_runner_obj.get("logger")
        self.video_path = return_video_download_location(self.video_runner_obj)
        self.frames_folder = return_video_frames_folder(self.video_runner_obj)
        self.logger.info(f"Initialization complete. Video path: {self.video_path}, Frames folder: {self.frames_folder}")

    @timeit
    def extract_frames(self) -> bool:
        if self._is_extraction_complete():
            return True

        if not self._create_frames_folder():
            return False

        try:
            vid, total_frames, video_fps, duration = self._get_video_info()
            adaptive_fps = self.calculate_adaptive_fps(duration)
            frames_to_extract = int(duration * adaptive_fps)
            step = max(1, int(video_fps / adaptive_fps))

            self.logger.info(f"Extracting frames at {adaptive_fps} fps, step size: {step}")
            self.logger.info(f"Total frames to extract: {frames_to_extract}")

            frame_indices = np.arange(0, total_frames, step)
            scene_changes = self.detect_scene_changes(vid, threshold=self.config.get('scene_threshold', 30.0))

            self._extract_frames_parallel(frame_indices, scene_changes)
            self._save_extraction_progress(adaptive_fps, frames_to_extract, step, scene_changes)

            self.logger.info("Frame extraction completed successfully.")
            return True

        except Exception as e:
            self.logger.error(f"Error in frame extraction: {str(e)}")
            return False

    def _is_extraction_complete(self) -> bool:
        if get_status_for_youtube_id(self.video_runner_obj.get("video_id"), self.video_runner_obj.get("AI_USER_ID")) == "done":
            self.logger.info("Frames already extracted, skipping step.")
            return True
        return False

    def _create_frames_folder(self) -> bool:
        try:
            os.makedirs(self.frames_folder, exist_ok=True)
            return True
        except OSError as e:
            self.logger.error(f"Error creating frames folder: {str(e)}")
            return False

    def _get_video_info(self) -> tuple:
        vid = cv2.VideoCapture(self.video_path)
        if not vid.isOpened():
            raise IOError(f"Error opening video file: {self.video_path}")

        total_frames = int(vid.get(cv2.CAP_PROP_FRAME_COUNT))
        video_fps = vid.get(cv2.CAP_PROP_FPS)
        duration = total_frames / video_fps
        self.logger.info(f"Video info: total frames={total_frames}, fps={video_fps}, duration={duration}")
        return vid, total_frames, video_fps, duration

    def _extract_frames_parallel(self, frame_indices: np.ndarray, scene_changes: List[int]) -> None:
        with ThreadPoolExecutor(max_workers=self.config.get('max_workers', os.cpu_count())) as executor:
            futures = [executor.submit(self.process_frame, frame_idx, frame_idx in scene_changes)
                       for frame_idx in frame_indices]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    self.logger.error(f"Frame processing generated an exception: {exc}")

    def _save_extraction_progress(self, adaptive_fps: float, frames_extracted: int, step: int, scene_changes: List[int]) -> None:
        update_status(self.video_runner_obj.get("video_id"), self.video_runner_obj.get("AI_USER_ID"), "done")
        module_outputs = {
            'adaptive_fps': adaptive_fps,
            'frames_extracted': frames_extracted,
            'steps': step,
            'scene_changes': scene_changes
        }
        update_module_output(self.video_runner_obj.get("video_id"), self.video_runner_obj.get("AI_USER_ID"),
                             'frame_extraction', module_outputs)
        self.logger.info("Frame extraction progress and outputs saved in the database.")

    def process_frame(self, frame_idx: int, is_scene_change: bool) -> None:
        vid = cv2.VideoCapture(self.video_path)
        vid.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
        ret, frame = vid.read()
        vid.release()

        if ret:
            frame_type = "keyframe" if is_scene_change else "frame"
            frame_filename = os.path.join(self.frames_folder, f"{frame_type}_{frame_idx}.jpg")
            cv2.imwrite(frame_filename, frame)
            self.logger.info(f"Processed {frame_type} {frame_idx}")
        else:
            self.logger.warning(f"Failed to read frame {frame_idx}")

    def calculate_adaptive_fps(self, duration: float) -> float:
        base_fps = self.config.get('base_fps', 3)
        if duration <= 60:  # For videos up to 1 minute
            return max(base_fps, 1)
        elif duration <= 300:  # For videos up to 5 minutes
            return max(base_fps - 1, 1)
        elif duration <= 900:  # For videos up to 15 minutes
            return max(base_fps - 2, 1)
        else:  # For videos longer than 15 minutes
            return max(1, min(base_fps - 3, int(duration / 300)))

    def detect_scene_changes(self, vid: cv2.VideoCapture, threshold: float = 30.0) -> List[int]:
        scene_changes = []
        previous_frame = None
        frame_count = 0

        while True:
            ret, frame = vid.read()
            if not ret:
                break

            if previous_frame is not None:
                diff = cv2.absdiff(cv2.cvtColor(previous_frame, cv2.COLOR_BGR2GRAY),
                                   cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY))
                non_zero_count = np.count_nonzero(diff)
                if non_zero_count > threshold * frame.size / 100:
                    scene_changes.append(frame_count)

            previous_frame = frame
            frame_count += 1

        vid.set(cv2.CAP_PROP_POS_FRAMES, 0)  # Reset video to start
        return scene_changes