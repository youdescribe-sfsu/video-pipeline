import cv2
import os
import numpy as np
from typing import Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from web_server_module.web_server_database import get_status_for_youtube_id, update_status, update_module_output
from ..utils_module.utils import return_video_download_location, return_video_frames_folder
from ..utils_module.timeit_decorator import timeit

class FrameExtraction:
    def __init__(self, video_runner_obj: Dict[str, Any], default_fps: int = 3):
        print("Initializing FrameExtraction")
        self.video_runner_obj = video_runner_obj
        self.default_fps = default_fps
        self.logger = video_runner_obj.get("logger")
        self.video_path = return_video_download_location(self.video_runner_obj)
        self.frames_folder = return_video_frames_folder(self.video_runner_obj)
        print(f"Initialization complete. Video path: {self.video_path}, Frames folder: {self.frames_folder}")

    @timeit
    def extract_frames(self) -> bool:
        print("Starting extract_frames method")
        if self._is_extraction_complete():
            return True

        if not self._create_frames_folder():
            return False

        try:
            vid, total_frames, video_fps, duration = self._get_video_info()
            adaptive_fps = self.calculate_adaptive_fps(duration)
            step = max(5, int(video_fps / adaptive_fps))  # Ensure a minimum step size of 5

            print(f"Extracting frames at {adaptive_fps} fps with step size: {step}")
            self.logger.info(f"Extracting frames at {adaptive_fps} fps with step size: {step}")

            frame_indices = np.arange(0, total_frames, step)

            self._extract_frames_parallel(frame_indices)

            self._save_extraction_progress(adaptive_fps, len(frame_indices), step)

            print("Frame extraction completed successfully.")
            self.logger.info("Frame extraction completed successfully.")
            return True

        except Exception as e:
            print(f"Error in frame extraction: {str(e)}")
            self.logger.error(f"Error in frame extraction: {str(e)}")
            return False

    def _is_extraction_complete(self) -> bool:
        if get_status_for_youtube_id(self.video_runner_obj.get("video_id"), self.video_runner_obj.get("AI_USER_ID")) == "done":
            print("Frames already extracted, skipping step.")
            self.logger.info("Frames already extracted, skipping step.")
            return True
        return False

    def _create_frames_folder(self) -> bool:
        print(f"Creating frames folder: {self.frames_folder}")
        try:
            os.makedirs(self.frames_folder, exist_ok=True)
            return True
        except OSError as e:
            print(f"Error creating frames folder: {str(e)}")
            self.logger.error(f"Error creating frames folder: {str(e)}")
            return False

    def _get_video_info(self) -> tuple:
        print(f"Opening video file: {self.video_path}")
        vid = cv2.VideoCapture(self.video_path)
        if not vid.isOpened():
            raise IOError(f"Error opening video file: {self.video_path}")

        total_frames = int(vid.get(cv2.CAP_PROP_FRAME_COUNT))
        video_fps = vid.get(cv2.CAP_PROP_FPS)
        duration = total_frames / video_fps
        print(f"Video info: total frames={total_frames}, fps={video_fps}, duration={duration}")
        return vid, total_frames, video_fps, duration

    def _extract_frames_parallel(self, frame_indices: np.ndarray) -> None:
        print("Starting frame extraction with ThreadPoolExecutor")
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = [executor.submit(self.process_frame, frame_idx) for frame_idx in frame_indices]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    print(f"Frame processing generated an exception: {exc}")
                    self.logger.error(f"Frame processing generated an exception: {exc}")

    def _save_extraction_progress(self, adaptive_fps: float, frames_extracted: int, step: int) -> None:
        print("Frame extraction completed, saving progress and output values")
        update_status(self.video_runner_obj.get("video_id"), self.video_runner_obj.get("AI_USER_ID"), "done")

        module_outputs = {
            'adaptive_fps': adaptive_fps,
            'frames_extracted': frames_extracted,
            'steps': step
        }
        update_module_output(self.video_runner_obj.get("video_id"), self.video_runner_obj.get("AI_USER_ID"),
                             'frame_extraction', module_outputs)

        print(f"Frame extraction progress and outputs saved in the database.")

    def process_frame(self, frame_idx: int) -> None:
        print(f"Processing frame {frame_idx}")
        vid = cv2.VideoCapture(self.video_path)
        vid.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
        ret, frame = vid.read()
        vid.release()

        if ret:
            frame_filename = os.path.join(self.frames_folder, f"frame_{frame_idx}.jpg")
            cv2.imwrite(frame_filename, frame)
            print(f"Processed frame {frame_idx}")
            self.logger.info(f"Processed frame {frame_idx}")
        else:
            print(f"Failed to read frame {frame_idx}")
            self.logger.warning(f"Failed to read frame {frame_idx}")

    def calculate_adaptive_fps(self, duration: float) -> float:
        print(f"Calculating adaptive fps for duration: {duration}")
        if duration <= 60:  # For videos up to 1 minute
            return max(self.default_fps, 5)  # Minimum step size of 5
        elif duration <= 300:  # For videos up to 5 minutes
            return max(self.default_fps - 1, 5)
        elif duration <= 900:  # For videos up to 15 minutes
            return max(self.default_fps - 2, 5)
        else:  # For videos longer than 15 minutes
            return max(5, min(self.default_fps - 3, int(duration / 300)))
