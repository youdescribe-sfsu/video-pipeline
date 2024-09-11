import cv2
import os
import numpy as np
from typing import Dict, List, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from ..utils_module.utils import read_value_from_file, return_video_download_location, return_video_frames_folder, save_value_to_file
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
            frames_to_extract = int(duration * adaptive_fps)

            print(f"Extracting frames at {adaptive_fps} fps")
            print(f"Total frames to extract: {frames_to_extract}")
            self.logger.info(f"Extracting frames at {adaptive_fps} fps")
            self.logger.info(f"Total frames to extract: {frames_to_extract}")

            frame_indices = np.linspace(0, total_frames - 1, frames_to_extract, dtype=int)

            self._extract_frames_parallel(frame_indices)

            self._save_extraction_progress(adaptive_fps, frames_to_extract)
            self.set_video_common_values(adaptive_fps, frames_to_extract, video_fps)

            print("Frame extraction completed successfully.")
            self.logger.info("Frame extraction completed successfully.")
            return True

        except Exception as e:
            print(f"Error in frame extraction: {str(e)}")
            self.logger.error(f"Error in frame extraction: {str(e)}")
            return False

    def _is_extraction_complete(self) -> bool:
        if read_value_from_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['started']") == 'done':
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

    def _save_extraction_progress(self, adaptive_fps: float, frames_extracted: int) -> None:
        print("Frame extraction completed, saving progress")
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['started']", value='done')
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['adaptive_fps']", value=str(adaptive_fps))
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['frames_extracted']", value=frames_extracted)

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
            return max(self.default_fps, 1)
        elif duration <= 300:  # For videos up to 5 minutes
            return max(self.default_fps - 1, 1)
        elif duration <= 900:  # For videos up to 15 minutes
            return max(self.default_fps - 2, 1)
        else:  # For videos longer than 15 minutes
            return max(1, min(self.default_fps - 3, int(duration / 300)))

    @timeit
    def extract_frames_with_scene_detection(self) -> bool:
        if read_value_from_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['scene_detection_done']") == 'done':
            self.logger.info("Scene detection and keyframe extraction already completed, skipping step.")
            return True

        try:
            self.logger.info("Starting scene detection")
            scene_changes = self.detect_scene_changes()
            self.logger.info(f"Detected {len(scene_changes)} scene changes")

            self.logger.info("Extracting keyframes")
            self.extract_keyframes(scene_changes)

            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['scene_detection_done']", value='done')
            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['scene_changes']", value=scene_changes)

            self.logger.info("Scene detection and keyframe extraction completed successfully.")
            return True

        except Exception as e:
            self.logger.error(f"Error in scene detection and keyframe extraction: {str(e)}")
            return False

    def detect_scene_changes(self, threshold: float = 30.0) -> List[int]:
        scene_changes = []
        previous_frame = None

        vid = cv2.VideoCapture(self.video_path)

        while True:
            ret, frame = vid.read()
            if not ret:
                break

            if previous_frame is not None:
                diff = cv2.absdiff(previous_frame, frame)
                non_zero_count = np.count_nonzero(diff)
                if non_zero_count > threshold * frame.size / 100:
                    scene_changes.append(int(vid.get(cv2.CAP_PROP_POS_FRAMES)))

            previous_frame = frame

        vid.release()
        return scene_changes

    def extract_keyframes(self, scene_changes: List[int]) -> None:
        vid = cv2.VideoCapture(self.video_path)

        for frame_idx in scene_changes:
            vid.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
            ret, frame = vid.read()
            if ret:
                frame_filename = os.path.join(self.frames_folder, f"keyframe_{frame_idx}.jpg")
                cv2.imwrite(frame_filename, frame)
            else:
                self.logger.warning(f"Failed to read keyframe {frame_idx}")

        vid.release()

    def set_video_common_values(self, adaptive_fps: float, frames_extracted: int, video_fps: float) -> None:
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['video_common_values']['step']", value=str(video_fps / adaptive_fps))
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['video_common_values']['num_frames']", value=str(frames_extracted))
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['video_common_values']['frames_per_second']", value=str(adaptive_fps))

        print(f"Set video common values: step={int(video_fps / adaptive_fps)}, num_frames={frames_extracted}, frames_per_second={adaptive_fps}")
        self.logger.info(f"Set video common values: step={int(video_fps / adaptive_fps)}, num_frames={frames_extracted}, frames_per_second={adaptive_fps}")

if __name__ == "__main__":
    print("Running FrameExtraction as main")
    # For testing purposes
    video_runner_obj = {
        "video_id": "test_video",
        "logger": print  # Use print as a simple logger for testing
    }
    frame_extractor = FrameExtraction(video_runner_obj)
    success = frame_extractor.extract_frames()
    print(f"Frame extraction {'succeeded' if success else 'failed'}")

    scene_detection_success = frame_extractor.extract_frames_with_scene_detection()
    print(f"Scene detection and keyframe extraction {'succeeded' if scene_detection_success else 'failed'}")