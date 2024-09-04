import cv2
import os
import numpy as np
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from ..utils_module.utils import read_value_from_file, return_video_download_location, return_video_frames_folder, \
    save_value_to_file
from ..utils_module.timeit_decorator import timeit

class FrameExtraction:
    def __init__(self, video_runner_obj: Dict[str, int], default_fps: int = 3):
        self.video_runner_obj = video_runner_obj
        self.default_fps = default_fps
        self.logger = video_runner_obj.get("logger")
        self.video_path = return_video_download_location(self.video_runner_obj)
        self.frames_folder = return_video_frames_folder(self.video_runner_obj)

    @timeit
    def extract_frames(self) -> bool:
        if read_value_from_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['started']") == 'done':
            self.logger.info("Frames already extracted, skipping step.")
            return True

        if not os.path.exists(self.frames_folder):
            os.makedirs(self.frames_folder)

        try:
            vid = cv2.VideoCapture(self.video_path)
            if not vid.isOpened():
                raise IOError(f"Error opening video file: {self.video_path}")

            total_frames = int(vid.get(cv2.CAP_PROP_FRAME_COUNT))
            video_fps = vid.get(cv2.CAP_PROP_FPS)
            duration = total_frames / video_fps
            vid.release()  # Release the main VideoCapture object

            adaptive_fps = self.calculate_adaptive_fps(duration)
            frames_to_extract = int(duration * adaptive_fps)

            self.logger.info(f"Extracting frames at {adaptive_fps} fps")
            self.logger.info(f"Total frames to extract: {frames_to_extract}")

            frame_indices = np.linspace(0, total_frames - 1, frames_to_extract, dtype=int)

            with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
                futures = [executor.submit(self.process_frame, frame_idx) for frame_idx in frame_indices]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as exc:
                        self.logger.error(f"Frame processing generated an exception: {exc}")

            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['started']",
                               value='done')
            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['adaptive_fps']",
                               value=adaptive_fps)
            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['frames_extracted']",
                               value=frames_to_extract)

            self.logger.info("Frame extraction completed successfully.")
            return True

        except Exception as e:
            self.logger.error(f"Error in frame extraction: {str(e)}")
            return False

    def process_frame(self, frame_idx: int) -> None:
        vid = cv2.VideoCapture(self.video_path)  # Create a new VideoCapture object for each thread
        vid.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
        ret, frame = vid.read()
        vid.release()  # Release the VideoCapture object immediately after use

        if ret:
            frame_filename = os.path.join(self.frames_folder, f"frame_{frame_idx}.jpg")
            cv2.imwrite(frame_filename, frame)
            self.logger.info(f"Processed frame {frame_idx}")
        else:
            self.logger.warning(f"Failed to read frame {frame_idx}")

    def calculate_adaptive_fps(self, duration: float) -> float:
        if duration <= 60:  # For videos up to 1 minute
            return max(self.default_fps, 1)
        elif duration <= 300:  # For videos up to 5 minutes
            return max(self.default_fps - 1, 1)
        elif duration <= 900:  # For videos up to 15 minutes
            return max(self.default_fps - 2, 1)
        else:  # For videos longer than 15 minutes
            return max(1, min(self.default_fps - 3, duration / 300))

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

    @timeit
    def extract_frames_with_scene_detection(self) -> bool:
        if read_value_from_file(video_runner_obj=self.video_runner_obj,
                                key="['FrameExtraction']['scene_detection_done']") == 'done':
            self.logger.info("Scene detection and keyframe extraction already completed, skipping step.")
            return True

        try:
            self.logger.info("Starting scene detection")
            scene_changes = self.detect_scene_changes()
            self.logger.info(f"Detected {len(scene_changes)} scene changes")

            self.logger.info("Extracting keyframes")
            self.extract_keyframes(scene_changes)

            save_value_to_file(video_runner_obj=self.video_runner_obj,
                               key="['FrameExtraction']['scene_detection_done']", value='done')
            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['scene_changes']",
                               value=scene_changes)

            self.logger.info("Scene detection and keyframe extraction completed successfully.")
            return True

        except Exception as e:
            self.logger.error(f"Error in scene detection and keyframe extraction: {str(e)}")
            return False


if __name__ == "__main__":
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