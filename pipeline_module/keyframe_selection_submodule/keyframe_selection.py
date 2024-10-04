import csv
from web_server_module.web_server_database import get_status_for_youtube_id, update_status, update_module_output, get_module_output
from ..utils_module.utils import return_video_frames_folder, return_video_folder_name, FRAME_INDEX_SELECTOR, \
    KEY_FRAME_HEADERS, KEYFRAMES_CSV, TIMESTAMP_SELECTOR, OBJECTS_CSV, return_video_download_location
from ..utils_module.timeit_decorator import timeit
import os
import cv2
import numpy as np

class KeyframeSelection:
    def __init__(self, video_runner_obj, target_keyframes_per_second=1):
        print("Initializing KeyframeSelection")
        self.video_runner_obj = video_runner_obj
        self.target_keyframes_per_second = target_keyframes_per_second
        self.logger = video_runner_obj.get("logger")
        print(f"Initialization complete. Target keyframes per second: {target_keyframes_per_second}")

    @timeit
    def run_keyframe_selection(self):
        try:
            print("Starting run_keyframe_selection method")
            self.logger.info(f"Running keyframe selection for {self.video_runner_obj['video_id']}")

            if get_status_for_youtube_id(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"]) == "done":
                self.logger.info("Keyframe selection already done, skipping step.")
                print("Keyframe selection already done, skipping step.")
                return True

            print("Reading video common values from the database")
            video_common_values = self.load_video_common_values()
            if video_common_values is None:
                return False

            step, num_frames, frames_per_second = video_common_values

            print(f"Running keyframe selection logic with step={step}, num_frames={num_frames}, fps={frames_per_second}")
            self.logger.info(f"Running keyframe selection with step={step}, num_frames={num_frames}, fps={frames_per_second}")

            keyframes_data = self.select_keyframes(step, num_frames, frames_per_second)
            self.save_keyframes_to_csv(keyframes_data)

            update_module_output(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"], 'keyframe_selection', {"keyframes": keyframes_data})

            update_status(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"], "done")

            self.logger.info("Keyframe selection completed successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error in keyframe selection: {str(e)}")
            return False

    def load_video_common_values(self):
        try:
            previous_outputs = get_module_output(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"], 'frame_extraction')
            if not previous_outputs:
                raise ValueError("No video common values found from frame extraction")

            step = int(float(previous_outputs['steps']))
            num_frames = int(float(previous_outputs['frames_extracted']))
            frames_per_second = float(previous_outputs['adaptive_fps'])

            return step, num_frames, frames_per_second
        except (ValueError, KeyError, TypeError) as e:
            error_msg = f"Error retrieving video common values: {str(e)}"
            print(error_msg)
            self.logger.error(error_msg)
            return None

    def select_keyframes(self, step, num_frames, frames_per_second):
        keyframes_data = []
        for i in range(0, num_frames, step):
            timestamp = i / frames_per_second
            is_keyframe = self.detect_scene_changes(i)
            keyframes_data.append({
                'frame_index': i,
                'timestamp': timestamp,
                'is_keyframe': is_keyframe
            })
        return keyframes_data

    def detect_scene_changes(self, frame_idx: int) -> bool:
        vid = cv2.VideoCapture(return_video_download_location(self.video_runner_obj))
        vid.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
        ret, current_frame = vid.read()
        if not ret:
            return False

        vid.set(cv2.CAP_PROP_POS_FRAMES, frame_idx - 1)
        ret, previous_frame = vid.read()
        vid.release()

        if not ret or previous_frame is None:
            return False

        diff = cv2.absdiff(previous_frame, current_frame)
        non_zero_count = np.count_nonzero(diff)
        return non_zero_count > (30.0 * current_frame.size / 100)

    def save_keyframes_to_csv(self, keyframes_data):
        output_file = os.path.join(return_video_folder_name(self.video_runner_obj), KEYFRAMES_CSV)
        print(f"Saving keyframe results to {output_file}")
        self.logger.info(f"Saving keyframe results to {output_file}")

        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=[FRAME_INDEX_SELECTOR, TIMESTAMP_SELECTOR, 'is_keyframe'])
            writer.writeheader()
            for keyframe in keyframes_data:
                writer.writerow(keyframe)
        print(f"Keyframe selection results saved to {output_file}")
        self.logger.info(f"Keyframe selection results saved to {output_file}")