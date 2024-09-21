import csv
from ..utils_module.utils import read_value_from_file, return_video_frames_folder, \
    return_video_folder_name, save_value_to_file
from ..utils_module.timeit_decorator import timeit
from ..utils_module.utils import FRAME_INDEX_SELECTOR, KEY_FRAME_HEADERS, KEYFRAMES_CSV, TIMESTAMP_SELECTOR, OBJECTS_CSV

class KeyframeSelection:
    def __init__(self, video_runner_obj, target_keyframes_per_second=1):
        print("Initializing KeyframeSelection")
        self.video_runner_obj = video_runner_obj
        self.target_keyframes_per_second = target_keyframes_per_second
        self.logger = video_runner_obj.get("logger")
        print(f"Initialization complete. Target keyframes per second: {target_keyframes_per_second}")

    @timeit
    def run_keyframe_selection(self):
        print("Starting run_keyframe_selection method")
        self.logger.info(f"Running keyframe selection for {self.video_runner_obj['video_id']}")

        try:
            print("Saving initial progress")
            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['KeyframeSelection']['started']",
                               value=str(True))
        except Exception as e:
            error_msg = f"Error saving KeyframeSelection progress: {str(e)}"
            print(error_msg)
            self.logger.error(error_msg)

        print("Checking if keyframe selection was already done")
        if read_value_from_file(video_runner_obj=self.video_runner_obj,
                                key="['KeyframeSelection']['started']") == 'done':
            self.logger.info("Keyframe selection already done, skipping step.")
            print("Keyframe selection already done, skipping step.")
            return True

        print("Reading video common values")
        step = read_value_from_file(video_runner_obj=self.video_runner_obj, key="['video_common_values']['step']")
        num_frames = read_value_from_file(video_runner_obj=self.video_runner_obj,
                                          key="['video_common_values']['num_frames']")
        frames_per_second = read_value_from_file(video_runner_obj=self.video_runner_obj,
                                                 key="['video_common_values']['frames_per_second']")

        print(f"Raw video common values: step={step}, num_frames={num_frames}, frames_per_second={frames_per_second}")
        self.logger.info(
            f"Raw video common values: step={step}, num_frames={num_frames}, frames_per_second={frames_per_second}")

        try:
            # Convert values to appropriate types
            step = int(float(step)) if step is not None else None
            num_frames = int(float(num_frames)) if num_frames is not None else None
            frames_per_second = float(frames_per_second) if frames_per_second is not None else None
        except ValueError as e:
            error_msg = f"Error converting video common values: {str(e)}"
            print(error_msg)
            self.logger.error(error_msg)
            return False

        print(
            f"Converted video common values: step={step}, num_frames={num_frames}, frames_per_second={frames_per_second}")
        self.logger.info(
            f"Converted video common values: step={step}, num_frames={num_frames}, frames_per_second={frames_per_second}")

        if None in (step, num_frames, frames_per_second):
            error_msg = f"Invalid video common values: step={step}, num_frames={num_frames}, frames_per_second={frames_per_second}"
            print(error_msg)
            self.logger.error(error_msg)

            # Add additional logging to help diagnose the issue
            self.logger.error("Dumping all video_common_values:")
            all_common_values = read_value_from_file(video_runner_obj=self.video_runner_obj,
                                                     key="['video_common_values']")
            self.logger.error(str(all_common_values))

            return False

        # ... rest of the method ...