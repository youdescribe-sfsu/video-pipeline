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
            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['KeyframeSelection']['started']", value=True)
        except Exception as e:
            error_msg = f"Error saving KeyframeSelection progress: {str(e)}"
            print(error_msg)
            self.logger.error(error_msg)

        print("Checking if keyframe selection was already done")
        if read_value_from_file(video_runner_obj=self.video_runner_obj, key="['KeyframeSelection']['started']") == 'done':
            self.logger.info("Keyframe selection already done, skipping step.")
            print("Keyframe selection already done, skipping step.")
            return True

        print("Reading video common values")
        step = read_value_from_file(video_runner_obj=self.video_runner_obj, key="['video_common_values']['step']")
        num_frames = read_value_from_file(video_runner_obj=self.video_runner_obj, key="['video_common_values']['num_frames']")
        frames_per_second = read_value_from_file(video_runner_obj=self.video_runner_obj, key="['video_common_values']['frames_per_second']")
        print(f"Video common values: step={step}, num_frames={num_frames}, frames_per_second={frames_per_second}")

        if None in (step, num_frames, frames_per_second):
            error_msg = f"Invalid video common values: step={step}, num_frames={num_frames}, frames_per_second={frames_per_second}"
            print(error_msg)
            self.logger.error(error_msg)
            return False

        incsvpath = return_video_folder_name(self.video_runner_obj) + "/" + OBJECTS_CSV
        print(f"Reading object detection results from {incsvpath}")
        self.logger.info(f"Reading object detection results from {incsvpath}")

        try:
            print("Processing object detection results")
            with open(incsvpath, newline='', encoding='utf-8') as incsvfile:
                reader = csv.reader(incsvfile)
                header = next(reader)  # skip header
                rows = [row for row in reader]

            frame_values = []
            for row in rows:
                frame_index = int(row[0])
                weights = [float(x) for x in row[1::2] if x != '']
                value = sum([x * x for x in weights])
                frame_values.append((frame_index, value))
            print(f"Processed {len(frame_values)} frames")

            print("Calculating keyframes")
            video_fps = step * frames_per_second
            frames_per_target_period = video_fps / self.target_keyframes_per_second
            keyframes = []
            last_keyframe = -step
            for (index, value) in frame_values:
                if index - last_keyframe > 2 * frames_per_target_period or index + step >= num_frames:
                    window = frame_values[last_keyframe // step + 1:index // step]
                    width = index - last_keyframe
                    a = -4.0 / (width * width)
                    b = 4.0 / width
                    best = -1
                    best_val = -1.0
                    for (index_w, value_w) in window:
                        rel_index = index_w - last_keyframe
                        coeff = a * rel_index * rel_index + b * rel_index
                        modified_value = coeff * value_w
                        if modified_value >= best_val:
                            best = index_w
                            best_val = modified_value
                    keyframes.append(best)
                    last_keyframe = best
            print(f"Selected {len(keyframes)} keyframes")

            outcsvpath = return_video_folder_name(self.video_runner_obj) + "/" + KEYFRAMES_CSV
            print(f"Writing keyframe selection results to {outcsvpath}")
            self.logger.info(f"Writing keyframe selection results to {KEYFRAMES_CSV}")

            seconds_per_frame = 1.0 / video_fps
            with open(outcsvpath, 'w', newline='', encoding='utf-8') as outcsvfile:
                writer = csv.writer(outcsvfile)
                writer.writerow([KEY_FRAME_HEADERS[FRAME_INDEX_SELECTOR], KEY_FRAME_HEADERS[TIMESTAMP_SELECTOR]])
                for frame_index in keyframes:
                    new_row = [frame_index, float(frame_index) * seconds_per_frame]
                    self.logger.info(f"Frame Index: {frame_index} Timestamp: {float(frame_index) * seconds_per_frame}")
                    writer.writerow(new_row)

            print("Keyframe selection complete")
            self.logger.info(f"Keyframe selection complete for {self.video_runner_obj['video_id']}")
            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['KeyframeSelection']['started']", value='done')
            return True

        except Exception as e:
            error_msg = f"Error occurred in keyframe selection: {str(e)}"
            print(error_msg)
            self.logger.error(error_msg, exc_info=True)
            return False