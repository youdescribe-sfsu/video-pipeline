import csv
from utils import return_video_frames_folder,return_video_folder_name
from timeit_decorator import timeit
from utils import FRAME_INDEX_SELECTOR, KEY_FRAME_HEADERS,KEYFRAMES_CSV,KEYFRAMES_CSV,TIMESTAMP_SELECTOR,OBJECTS_CSV,KEYFRAMES_CSV


class KeyframeSelection:
    def __init__(self, video_runner_obj, target_keyframes_per_second=1):
        """
        Initialize ImportVideo object.
        
        Parameters:
        video_runner_obj (Dict[str, int]): A dictionary that contains the information of the video.
            The keys are "video_id", "video_start_time", and "video_end_time", and their values are integers.
        target_keyframes_per_second (int): The target number of keyframes per second.
        """
        self.video_runner_obj = video_runner_obj
        self.target_keyframes_per_second = target_keyframes_per_second
        pass

    @timeit
    def run_keyframe_selection(self):
        """
        Iteratively selects the keyframe that has the highest sum of square
        confidences and is reasonably close to 1/target_keyframes_per_second seconds
        after the previous keyframe
        """
        video_frames_path = return_video_frames_folder(self.video_runner_obj)
        with open('{}/data.txt'.format(video_frames_path), 'r') as datafile:
            data = datafile.readline().split()
            step = int(data[0])
            num_frames = int(data[1])
            frames_per_second = float(data[2])
        
        incsvpath = return_video_folder_name(self.video_runner_obj)+ "/" + OBJECTS_CSV
        with open(incsvpath, newline='', encoding='utf-8') as incsvfile:
            reader = csv.reader(incsvfile)
            header = next(reader) # skip header
            rows = [row for row in reader]
        
        frame_values = []
        for row in rows:
            frame_index = int(row[0])
            weights = [float(x) for x in row[1::2] if x != '']
            value = sum([x*x for x in weights])
            frame_values.append((frame_index, value))
        
        video_fps = step * frames_per_second
        frames_per_target_period = video_fps / self.target_keyframes_per_second
        keyframes = []
        last_keyframe = -step
        for (index, value) in frame_values:
            if index - last_keyframe > 2*frames_per_target_period or index + step >= num_frames:
                window = frame_values[last_keyframe//step + 1:index//step]
                width = index - last_keyframe
                a = -4.0/(width*width)
                b = 4.0/width
                best = -1
                best_val = -1.0
                for (index_w, value_w) in window:
                    rel_index = index_w - last_keyframe
                    coeff = a*rel_index*rel_index + b*rel_index
                    modified_value = coeff*value_w
                    if modified_value >= best_val:
                        best = index_w
                        best_val = modified_value
                keyframes.append(best)
                last_keyframe = best
        
        seconds_per_frame = 1.0/video_fps
        outcsvpath = return_video_folder_name(self.video_runner_obj)+ "/" + KEYFRAMES_CSV
        with open(outcsvpath, 'w', newline='', encoding='utf-8') as outcsvfile:
            writer = csv.writer(outcsvfile)
            writer.writerow([KEY_FRAME_HEADERS[FRAME_INDEX_SELECTOR], KEY_FRAME_HEADERS[TIMESTAMP_SELECTOR]])
            for frame_index in keyframes:
                new_row = [frame_index, float(frame_index)*seconds_per_frame]
                print(frame_index, float(frame_index)*seconds_per_frame)
                writer.writerow(new_row)