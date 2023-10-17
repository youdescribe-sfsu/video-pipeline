from .object_detection_helper import object_detection_to_csv
from typing import Dict

from ..utils_module.utils import load_progress_from_file, read_value_from_file, save_progress_to_file, save_value_to_file
class ObjectDetection:
    def __init__(self, video_runner_obj: Dict[str, int]):
        """
        Initialize ImportVideo object.
        
        Parameters:
        video_runner_obj (Dict[str, int]): A dictionary that contains the information of the video.
            The keys are "video_id", "video_start_time", and "video_end_time", and their values are integers.
        page_port (int): The port number of Object Detection.
        """
        self.video_runner_obj = video_runner_obj
    
    
    def run_object_detection(self):
        try:
            self.video_runner_obj["logger"].info(f"Running object detection for {self.video_runner_obj['video_id']}")
            print("=== TRACK OBJECTS ===")
            if read_value_from_file(video_runner_obj=self.video_runner_obj,key="['ObjectDetection']['started']") == 'done':

                self.video_runner_obj["logger"].info("Object detection already done, skipping step.")
                print("Object detection already done, skipping step.")
                return True
            if(object_detection_to_csv(self.video_runner_obj)):
                save_value_to_file(video_runner_obj=self.video_runner_obj, key="['ObjectDetection']['started']", value='done')
            return True
        except Exception as e:
            print("OBJECT TRACKING ERROR: ",e)
            return False