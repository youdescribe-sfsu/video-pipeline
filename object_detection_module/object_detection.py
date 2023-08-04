from object_detection_module.object_detection_helper import object_detection_to_csv
from typing import Dict
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
            object_detection_to_csv(self.video_runner_obj)
            return True
        except Exception as e:
            print("OBJECT TRACKING ERROR: ",e)
            return False