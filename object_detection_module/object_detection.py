from object_detection_module.object_detection_helper import object_detection_to_csv
from typing import Dict
class ObjectDetection:
    def __init__(self, video_runner_obj: Dict[str, int],page_port:int):
        """
        Initialize ImportVideo object.
        
        Parameters:
        video_runner_obj (Dict[str, int]): A dictionary that contains the information of the video.
            The keys are "video_id", "video_start_time", and "video_end_time", and their values are integers.
        page_port (int): The port number of Object Detection.
        """
        self.video_runner_obj = video_runner_obj
        self.page_port = page_port
    
    
    def run_object_detection(self):
        try:
            print("=== TRACK OBJECTS ===")
            object_detection_to_csv(self.video_runner_obj,'http://localhost:{}/upload'.format(self.page_port))
            return True
        except Exception as e:
            print("OBJECT TRACKING ERROR: ",e)
            return False