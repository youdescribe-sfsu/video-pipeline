from object_detection_module.object_detection_helper import object_detection_to_csv

class ObjectDetection:
    def __init__(self,video_id,page_port):
        self.video_id = video_id
        self.page_port = page_port
    
    
    def run_object_detection(self):
        try:
            print("=== TRACK OBJECTS ===")
            object_detection_to_csv(self.video_id,'http://localhost:{}/upload'.format(self.page_port))
            return True
        except Exception as e:
            print("OBJECT TRACKING ERROR: ",e)
            return False