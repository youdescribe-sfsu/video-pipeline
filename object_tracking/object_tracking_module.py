from object_tracking.detect_objects import object_tracking_to_csv
from object_tracking.keyframe_timestamps import keyframes_from_object_tracking
from object_tracking.keyframe_captions import captions_to_csv
from object_tracking.combine_captions_objects import combine_captions_objects

class ObjectTracking:
    def __init__(self, video_id,pagePort):
        self.video_id = video_id
        self.pagePort = pagePort
    
    def run_object_detection_and_get_captions(self):
        try:
            # # Keyframe selection
            print("=== TRACK OBJECTS ===")
            object_tracking_to_csv(self.video_id,'http://localhost:{}/upload'.format(self.pagePort))
            print("=== FIND KEYFRAMES ===")
            keyframes_from_object_tracking(self.video_id)
            print("=== GET CAPTIONS ===")
            captions_to_csv(self.video_id)
            print("=== COMBINE CAPTIONS AND OBJECTS ===")
            combine_captions_objects(self.video_id)
            return True
        except Exception as e:
            print("OBJECT TRACKING ERROR: ",e)
            return False