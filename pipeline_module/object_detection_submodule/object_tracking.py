"""
Unused Python File
------------------
This Python file contains code that is currently not being used anywhere in the project.
It is kept for reference purposes or potential future use.

Date: August 12, 2023
"""


from object_detection_submodule.object_detection_helper import object_detection_to_csv
from object_detection_submodule.keyframe_selection import keyframe_selection
from object_detection_submodule.keyframe_captions import captions_to_csv
from object_detection_submodule.combine_captions_objects import combine_captions_objects
from text_summarization_submodule.text_summary import TextSummary

class ObjectTracking:
    def __init__(self, video_id,pagePort):
        self.video_id = video_id
        self.pagePort = pagePort
    
    def run_object_detection_and_get_captions(self):
        try:
            # # Keyframe selection
            print("=== TRACK OBJECTS ===")
            object_detection_to_csv(self.video_id,'http://localhost:{}/upload'.format(self.pagePort))
            print("=== FIND KEYFRAMES ===")
            keyframe_selection(self.video_id)
            print("=== GET CAPTIONS ===")
            captions_to_csv(self.video_id)
            print("=== COMBINE CAPTIONS AND OBJECTS ===")
            combine_captions_objects(self.video_id)
            print("==== GENERATE TEXT SUMMARY ====")
            textSummary = TextSummary(self.video_id)
            textSummary.generateTextSummary()
            return True
        except Exception as e:
            print("OBJECT TRACKING ERROR: ",e)
            return False