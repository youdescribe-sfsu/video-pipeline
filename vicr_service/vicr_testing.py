import sys
from object_detection_module.combine_captions_objects import combine_captions_objects
from object_detection_module.keyframe_captions import captions_to_csv
from utils import CAPTIONS_AND_OBJECTS_CSV, returnVideoFolderName,OUTPUT_AVG_CSV
from nodejs import node

from vicr_scoring import get_vicr_score_from_service


if __name__ == "__main__":
    video_id = sys.argv[1]
    path = returnVideoFolderName(video_id)
    print("=== GET CAPTIONS ===")
    captions_to_csv(video_id)
    print("=== COMBINE CAPTIONS AND OBJECTS ===")
    combine_captions_objects(video_id)

    # TODO VILBERT SCORING

    # TODO Convert to python
    node.call(['csv.js',path+'/'+CAPTIONS_AND_OBJECTS_CSV,path+'/'+OUTPUT_AVG_CSV])
    
    ## VICR SCORING
    get_vicr_score_from_service(video_id)
    