FRAMES = "_frames"
OCR_TEXT_CSV_FILE_NAME = "ocr_text.csv"
OCR_FILTER_CSV_FILE_NAME = "ocr_filter.csv"
OCR_FILTER_CSV_2_FILE_NAME = "ocr_filter_2.csv"
OCR_FILTER_REMOVE_SIMILAR = "ocr_filter_remove_similar.csv"
OBJECTS_CSV="objects.csv"
KEYFRAMES_CSV="keyframes.csv"
CAPTIONS_CSV="captions.csv"
CAPTIONS_AND_OBJECTS_CSV="captions_and_objects.csv"
OUTPUT_AVG_CSV="outputavg.csv"
SCENE_SEGMENTED_FILE_CSV="scenesegmentedfile.csv"
SUMMARIZED_SCENES = "summarized_scenes.json"
TRANSCRIPTS = "transcripts.json"
DIALOGS = "dialogs.json"
VICR_CSV = "vicr.csv"
import os

def returnVideoFolderName(video_id):
    '''Returns the folder name for a video'''
    start_time = os.getenv('START_TIME') or None
    end_time = os.getenv('END_TIME') or None
    if(start_time != None and end_time != None):
        return "/home/datasets/pipeline/" + video_id+"_start_"+str(start_time)+"_end_"+str(end_time) + "_files"
    return "/home/datasets/pipeline/" + video_id + "_files"

def returnVideoDownloadLocation(video_id):
    return returnVideoFolderName(video_id) + '/' + video_id

def returnVideoFramesFolder(video_id):
    return returnVideoFolderName(video_id)+ '/frames'

def returnAudioFileName(video_id):
    return video_id+".flac"