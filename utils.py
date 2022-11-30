#!/usr/bin/python
FRAMES = '_frames'
OCR_TEXT_ANNOTATIONS_FILE_NAME = 'ocr_text_annotations.csv'
OCR_TEXT_CSV_FILE_NAME = 'ocr_text.csv'
OCR_FILTER_CSV_FILE_NAME = 'ocr_filter.csv'
OCR_FILTER_CSV_2_FILE_NAME = 'ocr_filter_2.csv'
OCR_FILTER_REMOVE_SIMILAR = 'ocr_filter_remove_similar.csv'
OBJECTS_CSV = 'objects.csv'
KEYFRAMES_CSV = 'keyframes.csv'
CAPTIONS_CSV = 'captions.csv'
CAPTIONS_AND_OBJECTS_CSV = 'captions_and_objects.csv'
OUTPUT_AVG_CSV = 'outputavg.csv'
SCENE_SEGMENTED_FILE_CSV = 'scenesegmentedfile.csv'
SUMMARIZED_SCENES = 'summarized_scenes.json'
TRANSCRIPTS = 'transcripts.json'
DIALOGS = 'dialogs.json'
VICR_CSV = 'vicr.csv'
COUNT_VERTICE = 'count_vertice.json'

OCR_HEADERS = {
    'frame_index': 'Frame Index',
    'timestamp': 'Timestamp',
    'ocr_text': 'OCR Text'
}

FRAME_INDEX_SELECTOR = 'frame_index'
TIMESTAMP_SELECTOR = 'timestamp'
OCR_TEXT_SELECTOR = 'ocr_text'


import os


def returnVideoFolderName(video_id):
    '''Returns the folder name for a video'''

    CURRENT_ENV = os.environ.get('CURRENT_ENV')
    start_time = os.getenv('START_TIME') or None
    end_time = os.getenv('END_TIME') or None
    if start_time != None and end_time != None:
        if CURRENT_ENV == 'development':
            return video_id + '_' + start_time + '_' + end_time
        else:
            return '/home/datasets/pipeline/' + video_id + '_start_' \
                + str(start_time) + '_end_' + str(end_time) + '_files'
    if CURRENT_ENV == 'development':
        return video_id + '_files'
    else:
        return '/home/datasets/pipeline/' + video_id + '_files'


def returnVideoDownloadLocation(video_id):
    return returnVideoFolderName(video_id) + '/' + video_id + '.mp4'


def returnVideoFramesFolder(video_id):
    return returnVideoFolderName(video_id) + '/frames'


def returnAudioFileName(video_id):
    return video_id + '.flac'


def returnIntIfPossible(value):
    try:
        decimal = value % 1
        if decimal == 0:
            return int(value)
        else:
            return value
    except:
        return value
