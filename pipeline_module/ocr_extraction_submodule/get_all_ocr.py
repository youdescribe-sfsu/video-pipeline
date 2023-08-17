import csv
from utils_module.utils import OCR_TEXT_ANNOTATIONS_FILE_NAME,return_video_folder_name,OCR_TEXT_CSV_FILE_NAME,COUNT_VERTICE,OCR_HEADERS,FRAME_INDEX_SELECTOR,TIMESTAMP_SELECTOR,OCR_TEXT_SELECTOR
from utils_module.timeit_decorator import timeit
import json


@timeit
def get_all_ocr(video_runner_obj):
    """
    Extracts text from a video and stores it in a CSV file.
    :param video_runner_obj (Dict[str, int]): A dictionary that contains the information of the video.
        The keys are "video_id", "video_start_time", and "video_end_time", and their values are integers.
    :return: None
    """
    video_runner_obj["logger"].info(f"Getting all OCR for {video_runner_obj['video_id']}")
    annotation_file = open(return_video_folder_name(video_runner_obj)+"/"+COUNT_VERTICE)
    annotation_file_json = json.load(annotation_file)
    max_count_annotation = None
    if(len(annotation_file_json)>0):
        max_count_annotation = annotation_file_json[0]
    annotation_file.close()
    description_to_remove = None
    if(max_count_annotation != None and max_count_annotation["percentage"] > 60):
        description_to_remove = max_count_annotation["description"]
    outcsvpath = return_video_folder_name(video_runner_obj)+ "/" + OCR_TEXT_ANNOTATIONS_FILE_NAME
    ocr_text_csv = return_video_folder_name(video_runner_obj)+ "/" + OCR_TEXT_CSV_FILE_NAME
    ocr_text_csv_file = open(ocr_text_csv, 'w', newline='', encoding='utf-8')
    ocr_text_csv_writer = csv.writer(ocr_text_csv_file)
    ocr_text_csv_writer.writerow([OCR_HEADERS[FRAME_INDEX_SELECTOR], OCR_HEADERS[TIMESTAMP_SELECTOR], OCR_HEADERS[OCR_TEXT_SELECTOR]])
    video_runner_obj["logger"].info(f"Writing OCR to {ocr_text_csv}")
    with open(outcsvpath, encoding='utf-8') as csvf: 
        csvReader = csv.DictReader(csvf) 
        #convert each csv row into python dict
        for row in csvReader: 
            #add this python dict to json array
            ocr_text = json.loads(row[OCR_HEADERS[OCR_TEXT_SELECTOR]])
            frame_index = row[OCR_HEADERS[FRAME_INDEX_SELECTOR]]
            timestamp = row[OCR_HEADERS[TIMESTAMP_SELECTOR]]
            if(len(ocr_text['textAnnotations']) > 0):
                text_description = ocr_text['textAnnotations'][0]['description']
                replaced_text_description = replace_all(text_description, description_to_remove)
                if(len(replaced_text_description) > 0):
                    ocr_text_csv_writer.writerow([frame_index, timestamp, replaced_text_description])
                    ocr_text_csv_file.flush()
        ocr_text_csv_file.close()

def replace_all(text_description, description_to_remove):
    if(description_to_remove is not None):
        for description in description_to_remove:
            text_description = text_description.replace(description, "")
    return text_description