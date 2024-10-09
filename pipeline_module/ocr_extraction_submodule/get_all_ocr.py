import csv
from ..utils_module.utils import OCR_TEXT_ANNOTATIONS_FILE_NAME, return_video_folder_name, OCR_TEXT_CSV_FILE_NAME, \
    COUNT_VERTICE, OCR_HEADERS, FRAME_INDEX_SELECTOR, TIMESTAMP_SELECTOR, OCR_TEXT_SELECTOR
from ..utils_module.timeit_decorator import timeit
from web_server_module.web_server_database import get_status_for_youtube_id, update_status, update_module_output
import json
import os


@timeit
def get_all_ocr(video_runner_obj):
    """
    Extracts text from a video and stores it in a CSV file.
    :param video_runner_obj (Dict[str, int]): A dictionary that contains the information of the video.
        The keys are "video_id", "video_start_time", and "video_end_time", and their values are integers.
    :return: bool: Returns True if successful, False otherwise.
    """
    try:
        video_runner_obj["logger"].info(f"Getting all OCR for {video_runner_obj['video_id']}")

        # Check if COUNT_VERTICE file exists
        count_vertice_path = return_video_folder_name(video_runner_obj) + "/" + COUNT_VERTICE
        if not os.path.exists(count_vertice_path):
            video_runner_obj["logger"].warning(f"COUNT_VERTICE file not found: {count_vertice_path}")
            return False

        with open(count_vertice_path, 'r') as annotation_file:
            annotation_file_json = json.load(annotation_file)

        max_count_annotation = annotation_file_json[0] if annotation_file_json else None
        description_to_remove = max_count_annotation["description"] if max_count_annotation and max_count_annotation[
            "percentage"] > 60 else None

        outcsvpath = return_video_folder_name(video_runner_obj) + "/" + OCR_TEXT_ANNOTATIONS_FILE_NAME
        ocr_text_csv = return_video_folder_name(video_runner_obj) + "/" + OCR_TEXT_CSV_FILE_NAME

        # Check if input file exists
        if not os.path.exists(outcsvpath):
            video_runner_obj["logger"].warning(f"Input OCR file not found: {outcsvpath}")
            return False

        with open(ocr_text_csv, 'w', newline='', encoding='utf-8') as ocr_text_csv_file, \
                open(outcsvpath, 'r', encoding='utf-8') as csvf:

            ocr_text_csv_writer = csv.writer(ocr_text_csv_file)
            ocr_text_csv_writer.writerow(
                [OCR_HEADERS[FRAME_INDEX_SELECTOR], OCR_HEADERS[TIMESTAMP_SELECTOR], OCR_HEADERS[OCR_TEXT_SELECTOR]])

            csvReader = csv.DictReader(csvf)
            for row in csvReader:
                try:
                    ocr_text = json.loads(row[OCR_HEADERS[OCR_TEXT_SELECTOR]])
                    frame_index = row[OCR_HEADERS[FRAME_INDEX_SELECTOR]]
                    timestamp = row[OCR_HEADERS[TIMESTAMP_SELECTOR]]
                    if ocr_text and isinstance(ocr_text, list) and len(ocr_text) > 0:
                        text_description = ocr_text[0]['description']
                        replaced_text_description = replace_all(text_description, description_to_remove)
                        if replaced_text_description:
                            ocr_text_csv_writer.writerow([frame_index, timestamp, replaced_text_description])
                    else:
                        video_runner_obj["logger"].warning(f"Unexpected OCR text structure for frame {frame_index}")
                except json.JSONDecodeError:
                    video_runner_obj["logger"].error(f"Failed to parse OCR text JSON for frame {frame_index}")
                except KeyError as e:
                    video_runner_obj["logger"].error(f"Missing expected key in OCR data: {str(e)}")
                except Exception as e:
                    video_runner_obj["logger"].error(f"Unexpected error processing OCR data: {str(e)}")

        video_runner_obj["logger"].info(f"OCR extraction completed. Results saved to {ocr_text_csv}")

        # Update database
        update_module_output(video_runner_obj["video_id"], video_runner_obj["AI_USER_ID"], 'get_all_ocr',
                             {"ocr_csv_path": ocr_text_csv})
        update_status(video_runner_obj["video_id"], video_runner_obj["AI_USER_ID"], "done")

        return True

    except Exception as e:
        video_runner_obj["logger"].error(f"Error in get_all_ocr: {str(e)}")
        return False


def replace_all(text_description, description_to_remove):
    if description_to_remove:
        for description in description_to_remove:
            text_description = text_description.replace(description, "")
    return text_description