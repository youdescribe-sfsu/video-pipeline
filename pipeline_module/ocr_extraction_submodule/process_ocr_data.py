import csv
import json
from web_server_module.web_server_database import update_module_output
from ..utils_module.utils import return_video_folder_name, OCR_TEXT_CSV_FILE_NAME, OCR_FILTER_CSV_FILE_NAME, \
    OCR_FILTER_REMOVE_SIMILAR, COUNT_VERTICE
from ..utils_module.timeit_decorator import timeit


@timeit
def process_ocr_data(video_runner_obj, ocr_annotations):
    logger = video_runner_obj.get("logger")

    # Generate OCR_TEXT_CSV_FILE_NAME
    generate_ocr_text_csv(video_runner_obj, ocr_annotations)

    # Detect watermarks and generate COUNT_VERTICE
    watermarks = detect_watermarks(video_runner_obj, ocr_annotations)

    # Remove watermarks and filter
    filtered_data = remove_watermarks_and_filter(ocr_annotations, watermarks)

    # Generate OCR_FILTER_CSV_FILE_NAME
    generate_ocr_filter_csv(video_runner_obj, filtered_data)

    # Remove similar entries and generate OCR_FILTER_REMOVE_SIMILAR
    final_filtered_data = remove_similar_entries(filtered_data)
    generate_ocr_filter_remove_similar(video_runner_obj, final_filtered_data)

    update_module_output(video_runner_obj["video_id"], video_runner_obj["AI_USER_ID"],
                         'process_ocr_data', {"ocr_processing_complete": True})

    return final_filtered_data


def generate_ocr_text_csv(video_runner_obj, ocr_annotations):
    output_file = f"{return_video_folder_name(video_runner_obj)}/{OCR_TEXT_CSV_FILE_NAME}"
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Frame Index", "Timestamp", "Text"])
        for ann in ocr_annotations:
            for text in ann["texts"]:
                writer.writerow([ann["frame_index"], ann["timestamp"], text["description"]])


def detect_watermarks(video_runner_obj, ocr_annotations):
    watermarks = []
    text_count = {}
    total_frames = len(ocr_annotations)

    for ann in ocr_annotations:
        for text in ann["texts"]:
            description = text["description"]
            text_count[description] = text_count.get(description, 0) + 1

    threshold = 0.8 * total_frames
    watermarks = [text for text, count in text_count.items() if count > threshold]

    output_file = f"{return_video_folder_name(video_runner_obj)}/{COUNT_VERTICE}"
    with open(output_file, 'w', encoding='utf-8') as jsonfile:
        json.dump({"watermarks": watermarks}, jsonfile)

    return watermarks


def remove_watermarks_and_filter(ocr_annotations, watermarks):
    filtered_data = []
    for ann in ocr_annotations:
        filtered_texts = [text for text in ann["texts"] if text["description"] not in watermarks]
        if filtered_texts:
            filtered_data.append({
                "frame_index": ann["frame_index"],
                "timestamp": ann["timestamp"],
                "texts": filtered_texts
            })
    return filtered_data

def generate_ocr_filter_csv(video_runner_obj, filtered_data):
    output_file = f"{return_video_folder_name(video_runner_obj)}/{OCR_FILTER_CSV_FILE_NAME}"
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Frame Index", "Timestamp", "Text"])
        for data in filtered_data:
            for text in data["texts"]:
                writer.writerow([data["frame_index"], data["timestamp"], text["description"]])

def remove_similar_entries(filtered_data):
    final_filtered_data = []
    seen_texts = set()
    for data in filtered_data:
        unique_texts = []
        for text in data["texts"]:
            if text["description"] not in seen_texts:
                seen_texts.add(text["description"])
                unique_texts.append(text)
        if unique_texts:
            final_filtered_data.append({
                "frame_index": data["frame_index"],
                "timestamp": data["timestamp"],
                "texts": unique_texts
            })
    return final_filtered_data

def generate_ocr_filter_remove_similar(video_runner_obj, final_filtered_data):
    output_file = f"{return_video_folder_name(video_runner_obj)}/{OCR_FILTER_REMOVE_SIMILAR}"
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Frame Index", "Timestamp", "Text"])
        for data in final_filtered_data:
            for text in data["texts"]:
                writer.writerow([data["frame_index"], data["timestamp"], text["description"]])
