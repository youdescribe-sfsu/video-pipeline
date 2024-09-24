from ..utils_module.utils import return_video_folder_name, OCR_TEXT_ANNOTATIONS_FILE_NAME, COUNT_VERTICE
from web_server_module.web_server_database import get_status_for_youtube_id, update_status
import csv
import json
import sys

csv.field_size_limit(2 ** 31 - 1)


def detect_watermark(video_runner_obj):
    """
    Parameters:
    video_runner_obj (Dict[str, int]): A dictionary that contains the information of the video.
    """
    if get_status_for_youtube_id(video_runner_obj["video_id"], video_runner_obj["AI_USER_ID"]) == "done":
        video_runner_obj["logger"].info("Watermark detection already completed, skipping step.")
        return True

    path = return_video_folder_name(video_runner_obj) + "/" + OCR_TEXT_ANNOTATIONS_FILE_NAME
    count_obj = []

    with open(path, encoding='utf-8') as csvf:
        csvReader = csv.DictReader(csvf)
        row_count = 0
        for row in csvReader:
            ocr_text = json.loads(row["OCR Text"])
            if len(ocr_text["textAnnotations"]) > 0:
                row_count += 1
                text_annotations = ocr_text["textAnnotations"]
                for i in range(len(text_annotations)):
                    vertice = text_annotations[i]["boundingPoly"]["vertices"]
                    description = text_annotations[i]["description"]
                    locale = text_annotations[i]["locale"]
                    if locale == "en" or len(locale) == 0:
                        found = False
                        for j in range(len(count_obj)):
                            if isSamePolygon(vertice, count_obj[j]["vertice"]):
                                count_obj[j]["count"] += 1
                                if description not in count_obj[j]["description"]:
                                    count_obj[j]["description"].append(description)
                                found = True
                                break
                        if not found:
                            count_obj.append({
                                "vertice": vertice,
                                "description": [description],
                                "count": 1
                            })

    video_runner_obj["logger"].info(f"Total rows: {row_count}")
    count_obj = sorted(count_obj, key=lambda i: i['count'], reverse=True)

    if len(count_obj) > 0:
        max_count = count_obj[0]["count"]
        vertice_with_max_count = count_obj[0]["vertice"]
        count_obj[0]['percentage'] = max_count / row_count * 100
        video_runner_obj["logger"].info(f"Percentage of frames with watermark: {max_count / row_count * 100}")

    with open(return_video_folder_name(video_runner_obj) + "/" + COUNT_VERTICE, 'w', encoding='utf-8') as jsonf:
        jsonString = json.dumps(count_obj)
        jsonf.write(jsonString)

    # Save output to the database for future use
    update_module_output(video_runner_obj["video_id"], video_runner_obj["AI_USER_ID"], 'detect_watermark',
                         {"watermark_info": count_obj})

    update_status(video_runner_obj["video_id"], video_runner_obj["AI_USER_ID"], "done")
    video_runner_obj["logger"].info("Watermark detection completed.")
