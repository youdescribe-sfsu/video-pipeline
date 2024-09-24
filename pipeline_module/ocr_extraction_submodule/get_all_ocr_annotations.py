from web_server_module.web_server_database import get_status_for_youtube_id, update_status, update_module_output
import csv
import json
from ..utils_module.utils import return_video_frames_folder, OCR_TEXT_ANNOTATIONS_FILE_NAME, return_video_folder_name, \
    OCR_HEADERS
from ..utils_module.timeit_decorator import timeit
import os

@timeit
def get_all_ocr_annotations(video_runner_obj, start=0):
    """
    Extracts OCR annotations from video frames using the detected text from each frame and saves the results.

    Parameters:
    video_runner_obj (Dict): A dictionary containing video processing details.
    start (int): The starting frame index for the OCR annotations extraction.
    """
    if get_status_for_youtube_id(video_runner_obj["video_id"], video_runner_obj["AI_USER_ID"]) == "done":
        video_runner_obj["logger"].info("OCR annotations extraction already completed, skipping step.")
        return True

    try:
        video_frames_folder = return_video_frames_folder(video_runner_obj)
        outcsvpath = return_video_folder_name(video_runner_obj) + "/" + OCR_TEXT_ANNOTATIONS_FILE_NAME
        annotations = []

        with open(outcsvpath, 'w', newline='', encoding='utf-8') as outcsvfile:
            writer = csv.writer(outcsvfile)
            writer.writerow([OCR_HEADERS["frame_index"], OCR_HEADERS["timestamp"], OCR_HEADERS["ocr_text"]])

            num_frames = len([f for f in os.listdir(video_frames_folder) if f.endswith('.jpg')])
            step = 1
            seconds_per_frame = 1 / video_runner_obj.get("video_fps", 30)

            for frame_index in range(start, num_frames, step):
                frame_filename = f'{video_frames_folder}/frame_{frame_index}.jpg'
                texts = detect_text(frame_filename)
                if texts:
                    new_row = [frame_index, float(frame_index) * seconds_per_frame, json.dumps(texts)]
                    annotations.append(new_row)
                    writer.writerow(new_row)

        # Save the OCR annotations to the database for future use
        update_module_output(video_runner_obj["video_id"], video_runner_obj["AI_USER_ID"], 'get_all_ocr_annotations',
                             {"ocr_annotations": annotations})

        update_status(video_runner_obj["video_id"], video_runner_obj["AI_USER_ID"], "done")
        video_runner_obj["logger"].info("OCR annotations extraction completed.")

    except Exception as e:
        video_runner_obj["logger"].error(f"Error in OCR annotations extraction: {str(e)}")


def detect_text(frame_file: str) -> list:
    """
    Simulates text detection from the provided image frame file.

    Parameters:
    frame_file (str): The file path of the video frame image.

    Returns:
    list: A list of detected text annotations (simulated).
    """
    # This is a placeholder function. In real-world applications, this would call an OCR service like Google Vision.
    return [{"description": "Sample text", "boundingPoly": {"vertices": [{"x": 0, "y": 0}, {"x": 100, "y": 100}]}}]