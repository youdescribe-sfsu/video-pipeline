from web_server_module.web_server_database import get_status_for_youtube_id, update_status, update_module_output
import csv
import json
from ..utils_module.utils import return_video_frames_folder, OCR_TEXT_ANNOTATIONS_FILE_NAME, return_video_folder_name, \
    OCR_HEADERS
from ..utils_module.timeit_decorator import timeit
import os
from google.cloud import vision

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

        # Check if frames folder exists
        if not os.path.exists(video_frames_folder):
            video_runner_obj["logger"].error(f"Frames folder not found: {video_frames_folder}")
            return False

        with open(outcsvpath, 'w', newline='', encoding='utf-8') as outcsvfile:
            writer = csv.writer(outcsvfile)
            writer.writerow([OCR_HEADERS["frame_index"], OCR_HEADERS["timestamp"], OCR_HEADERS["ocr_text"]])

            frame_files = [f for f in os.listdir(video_frames_folder) if f.endswith('.jpg')]
            num_frames = len(frame_files)
            step = 1
            seconds_per_frame = 1 / video_runner_obj.get("video_fps", 30)

            client = vision.ImageAnnotatorClient()

            for frame_index in range(start, num_frames, step):
                frame_filename = f'{video_frames_folder}/frame_{frame_index}.jpg'
                if os.path.exists(frame_filename):
                    texts = detect_text(frame_filename, client)
                    if texts:
                        new_row = [frame_index, float(frame_index) * seconds_per_frame, json.dumps(texts)]
                        annotations.append(new_row)
                        writer.writerow(new_row)
                        outcsvfile.flush()
                else:
                    video_runner_obj["logger"].warning(f"Frame file not found: {frame_filename}")

        # Save the OCR annotations to the database for future use
        update_module_output(video_runner_obj["video_id"], video_runner_obj["AI_USER_ID"], 'get_all_ocr_annotations',
                             {"ocr_annotations": annotations})

        update_status(video_runner_obj["video_id"], video_runner_obj["AI_USER_ID"], "done")
        video_runner_obj["logger"].info("OCR annotations extraction completed.")
        return True

    except Exception as e:
        video_runner_obj["logger"].error(f"Error in OCR annotations extraction: {str(e)}")
        return False


def detect_text(frame_file: str, client: vision.ImageAnnotatorClient) -> list:
    """
    Detects text in an image file using Google Cloud Vision API.

    Parameters:
    frame_file (str): The file path of the video frame image.
    client (vision.ImageAnnotatorClient): The Vision API client.

    Returns:
    list: A list of detected text annotations.
    """
    with open(frame_file, 'rb') as image_file:
        content = image_file.read()

    image = vision.Image(content=content)
    response = client.text_detection(image=image)
    texts = response.text_annotations

    return [
        {
            "description": text.description,
            "bounding_poly": {
                "vertices": [
                    {"x": vertex.x, "y": vertex.y}
                    for vertex in text.bounding_poly.vertices
                ]
            }
        }
        for text in texts
    ]