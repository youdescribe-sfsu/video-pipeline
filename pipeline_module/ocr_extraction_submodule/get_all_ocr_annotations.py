from web_server_module.web_server_database import (
    get_status_for_youtube_id,
    update_status,
    update_module_output,
    get_module_output
)
import csv
import json
from ..utils_module.utils import (
    return_video_frames_folder,
    OCR_TEXT_ANNOTATIONS_FILE_NAME,
    return_video_folder_name,
    OCR_HEADERS
)
from ..utils_module.timeit_decorator import timeit
import os
from google.cloud import vision


@timeit
def get_all_ocr_annotations(video_runner_obj):
    """
    Extracts OCR annotations from video frames using the detected text from each frame and saves the results.
    """
    try:
        video_frames_folder = return_video_frames_folder(video_runner_obj)
        outcsvpath = os.path.join(
            return_video_folder_name(video_runner_obj),
            OCR_TEXT_ANNOTATIONS_FILE_NAME
        )
        annotations = []

        # Check if frames folder exists
        if not os.path.exists(video_frames_folder):
            video_runner_obj["logger"].error(f"Frames folder not found: {video_frames_folder}")
            return False

        # Retrieve frame extraction data from the database
        frame_extraction_data = get_module_output(
            video_runner_obj["video_id"],
            video_runner_obj["AI_USER_ID"],
            'frame_extraction'
        )
        if not frame_extraction_data:
            video_runner_obj["logger"].error("Frame extraction data not found in database")
            return False

        step = int(frame_extraction_data['steps'])
        num_frames = int(frame_extraction_data['frames_extracted'])
        frames_per_second = float(frame_extraction_data['adaptive_fps'])

        with open(outcsvpath, 'w', newline='', encoding='utf-8') as outcsvfile:
            writer = csv.writer(outcsvfile)
            writer.writerow([
                OCR_HEADERS["frame_index"],
                OCR_HEADERS["timestamp"],
                OCR_HEADERS["ocr_text"]
            ])

            client = vision.ImageAnnotatorClient()

            for frame_index in range(0, num_frames, step):
                frame_filename = os.path.join(
                    video_frames_folder,
                    f'frame_{frame_index}.jpg'
                )
                if os.path.exists(frame_filename):
                    texts = detect_text(frame_filename, client)
                    if texts:
                        timestamp = frame_index / frames_per_second
                        new_row = [frame_index, timestamp, json.dumps(texts)]
                        annotations.append(new_row)
                        writer.writerow(new_row)
                        outcsvfile.flush()
                else:
                    video_runner_obj["logger"].warning(f"Frame file not found: {frame_filename}")

        # Save the OCR annotations to the database for future use
        update_module_output(
            video_runner_obj["video_id"],
            video_runner_obj["AI_USER_ID"],
            'get_all_ocr_annotations',
            {"ocr_annotations": annotations}
        )

        update_status(
            video_runner_obj["video_id"],
            video_runner_obj["AI_USER_ID"],
            "done"
        )
        video_runner_obj["logger"].info("OCR annotations extraction completed.")
        return True

    except Exception as e:
        video_runner_obj["logger"].error(f"Error in OCR annotations extraction: {str(e)}")
        return False


def detect_text(frame_file: str, client: vision.ImageAnnotatorClient) -> dict:
    """
    Detects text in an image file using Google Cloud Vision API.
    """
    try:
        with open(frame_file, 'rb') as image_file:
            content = image_file.read()

        image = vision.Image(content=content)
        response = client.text_detection(image=image)

        if response.error.message:
            raise Exception(f"Google Vision API error: {response.error.message}")

        texts = response.text_annotations

        text_annotations = [
            {
                "description": text.description,
                "bounding_poly": {
                    "vertices": [
                        {"x": vertex.x if vertex.x is not None else 0,
                         "y": vertex.y if vertex.y is not None else 0}
                        for vertex in text.bounding_poly.vertices
                    ]
                }
            }
            for text in texts
        ]

        # Return a dictionary with 'Text Annotations' key
        return {
            "Text Annotations": text_annotations
        }

    except Exception as e:
        # Log the error and return an empty dictionary
        print(f"Error in detect_text for file {frame_file}: {str(e)}")
        return {}
