import os
import csv
import json
from ..utils_module.timeit_decorator import timeit
from ..utils_module.utils import return_video_folder_name, OCR_TEXT_ANNOTATIONS_FILE_NAME, return_video_frames_folder
from web_server_module.web_server_database import get_module_output, update_module_output
from ..utils_module.google_services import google_service_manager

@timeit
def get_ocr_annotations(video_runner_obj):
    try:
        frame_extraction_data = get_module_output(
            video_runner_obj["video_id"],
            video_runner_obj["AI_USER_ID"],
            'frame_extraction'
        )
        if not frame_extraction_data:
            raise ValueError("Frame extraction data not found in database")

        video_frames_folder = return_video_frames_folder(video_runner_obj)
        step = int(frame_extraction_data['steps'])
        num_frames = int(frame_extraction_data['frames_extracted'])
        frames_per_second = float(frame_extraction_data['adaptive_fps'])

        # Get Vision client from service manager
        vision_client = google_service_manager.vision_client
        annotations = []

        output_file = f"{return_video_folder_name(video_runner_obj)}/{OCR_TEXT_ANNOTATIONS_FILE_NAME}"
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Frame Index", "Timestamp", "OCR Text"])

            for frame_index in range(0, num_frames, step):
                frame_filename = f'{video_frames_folder}/frame_{frame_index}.jpg'
                if os.path.exists(frame_filename):
                    texts = detect_text(frame_filename, vision_client)
                    if texts:
                        timestamp = frame_index / frames_per_second
                        writer.writerow([frame_index, timestamp, json.dumps(texts)])
                        annotations.append({
                            "frame_index": frame_index,
                            "timestamp": timestamp,
                            "texts": texts
                        })

        update_module_output(
            video_runner_obj["video_id"],
            video_runner_obj["AI_USER_ID"],
            'get_ocr_annotations',
            {"ocr_annotations_file": output_file}
        )
        return annotations

    except Exception as e:
        video_runner_obj["logger"].error(f"Error in OCR annotations extraction: {str(e)}")
        raise

def detect_text(frame_file: str, vision_client) -> list:
    try:
        with open(frame_file, 'rb') as image_file:
            content = image_file.read()

        image = {"content": content}
        response = vision_client.text_detection(image=image)
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
    except Exception as e:
        logger.error(f"Error in text detection for {frame_file}: {str(e)}")
        return []