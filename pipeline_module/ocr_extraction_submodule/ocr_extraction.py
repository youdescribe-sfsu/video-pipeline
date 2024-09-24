import os
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import vision
from typing import Dict, Any
from web_server_module.web_server_database import get_status_for_youtube_id, update_status, update_module_output
from ..utils_module.utils import (
    return_video_folder_name,
    return_video_frames_folder,
    OCR_TEXT_ANNOTATIONS_FILE_NAME
)
from ..utils_module.timeit_decorator import timeit

class OcrExtraction:
    def __init__(self, video_runner_obj: Dict[str, Any]):
        print("Initializing OcrExtraction")
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        self.client = vision.ImageAnnotatorClient()
        self.frames_folder = return_video_frames_folder(video_runner_obj)
        self.output_folder = return_video_folder_name(video_runner_obj)
        print(f"Initialized with video folder: {self.frames_folder}")

    @timeit
    def run_ocr_detection(self) -> bool:
        """
        Detects OCR in video frames using Google's Vision API and saves the results.
        """
        if get_status_for_youtube_id(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"]) == "done":
            self.logger.info("OCR detection already completed, skipping step.")
            return True

        try:
            self.logger.info("Starting OCR detection process")
            frame_files = [f for f in os.listdir(self.frames_folder) if f.endswith('.jpg')]

            results = self.process_frames_in_parallel(frame_files)
            self.save_ocr_results(results)

            # Save OCR results to the database for use by future modules
            update_module_output(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"], 'ocr_extraction', {"ocr_results": results})

            update_status(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"], "done")
            self.logger.info("OCR detection process completed successfully.")
            return True

        except Exception as e:
            self.logger.error(f"Error in OCR detection: {str(e)}")
            return False

    def process_frames_in_parallel(self, frame_files: list) -> Dict[int, str]:
        """
        Process video frames in parallel using Google's Vision API for OCR detection.
        """
        results = {}
        print("Processing frames in parallel")

        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = [executor.submit(self.detect_text, frame_file) for frame_file in frame_files]

            for future in as_completed(futures):
                try:
                    frame_index, text_annotations = future.result()
                    results[frame_index] = text_annotations
                except Exception as e:
                    self.logger.error(f"Error processing frames: {str(e)}")

        return results

    def detect_text(self, frame_file: str) -> tuple:
        """
        Detects text in a single frame using Google's Vision API.
        """
        frame_path = os.path.join(self.frames_folder, frame_file)
        with open(frame_path, "rb") as image_file:
            content = image_file.read()

        image = vision.Image(content=content)
        response = self.client.text_detection(image=image)

        if response.error.message:
            raise Exception(f"{response.error.message}")

        text_annotations = response.text_annotations
        frame_index = int(os.path.splitext(frame_file)[0].split('_')[-1])
        return frame_index, text_annotations

    def save_ocr_results(self, ocr_results: Dict[int, str]) -> None:
        """
        Saves the OCR results to a CSV file.
        """
        print("Saving OCR results to file")
        output_file = os.path.join(self.output_folder, OCR_TEXT_ANNOTATIONS_FILE_NAME)

        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Frame Index", "Text Annotations"])

            for frame_index, text_annotations in ocr_results.items():
                writer.writerow([frame_index, text_annotations])

        self.logger.info(f"OCR results saved to {output_file}")
