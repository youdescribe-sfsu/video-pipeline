import os
import csv
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import vision
from typing import Dict, Any, List
import time
from web_server_module.web_server_database import get_status_for_youtube_id, update_status, update_module_output
from ..utils_module.utils import (
    return_video_folder_name,
    return_video_frames_folder,
    OCR_TEXT_ANNOTATIONS_FILE_NAME,
    OCR_FILTER_REMOVE_SIMILAR
)
from ..utils_module.timeit_decorator import timeit
from filter_ocr import filter_ocr_remove_similarity

class OcrExtraction:
    def __init__(self, video_runner_obj: Dict[str, Any]):
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        self.client = vision.ImageAnnotatorClient()
        self.frames_folder = return_video_frames_folder(video_runner_obj)
        self.output_folder = return_video_folder_name(video_runner_obj)
        self.logger.info(f"OcrExtraction initialized with video folder: {self.frames_folder}")

    @timeit
    def run_ocr_detection(self) -> bool:
        """
        Detects OCR in video frames using Google's Vision API and saves the results.
        """
        self.logger.info("Starting OCR detection process")

        if get_status_for_youtube_id(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"]) == "done":
            self.logger.info("OCR detection already completed, skipping step.")
            return True

        try:
            frame_files = self.get_frame_files()
            self.logger.info(f"Found {len(frame_files)} frames to process")

            results = self.process_frames_in_parallel(frame_files)
            self.save_ocr_results(results)

            self.logger.info("Starting OCR filtering process")
            self.run_ocr_filtering()

            if not self.verify_output_files():
                raise Exception("Not all required output files were generated")

            update_module_output(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"],
                                 'ocr_extraction', {"ocr_results": results})

            update_status(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"], "done")
            self.logger.info("OCR detection process completed successfully.")
            return True

        except Exception as e:
            self.logger.error(f"Error in OCR detection: {str(e)}")
            self.logger.exception("Full traceback:")
            return False

    def get_frame_files(self) -> List[str]:
        """Get list of frame files to process."""
        self.logger.info(f"Getting frame files from {self.frames_folder}")
        frame_files = [f for f in os.listdir(self.frames_folder) if f.endswith('.jpg')]
        self.logger.info(f"Found {len(frame_files)} frame files")
        return frame_files

    def process_frames_in_parallel(self, frame_files: List[str]) -> Dict[int, List[Dict]]:
        """Process video frames in parallel using Google's Vision API for OCR detection."""
        results = {}
        self.logger.info(f"Processing {len(frame_files)} frames in parallel")

        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            future_to_frame = {executor.submit(self.process_frame, frame_file): frame_file for frame_file in frame_files}
            for future in as_completed(future_to_frame):
                frame_file = future_to_frame[future]
                try:
                    frame_index, text_annotations = future.result()
                    results[frame_index] = text_annotations
                    self.logger.info(f"Processed frame {frame_index}")
                except Exception as e:
                    self.logger.error(f"Error processing frame {frame_file}: {str(e)}")

        self.logger.info(f"Completed processing {len(results)} frames")
        return results

    def process_frame(self, frame_file: str) -> tuple:
        """Process a single frame using Google's Vision API."""
        self.logger.info(f"Processing frame: {frame_file}")
        start_time = time.time()
        frame_path = os.path.join(self.frames_folder, frame_file)

        with open(frame_path, "rb") as image_file:
            content = image_file.read()

        image = vision.Image(content=content)
        response = self.client.text_detection(image=image)

        if response.error.message:
            raise Exception(f"{response.error.message}")

        text_annotations = self.convert_annotations_to_dict(response.text_annotations)
        frame_index = int(os.path.splitext(frame_file)[0].split('_')[-1])

        process_time = time.time() - start_time
        self.logger.info(f"Frame {frame_index} processed in {process_time:.2f}s.")

        return frame_index, text_annotations

    def convert_annotations_to_dict(self, annotations):
        """Convert EntityAnnotation objects to dictionaries."""
        return [
            {
                'description': annotation.description,
                'bounding_poly': {
                    'vertices': [
                        {'x': vertex.x, 'y': vertex.y}
                        for vertex in annotation.bounding_poly.vertices
                    ]
                },
                'locale': annotation.locale
            }
            for annotation in annotations
        ]

    def save_ocr_results(self, ocr_results: Dict[int, List[Dict]]) -> None:
        """Saves the OCR results to a CSV file."""
        self.logger.info(f"Saving OCR results for {len(ocr_results)} frames")
        output_file = os.path.join(self.output_folder, OCR_TEXT_ANNOTATIONS_FILE_NAME)

        try:
            with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["Frame Index", "Text Annotations"])

                for frame_index, text_annotations in ocr_results.items():
                    writer.writerow([frame_index, json.dumps(text_annotations)])

            self.logger.info(f"OCR results saved to {output_file}")
        except Exception as e:
            self.logger.error(f"Error saving OCR results: {str(e)}")
            raise

    def run_ocr_filtering(self):
        """Run OCR filtering process"""
        self.logger.info("Starting OCR filtering process")
        try:
            filter_ocr_remove_similarity(self.video_runner_obj)
            self.logger.info("OCR filtering process completed successfully")
        except Exception as e:
            self.logger.error(f"Error in OCR filtering process: {str(e)}")
            raise

    def verify_output_files(self):
        """Verify that all required output files exist"""
        required_files = [OCR_TEXT_ANNOTATIONS_FILE_NAME, OCR_FILTER_REMOVE_SIMILAR]
        for file in required_files:
            file_path = os.path.join(self.output_folder, file)
            if not os.path.exists(file_path):
                self.logger.error(f"Required file not found: {file_path}")
                return False
        return True

    def test_single_frame(self, frame_file: str) -> Dict[str, Any]:
        """Test OCR on a single frame. Used for debugging and isolated testing."""
        self.logger.info(f"Testing OCR on single frame: {frame_file}")
        try:
            frame_index, text_annotations = self.process_frame(frame_file)
            self.logger.info(f"Successfully processed test frame {frame_index}")
            return {
                "success": True,
                "frame_index": frame_index,
                "text_annotations": text_annotations
            }
        except Exception as e:
            self.logger.error(f"Error in test_single_frame: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }