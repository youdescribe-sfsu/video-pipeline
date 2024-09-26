import os
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import vision
from typing import Dict, Any
import psutil
import time
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
        self.max_retries = 3
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
            self.check_filesystem()
            frame_files = self.get_frame_files()
            self.logger.info(f"Found {len(frame_files)} frames to process")

            results = self.process_frames_in_parallel(frame_files)
            self.save_ocr_results(results)

            update_module_output(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"],
                                 'ocr_extraction', {"ocr_results": results})

            update_status(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"], "done")
            self.logger.info("OCR detection process completed successfully.")
            return True

        except Exception as e:
            self.logger.error(f"Error in OCR detection: {str(e)}")
            return False

    def check_filesystem(self):
        """Check filesystem permissions and available space."""
        if not os.access(self.frames_folder, os.R_OK):
            raise PermissionError(f"No read permission for {self.frames_folder}")
        if not os.access(self.output_folder, os.W_OK):
            raise PermissionError(f"No write permission for {self.output_folder}")

        free_space = psutil.disk_usage(self.output_folder).free
        if free_space < 1_000_000_000:  # 1 GB
            raise IOError(f"Insufficient disk space. Only {free_space / 1_000_000_000:.2f} GB available.")

    def get_frame_files(self) -> List[str]:
        """Get list of frame files to process."""
        return [f for f in os.listdir(self.frames_folder) if f.endswith('.jpg')]

    def process_frames_in_parallel(self, frame_files: List[str]) -> Dict[int, str]:
        """Process video frames in parallel using Google's Vision API for OCR detection."""
        results = {}
        self.logger.info("Processing frames in parallel")

        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            future_to_frame = {executor.submit(self.process_frame_with_retry, frame_file): frame_file for frame_file in
                               frame_files}
            for future in as_completed(future_to_frame):
                frame_file = future_to_frame[future]
                try:
                    frame_index, text_annotations = future.result()
                    results[frame_index] = text_annotations
                    self.logger.info(f"Processed frame {frame_index}")
                except Exception as e:
                    self.logger.error(f"Error processing frame {frame_file}: {str(e)}")

        return results

    def process_frame_with_retry(self, frame_file: str, max_retries: int = 3) -> tuple:
        """Process a single frame with retry logic."""
        for attempt in range(max_retries):
            try:
                return self.process_frame(frame_file)
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                self.logger.warning(f"Attempt {attempt + 1} failed for frame {frame_file}: {str(e)}. Retrying...")
                time.sleep(1)  # Wait before retrying

    def process_frame(self, frame_file: str) -> tuple:
        """Process a single frame using Google's Vision API."""
        start_time = time.time()
        frame_path = os.path.join(self.frames_folder, frame_file)
        with open(frame_path, "rb") as image_file:
            content = image_file.read()

        image = vision.Image(content=content)
        response = self.client.text_detection(image=image)

        if response.error.message:
            raise Exception(f"{response.error.message}")

        text_annotations = response.text_annotations
        frame_index = int(os.path.splitext(frame_file)[0].split('_')[-1])

        process_time = time.time() - start_time
        memory_usage = psutil.virtual_memory().percent
        self.logger.info(f"Frame {frame_index} processed in {process_time:.2f}s. Memory usage: {memory_usage}%")

        return frame_index, text_annotations

    def save_ocr_results(self, ocr_results: Dict[int, str]) -> None:
        """Saves the OCR results to a CSV file."""
        self.logger.info("Saving OCR results to file")
        output_file = os.path.join(self.output_folder, OCR_TEXT_ANNOTATIONS_FILE_NAME)

        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Frame Index", "Text Annotations"])

            for frame_index, text_annotations in ocr_results.items():
                writer.writerow([frame_index, text_annotations])

        self.logger.info(f"OCR results saved to {output_file}")

    def test_single_frame(self, frame_file: str) -> Dict[str, Any]:
        """Test OCR on a single frame. Used for debugging and isolated testing."""
        try:
            frame_index, text_annotations = self.process_frame(frame_file)
            return {
                "success": True,
                "frame_index": frame_index,
                "text_annotations": text_annotations
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }