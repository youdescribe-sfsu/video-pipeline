import os
from typing import Dict, Any
from web_server_module.web_server_database import get_status_for_youtube_id, update_status, update_module_output
from ..utils_module.utils import return_video_folder_name, return_video_frames_folder, OCR_TEXT_ANNOTATIONS_FILE_NAME
from ..utils_module.timeit_decorator import timeit
from .detect_watermark import detect_watermark
from .filter_ocr import filter_ocr, filter_ocr_remove_similarity
from .get_all_ocr import get_all_ocr
from .get_all_ocr_annotations import get_all_ocr_annotations

class OcrExtraction:
    def __init__(self, video_runner_obj: Dict[str, Any]):
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        self.frames_folder = return_video_frames_folder(video_runner_obj)
        self.output_folder = return_video_folder_name(video_runner_obj)
        self.logger.info(f"OcrExtraction initialized with video folder: {self.frames_folder}")

    @timeit
    def run_ocr_detection(self) -> bool:
        """
        Runs the complete OCR detection process, including watermark detection,
        OCR extraction, and filtering.
        """
        self.logger.info("Starting OCR detection process")

        try:
            # Step 1: Get all OCR annotations
            if not get_all_ocr_annotations(self.video_runner_obj):
                raise Exception("Failed to get OCR annotations")

            # Step 2: Detect watermark
            if not detect_watermark(self.video_runner_obj):
                raise Exception("Failed to detect watermark")

            # Step 3: Get all OCR
            if not get_all_ocr(self.video_runner_obj):
                raise Exception("Failed to get all OCR")

            # Step 4: Filter OCR
            if not filter_ocr(self.video_runner_obj):
                raise Exception("Failed to filter OCR")

            # Step 5: Remove similar OCR entries
            if not filter_ocr_remove_similarity(self.video_runner_obj):
                raise Exception("Failed to remove similar OCR entries")

            # Verify all required files exist
            if not self.verify_output_files():
                raise Exception("Not all required output files were generated")

            update_status(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"], "done")
            self.logger.info("OCR detection process completed successfully.")
            return True

        except Exception as e:
            self.logger.error(f"Error in OCR detection: {str(e)}")
            return False

    def verify_output_files(self):
        """Verify that all required output files exist"""
        required_files = [
            OCR_TEXT_ANNOTATIONS_FILE_NAME,
            "ocr_text.csv",
            "ocr_filter.csv",
            "ocr_filter_remove_similar.csv"
        ]
        for file in required_files:
            file_path = os.path.join(self.output_folder, file)
            if not os.path.exists(file_path):
                self.logger.error(f"Required file not found: {file_path}")
                return False
        return True