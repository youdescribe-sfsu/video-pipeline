from .get_ocr_annotations import get_ocr_annotations
from .process_ocr_data import process_ocr_data
from web_server_module.web_server_database import update_status
from ..utils_module.timeit_decorator import timeit


class OcrExtraction:
    def __init__(self, video_runner_obj):
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")

    @timeit
    def run_ocr_detection(self):
        try:
            self.logger.info("Starting OCR detection process")

            # Step 1: Extract OCR annotations
            ocr_annotations = get_ocr_annotations(self.video_runner_obj)

            # Step 2: Process OCR data
            process_ocr_data(self.video_runner_obj, ocr_annotations)

            update_status(
                self.video_runner_obj["video_id"],
                self.video_runner_obj["AI_USER_ID"],
                "done"
            )
            self.logger.info("OCR detection process completed successfully.")
            return True

        except Exception as e:
            self.logger.error(f"Error in OCR detection: {str(e)}")
            return False