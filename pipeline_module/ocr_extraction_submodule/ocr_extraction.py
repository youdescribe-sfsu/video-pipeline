from .get_ocr_annotations import get_ocr_annotations
from .process_ocr_data import process_ocr_data
from web_server_module.web_server_database import update_status, update_module_output
from ..utils_module.utils import return_video_folder_name
from ..utils_module.timeit_decorator import timeit
import csv


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

            # Step 2: Process OCR data (watermark detection, removal, and filtering)
            filtered_ocr_data = process_ocr_data(self.video_runner_obj, ocr_annotations)

            # Step 3: Save filtered OCR data
            self.save_filtered_ocr_data(filtered_ocr_data)

            update_status(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"], "done")
            self.logger.info("OCR detection process completed successfully.")
            return True

        except Exception as e:
            self.logger.error(f"Error in OCR detection: {str(e)}")
            return False

    def save_filtered_ocr_data(self, filtered_ocr_data):
        output_file = f"{return_video_folder_name(self.video_runner_obj)}/filtered_ocr_data.csv"
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['frame_index', 'timestamp', 'text'])
            for row in filtered_ocr_data:
                writer.writerow(row)
        self.logger.info(f"Filtered OCR data saved to {output_file}")
        update_module_output(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"],
                             'ocr_extraction', {"filtered_ocr_file": output_file})