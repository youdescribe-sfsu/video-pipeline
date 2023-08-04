from ocr_extraction_module.get_all_ocr_annotations import get_all_ocr_annotations
from ocr_extraction_module.get_all_ocr import get_all_ocr
from ocr_extraction_module.filter_ocr import filter_ocr, filter_ocr_agreement, filter_ocr_remove_similarity
from ocr_extraction_module.detect_watermark import detect_watermark
from typing import Dict
class OcrExtraction:
    def __init__(self, video_runner_obj: Dict[str, int]):
        """
        Parameters:
        video_runner_obj (Dict[str, int]): A dictionary that contains the information of the video.
            The keys are "video_id", "video_start_time", and "video_end_time", and their values are integers.
        """
        self.video_runner_obj = video_runner_obj

    def run_ocr_detection(self,skip_detect_watermark=False):
        """
        Run OCR detection on the given video.

        Parameters:
        skip_detect_watermark (bool): A flag to skip detecting watermark (optional, default is False).

        Returns:
        bool: Returns True if the OCR detection is successful, False otherwise.
        """
        self.video_runner_obj["logger"].info(f"Running OCR detection on {self.video_runner_obj['video_id']}")
        print("=== GET ALL OCR ANNOTATIONS ===")
        get_all_ocr_annotations(self.video_runner_obj)
        self.video_runner_obj["logger"].info(f"OCR detection completed on {self.video_runner_obj['video_id']}")
        print("=== DETECT WATERMARK ===")
        self.video_runner_obj["logger"].info(f"Detecting watermark on {self.video_runner_obj['video_id']}")
        if(skip_detect_watermark == False):
            detect_watermark(self.video_runner_obj)
        print("PRINT OCR")
        self.video_runner_obj["logger"].info(f"Printing OCR on {self.video_runner_obj['video_id']}")
        get_all_ocr(self.video_runner_obj)
        print("=== FILTER OCR V1 ===")
        self.video_runner_obj["logger"].info(f"Filtering OCR on {self.video_runner_obj['video_id']}")
        filter_ocr(self.video_runner_obj)
        print("=== FILTER OCR V2 ===")
        self.video_runner_obj["logger"].info(f"Filtering OCR on {self.video_runner_obj['video_id']}")
        filter_ocr_agreement(self.video_runner_obj)
        print("=== REMOVE SIMILAR OCR ===")
        self.video_runner_obj["logger"].info(f"Removing similar OCR on {self.video_runner_obj['video_id']}")
        filter_ocr_remove_similarity(self.video_runner_obj)
        return True
            