from ocr_extraction_submodule.get_all_ocr_annotations import get_all_ocr_annotations
from ocr_extraction_submodule.get_all_ocr import get_all_ocr
from ocr_extraction_submodule.filter_ocr import filter_ocr, filter_ocr_agreement, filter_ocr_remove_similarity
from ocr_extraction_submodule.detect_watermark import detect_watermark
from typing import Dict

from utils_module.utils import load_progress_from_file, read_value_from_file, save_progress_to_file, save_value_to_file

class OcrExtraction:
    def __init__(self, video_runner_obj: Dict[str, int]):
        """
        Parameters:
        video_runner_obj (Dict[str, int]): A dictionary that contains the information of the video.
            The keys are "video_id", "video_start_time", and "video_end_time", and their values are integers.
        """
        self.video_runner_obj = video_runner_obj
        # self.progress_file = load_progress_from_file(video_runner_obj=video_runner_obj)

    def run_ocr_detection(self, skip_detect_watermark=False):
        """
        Run OCR detection on the given video.

        Parameters:
        skip_detect_watermark (bool): A flag to skip detecting watermark (optional, default is False).

        Returns:
        bool: Returns True if the OCR detection is successful, False otherwise.
        """
        
        # self.progress_file = load_progress_from_file(self.video_runner_obj)
        
        
        self.video_runner_obj["logger"].info(f"Running OCR detection on {self.video_runner_obj['video_id']}")
        print("=== GET ALL OCR ANNOTATIONS ===")
        get_all_ocr_annotations(self.video_runner_obj)
        self.video_runner_obj["logger"].info(f"OCR detection completed on {self.video_runner_obj['video_id']}")

        print("=== DETECT WATERMARK ===")
        self.video_runner_obj["logger"].info(f"Detecting watermark on {self.video_runner_obj['video_id']}")
        # self.progress_file = load_progress_from_file(self.video_runner_obj)
        # if self.progress_file['OCR']['detect_watermark'] == 0 and not skip_detect_watermark:
        if read_value_from_file(video_runner_obj=self.video_runner_obj,key="['OCR']['detect_watermark']") == 0 and not skip_detect_watermark:
            detect_watermark(self.video_runner_obj)
        # self.progress_file = load_progress_from_file(self.video_runner_obj)
        # self.progress_file['OCR']['detect_watermark'] = 1
        # save_progress_to_file(video_runner_obj=self.video_runner_obj, progress_data=self.progress_file)
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['OCR']['detect_watermark']", value=1)
        
        print("PRINT OCR")
        self.video_runner_obj["logger"].info(f"Printing OCR on {self.video_runner_obj['video_id']}")
        # self.progress_file = load_progress_from_file(self.video_runner_obj)
        # if self.progress_file['OCR']['get_all_ocr'] == 0:
        if read_value_from_file(video_runner_obj=self.video_runner_obj,key="['OCR']['get_all_ocr']") == 0:
            get_all_ocr(self.video_runner_obj)
        # self.progress_file = load_progress_from_file(self.video_runner_obj)
        # self.progress_file['OCR']['get_all_ocr'] = 1
        # save_progress_to_file(video_runner_obj=self.video_runner_obj, progress_data=self.progress_file)
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['OCR']['get_all_ocr']", value=1)

        print("=== FILTER OCR V1 ===")
        self.video_runner_obj["logger"].info(f"Filtering OCR on {self.video_runner_obj['video_id']}")
        # self.progress_file = load_progress_from_file(self.video_runner_obj)
        # if self.progress_file['OCR']['filter_ocr'] == 0:
        if read_value_from_file(video_runner_obj=self.video_runner_obj,key="['OCR']['filter_ocr']") == 0:
            filter_ocr(self.video_runner_obj)
        # self.progress_file = load_progress_from_file(self.video_runner_obj)
        # self.progress_file['OCR']['filter_ocr'] = 1
        # save_progress_to_file(video_runner_obj=self.video_runner_obj, progress_data=self.progress_file)
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['OCR']['filter_ocr']", value=1)
        
        print("=== FILTER OCR V2 ===")
        self.video_runner_obj["logger"].info(f"Filtering OCR on {self.video_runner_obj['video_id']}")
        # self.progress_file = load_progress_from_file(self.video_runner_obj)
        # if self.progress_file['OCR']['filter_ocr_agreement'] == 0:
        if read_value_from_file(video_runner_obj=self.video_runner_obj,key="['OCR']['filter_ocr_agreement']") == 0:
            filter_ocr_agreement(self.video_runner_obj)
        # self.progress_file = load_progress_from_file(self.video_runner_obj)
        # self.progress_file['OCR']['filter_ocr_agreement'] = 1
        # save_progress_to_file(video_runner_obj=self.video_runner_obj, progress_data=self.progress_file)
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['OCR']['filter_ocr_agreement']", value=1)
        
        print("=== REMOVE SIMILAR OCR ===")
        self.video_runner_obj["logger"].info(f"Removing similar OCR on {self.video_runner_obj['video_id']}")
        # self.progress_file = load_progress_from_file(self.video_runner_obj)
        # if self.progress_file['OCR']['filter_ocr_remove_similarity'] == 0:
        if read_value_from_file(video_runner_obj=self.video_runner_obj,key="['OCR']['filter_ocr_remove_similarity']") == 0:
            filter_ocr_remove_similarity(self.video_runner_obj)
        # self.progress_file = load_progress_from_file(self.video_runner_obj)
        # self.progress_file['OCR']['filter_ocr_remove_similarity'] = 1
        # save_progress_to_file(video_runner_obj=self.video_runner_obj, progress_data=self.progress_file)
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['OCR']['filter_ocr_remove_similarity']", value=1)

        return True
