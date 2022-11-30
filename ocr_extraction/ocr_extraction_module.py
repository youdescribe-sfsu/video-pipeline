from ocr_extraction.ocr_keyframes import print_all_ocr_annotations,print_all_ocr
from ocr_extraction.filter_ocr import filter_ocr, filter_ocr_agreement, filter_ocr_remove_similarity
from ocr_extraction.detect_watermark import detect_watermark

class OcrExtraction:
    def __init__(self, video_id, video_start_time, video_end_time):
        self.video_id = video_id
        self.video_start_time = video_start_time
        self.video_end_time = video_end_time

    def run_ocr_detection(self,skip_detect_watermark=False):
        try:
            print("=== GET ALL OCR ANNOTATIONS ===")
            print_all_ocr_annotations(self.video_id)
            print("=== DETECT WATERMARK ===")
            if(skip_detect_watermark == False):
                detect_watermark(self.video_id)
            print("PRINT OCR")
            print_all_ocr(self.video_id)
            print("=== FILTER OCR V1 ===")
            filter_ocr(self.video_id)
            print("=== FILTER OCR V2 ===")
            filter_ocr_agreement(self.video_id)
            print("=== REMOVE SIMILAR OCR ===")
            filter_ocr_remove_similarity(self.video_id)
            return True
        except Exception as e:
            print("OCR EXTRACTION ERROR: ",e)
            return False
            