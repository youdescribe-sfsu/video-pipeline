import os
import csv
import json
from typing import Dict, List, Any, Optional
from google.cloud import vision
from google.cloud.vision_v1 import AnnotateImageResponse
from ..utils_module.utils import (
    read_value_from_file,
    save_value_to_file,
    return_video_frames_folder,
    return_video_folder_name,
    OCR_TEXT_ANNOTATIONS_FILE_NAME,
    OCR_TEXT_CSV_FILE_NAME,
    OCR_FILTER_CSV_FILE_NAME,
    OCR_HEADERS,
    FRAME_INDEX_SELECTOR,
    TIMESTAMP_SELECTOR,
    OCR_TEXT_SELECTOR,
)
from ..utils_module.timeit_decorator import timeit
from concurrent.futures import ThreadPoolExecutor, as_completed
import langdetect


class OcrExtraction:
    def __init__(self, video_runner_obj: Dict[str, Any]):
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        self.client = vision.ImageAnnotatorClient()
        self.frames_folder = return_video_frames_folder(video_runner_obj)
        self.output_folder = return_video_folder_name(video_runner_obj)

    @timeit
    def run_ocr_detection(self) -> bool:
        if read_value_from_file(video_runner_obj=self.video_runner_obj, key="['OCR']['started']") == 'done':
            self.logger.info("OCR detection already completed, skipping step.")
            return True

        try:
            self.logger.info("Starting OCR detection process")
            frame_files = [f for f in os.listdir(self.frames_folder) if f.endswith('.jpg')]

            results = self.process_frames_in_parallel(frame_files)

            self.save_ocr_results(results)
            self.filter_and_save_ocr_text()

            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['OCR']['started']", value='done')
            self.logger.info("OCR detection process completed successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error in OCR detection: {str(e)}")
            return False

    def process_frames_in_parallel(self, frame_files: List[str]) -> List[Dict[str, Any]]:
        results = []
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            future_to_frame = {executor.submit(self.process_single_frame, frame): frame for frame in frame_files}
            for future in as_completed(future_to_frame):
                frame = future_to_frame[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as exc:
                    self.logger.error(f'{frame} generated an exception: {exc}')
        return results

    def process_single_frame(self, frame: str) -> Dict[str, Any]:
        frame_path = os.path.join(self.frames_folder, frame)
        frame_index = int(frame.split('_')[1].split('.')[0])
        timestamp = frame_index / self.get_video_fps()

        with open(frame_path, 'rb') as image_file:
            content = image_file.read()

        image = vision.Image(content=content)
        response = self.client.text_detection(image=image)

        return {
            'frame_index': frame_index,
            'timestamp': timestamp,
            'ocr_text': AnnotateImageResponse.to_json(response)
        }

    def save_ocr_results(self, results: List[Dict[str, Any]]) -> None:
        output_file = os.path.join(self.output_folder, OCR_TEXT_ANNOTATIONS_FILE_NAME)
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [OCR_HEADERS[FRAME_INDEX_SELECTOR], OCR_HEADERS[TIMESTAMP_SELECTOR],
                          OCR_HEADERS[OCR_TEXT_SELECTOR]]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for result in sorted(results, key=lambda x: x['frame_index']):
                writer.writerow({
                    OCR_HEADERS[FRAME_INDEX_SELECTOR]: result['frame_index'],
                    OCR_HEADERS[TIMESTAMP_SELECTOR]: result['timestamp'],
                    OCR_HEADERS[OCR_TEXT_SELECTOR]: result['ocr_text']
                })

    def filter_and_save_ocr_text(self) -> None:
        input_file = os.path.join(self.output_folder, OCR_TEXT_ANNOTATIONS_FILE_NAME)
        output_file = os.path.join(self.output_folder, OCR_FILTER_CSV_FILE_NAME)

        with open(input_file, 'r', encoding='utf-8') as infile, \
                open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            reader = csv.DictReader(infile)
            fieldnames = [OCR_HEADERS[FRAME_INDEX_SELECTOR], OCR_HEADERS[TIMESTAMP_SELECTOR],
                          OCR_HEADERS[OCR_TEXT_SELECTOR]]
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                ocr_text = json.loads(row[OCR_HEADERS[OCR_TEXT_SELECTOR]])
                filtered_text = self.filter_text(ocr_text)
                if filtered_text:
                    writer.writerow({
                        OCR_HEADERS[FRAME_INDEX_SELECTOR]: row[OCR_HEADERS[FRAME_INDEX_SELECTOR]],
                        OCR_HEADERS[TIMESTAMP_SELECTOR]: row[OCR_HEADERS[TIMESTAMP_SELECTOR]],
                        OCR_HEADERS[OCR_TEXT_SELECTOR]: filtered_text
                    })

    def filter_text(self, ocr_text: Dict[str, Any]) -> Optional[str]:
        if not ocr_text.get('textAnnotations'):
            return None

        full_text = ocr_text['textAnnotations'][0]['description']

        # Remove short texts (likely noise)
        if len(full_text) < 3:
            return None

        # Detect language
        try:
            lang = langdetect.detect(full_text)
        except langdetect.lang_detect_exception.LangDetectException:
            lang = 'unknown'

        # Filter non-English text if it's not a common language
        if lang not in ['en', 'es', 'fr', 'de', 'it', 'pt', 'unknown']:
            return None

        # Remove common noise patterns (you may want to expand this list)
        noise_patterns = ['www', 'http', '.com', '.org']
        for pattern in noise_patterns:
            if pattern in full_text.lower():
                return None

        return full_text

    def get_video_fps(self) -> float:
        return float(read_value_from_file(video_runner_obj=self.video_runner_obj,
                                          key="['video_common_values']['frames_per_second']"))


if __name__ == "__main__":
    # For testing purposes
    video_runner_obj = {
        "video_id": "test_video",
        "logger": print  # Use print as a simple logger for testing
    }
    ocr_extractor = OcrExtraction(video_runner_obj)
    success = ocr_extractor.run_ocr_detection()
    print(f"OCR extraction {'succeeded' if success else 'failed'}")