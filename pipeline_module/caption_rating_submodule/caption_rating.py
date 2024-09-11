import csv
import requests
import os
from typing import Dict, Any, List
from ..utils_module.utils import CAPTION_SCORE, load_progress_from_file, read_value_from_file, return_video_folder_name, \
    CAPTION_IMAGE_PAIR, OBJECTS_CSV, CAPTIONS_CSV, CAPTIONS_AND_OBJECTS_CSV, save_progress_to_file, save_value_to_file
from ..utils_module.timeit_decorator import timeit
from concurrent.futures import ThreadPoolExecutor, as_completed


class CaptionRating:
    def __init__(self, video_runner_obj: Dict[str, Any]):
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        self.caption_rating_endpoint = os.getenv('CAPTION_RATING_ENDPOINT', 'http://localhost:8082/api')
        self.caption_rating_threshold = float(os.getenv('CAPTION_RATING_THRESHOLD', '0.5'))

    @timeit
    def perform_caption_rating(self) -> bool:
        if read_value_from_file(video_runner_obj=self.video_runner_obj, key="['CaptionRating']['started']") == 'done':
            self.logger.info("CaptionRating Already processed")
            return True

        try:
            self.get_all_caption_rating()
            self.filter_captions()
            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['CaptionRating']['started']", value='done')
            return True
        except Exception as e:
            self.logger.error(f"Error in caption rating: {str(e)}")
            return False

    def get_caption_rating(self, image_data: Dict[str, str]) -> float:
        try:
            response = requests.post(self.caption_rating_endpoint, json={
                'img_url': image_data['frame_url'],
                'caption': image_data['caption']
            })
            response.raise_for_status()
            return float(response.text.strip("[]"))
        except requests.RequestException as e:
            self.logger.error(f"Error in caption rating request: {str(e)}")
            return 0.0

    def get_all_caption_rating(self) -> None:
        image_caption_csv_file = return_video_folder_name(self.video_runner_obj) + '/' + CAPTION_IMAGE_PAIR
        output_csv_file = return_video_folder_name(self.video_runner_obj) + '/' + CAPTION_SCORE

        if read_value_from_file(video_runner_obj=self.video_runner_obj,
                                key="['CaptionRating']['get_all_caption_rating']") == 1:
            self.logger.info("Caption rating already processed")
            return

        with open(image_caption_csv_file, 'r', newline='', encoding='utf-8') as captcsvfile, \
                open(output_csv_file, 'w', newline='', encoding='utf-8') as output_csvfile:
            reader = csv.DictReader(captcsvfile)
            fieldnames = ['frame_index', 'frame_url', 'caption', 'rating']
            writer = csv.DictWriter(output_csvfile, fieldnames=fieldnames)
            writer.writeheader()

            with ThreadPoolExecutor(max_workers=10) as executor:
                future_to_row = {executor.submit(self.process_row, row): row for row in reader}
                for future in as_completed(future_to_row):
                    row = future_to_row[future]
                    try:
                        result = future.result()
                        if result:
                            writer.writerow(result)
                    except Exception as e:
                        self.logger.error(f"Error processing row {row['frame_index']}: {str(e)}")

        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['CaptionRating']['get_all_caption_rating']",
                           value=str(1))

    def process_row(self, row: Dict[str, str]) -> Dict[str, Any]:
        captions = [row[f'caption{i}'] for i in range(1, 5) if f'caption{i}' in row]
        best_caption = max(captions,
                           key=lambda c: self.get_caption_rating({'frame_url': row['frame_url'], 'caption': c}))
        rating = self.get_caption_rating({'frame_url': row['frame_url'], 'caption': best_caption})
        self.logger.info(f"Rating for caption '{best_caption}' is {rating}")
        return {
            'frame_index': row['frame_index'],
            'frame_url': row['frame_url'],
            'caption': best_caption,
            'rating': rating
        }

    def filter_captions(self) -> None:
        if read_value_from_file(video_runner_obj=self.video_runner_obj,
                                key="['CaptionRating']['filter_captions']") == 1:
            self.logger.info("Caption filtering already processed")
            return

        caption_filter_csv = return_video_folder_name(self.video_runner_obj) + '/' + CAPTION_SCORE
        objects_csv = return_video_folder_name(self.video_runner_obj) + '/' + OBJECTS_CSV
        output_csv = return_video_folder_name(self.video_runner_obj) + '/' + CAPTIONS_AND_OBJECTS_CSV

        with open(caption_filter_csv, 'r', newline='', encoding='utf-8') as caption_file, \
                open(objects_csv, 'r', newline='', encoding='utf-8') as objects_file, \
                open(output_csv, 'w', newline='', encoding='utf-8') as output_file:

            caption_reader = csv.DictReader(caption_file)
            objects_reader = csv.DictReader(objects_file)

            fieldnames = ['frame_index', 'timestamp', 'caption', 'rating'] + list(next(objects_reader).keys())[2:]
            writer = csv.DictWriter(output_file, fieldnames=fieldnames)
            writer.writeheader()

            objects_data = list(objects_reader)

            for caption_row in caption_reader:
                if float(caption_row['rating']) > self.caption_rating_threshold:
                    output_row = {
                        'frame_index': caption_row['frame_index'],
                        'timestamp': next((obj['timestamp'] for obj in objects_data if
                                           obj['frame_index'] == caption_row['frame_index']), ''),
                        'caption': caption_row['caption'],
                        'rating': caption_row['rating']
                    }
                    object_row = next((obj for obj in objects_data if obj['frame_index'] == caption_row['frame_index']),
                                      {})
                    output_row.update({k: object_row.get(k, '') for k in fieldnames[4:]})
                    writer.writerow(output_row)

        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['CaptionRating']['filter_captions']", value=str(1))
        self.logger.info(f"Caption filtering complete for {self.video_runner_obj['video_id']}")


if __name__ == "__main__":
    # For testing purposes
    video_runner_obj = {
        "video_id": "test_video",
        "logger": print  # Use print as a simple logger for testing
    }
    caption_rater = CaptionRating(video_runner_obj)
    success = caption_rater.perform_caption_rating()
    print(f"Caption rating {'succeeded' if success else 'failed'}")