import csv
import requests
import os
from typing import Dict, Any
from web_server_module.web_server_database import get_status_for_youtube_id, update_status, update_module_output
from ..utils_module.utils import CAPTION_SCORE, return_video_folder_name, CAPTION_IMAGE_PAIR, OBJECTS_CSV, \
    CAPTIONS_AND_OBJECTS_CSV
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
        # Check progress using the database
        if get_status_for_youtube_id(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"]) == "done":
            self.logger.info("CaptionRating already processed")
            return True

        try:
            self.get_all_caption_rating()
            self.filter_captions()

            # Mark the task as done in the database
            update_status(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"], "done")
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

        # Save caption ratings to the database
        update_module_output(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"], 'caption_rating',
                             {"ratings": "completed"})

    def process_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
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

        self.logger.info(f"Caption filtering complete for {self.video_runner_obj['video_id']}")

