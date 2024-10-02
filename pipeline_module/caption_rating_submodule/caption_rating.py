import csv
import requests
import os
import traceback
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
        self.caption_rating_threshold = float(os.getenv('CAPTION_RATING_THRESHOLD', '0.5'))

    @timeit
    def perform_caption_rating(self) -> bool:
        try:
            # Check progress using the database
            if get_status_for_youtube_id(self.video_runner_obj["video_id"],
                                         self.video_runner_obj["AI_USER_ID"]) == "done":
                self.logger.info("CaptionRating already processed")
                return True

            self.get_all_caption_rating()
            self.filter_captions()

            # Mark the task as done in the database
            update_status(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"], "done")
            return True
        except Exception as e:
            self.logger.error(f"Error in perform_caption_rating: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False

    def get_caption_rating(self, image_data: Dict[str, str]) -> str:
        token = 'VVcVcuNLTwBAaxsb2FRYTYsTnfgLdxKmdDDxMQLvh7rac959eb96BCmmCrAY7Hc3'
        multipart_form_data = {
            'token': token,
            'img_url': image_data['frame_url'],
            'caption': image_data['caption']
        }
        page = 'http://localhost:{}/api'.format(os.getenv('CAPTION_RATING_SERVICE') or '8082')
        try:
            response = requests.post(page, data=multipart_form_data)
            if response.status_code != 200:
                self.logger.info("Server returned status {}.".format(response.status_code))

            return response.text.lstrip("['").rstrip("']")
        except:
            response = requests.post(page, data=multipart_form_data)
            if response.status_code != 200:
                self.logger.info("Server returned status {}.".format(response.status_code))
            return response.text.lstrip("['").rstrip("']")

    def get_all_caption_rating(self) -> None:
        try:
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
            update_module_output(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"],
                                 'caption_rating',
                                 {"ratings": "completed"})
        except Exception as e:
            self.logger.error(f"Error in get_all_caption_rating: {str(e)}")
            self.logger.error(traceback.format_exc())

    def process_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        try:
            rating = self.get_caption_rating(row)
            self.logger.info(f"Rating for caption '{row['caption']}' is {rating}")
            return {
                'frame_index': row['frame_index'],
                'frame_url': row['frame_url'],
                'caption': row['caption'],
                'rating': rating
            }
        except Exception as e:
            self.logger.error(f"Error in process_row: {str(e)}")
            return None

    def filter_captions(self) -> None:
        try:
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
                        object_row = next(
                            (obj for obj in objects_data if obj['frame_index'] == caption_row['frame_index']),
                            {})
                        output_row.update({k: object_row.get(k, '') for k in fieldnames[4:]})
                        writer.writerow(output_row)

            self.logger.info(f"Caption filtering complete for {self.video_runner_obj['video_id']}")

        except Exception as e:
            self.logger.error(f"Error in filter_captions: {str(e)}")
            self.logger.error(traceback.format_exc())