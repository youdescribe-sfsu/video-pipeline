import csv
import requests
import os
from typing import Dict, Any
from web_server_module.web_server_database import get_status_for_youtube_id, update_status, update_module_output
from ..utils_module.utils import CAPTION_SCORE, return_video_folder_name, CAPTION_IMAGE_PAIR, OBJECTS_CSV, \
    CAPTIONS_CSV, CAPTIONS_AND_OBJECTS_CSV


class CaptionRating:
    def __init__(self, video_runner_obj: Dict[str, Any]):
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        self.caption_rating_threshold = float(os.getenv('CAPTION_RATING_THRESHOLD', '0.5'))
        self.caption_rating_service = os.getenv('CAPTION_RATING_SERVICE', '8082')

    def perform_caption_rating(self) -> bool:
        if get_status_for_youtube_id(self.video_runner_obj["video_id"],
                                        self.video_runner_obj["AI_USER_ID"]) == "done":
            self.logger.info("CaptionRating already processed")
            return True

        try:
            self.get_all_caption_rating()
            self.filter_captions()
            update_status(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"], "done")
            return True
        except Exception as e:
            self.logger.error(f"Error in perform_caption_rating: {str(e)}")
            return False

    def get_caption_rating(self, image_data: Dict[str, str]) -> float:
        token = 'VVcVcuNLTwBAaxsb2FRYTYsTnfgLdxKmdDDxMQLvh7rac959eb96BCmmCrAY7Hc3'
        multipart_form_data = {
            'token': token,
            'img_url': image_data['frame_url'],
            'caption': image_data['caption']
        }
        page = f'http://localhost:{self.caption_rating_service}/api'

        try:
            response = requests.post(page, data=multipart_form_data)
            response.raise_for_status()
            rating_str = response.text.strip("[]'")
            return float(rating_str)
        except requests.RequestException as e:
            self.logger.error(f"Error in caption rating request: {str(e)}")
            return 0.0
        except ValueError as e:
            self.logger.error(f"Invalid rating value received: {rating_str}")
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

            for image_data in reader:
                rating = self.get_caption_rating(image_data)
                self.logger.info(f"Rating for caption '{image_data['caption']}' is {rating}")
                writer.writerow({
                    'frame_index': image_data['frame_index'],
                    'frame_url': image_data['frame_url'],
                    'caption': image_data['caption'],
                    'rating': rating
                })

        update_module_output(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"],
                             'caption_rating', {"ratings": "completed"})

    def filter_captions(self) -> None:
        caption_filter_csv = return_video_folder_name(self.video_runner_obj) + '/' + CAPTION_SCORE
        objects_csv = return_video_folder_name(self.video_runner_obj) + '/' + OBJECTS_CSV
        captions_csv = return_video_folder_name(self.video_runner_obj) + '/' + CAPTIONS_CSV
        output_csv = return_video_folder_name(self.video_runner_obj) + '/' + CAPTIONS_AND_OBJECTS_CSV

        filtered_frame_indices = set()
        with open(caption_filter_csv, 'r', newline='', encoding='utf-8') as caption_filter_file:
            reader = csv.DictReader(caption_filter_file)
            for row in reader:
                if float(row['rating']) > self.caption_rating_threshold:
                    filtered_frame_indices.add(row['frame_index'])

        with open(objects_csv, 'r', newline='', encoding='utf-8') as objcsvfile, \
                open(captions_csv, 'r', newline='', encoding='utf-8') as captcsvfile, \
                open(output_csv, 'w', newline='', encoding='utf-8') as outcsvfile:

            obj_reader = csv.reader(objcsvfile)
            capt_reader = csv.reader(captcsvfile)
            writer = csv.writer(outcsvfile)

            obj_header = next(obj_reader)
            capt_header = next(capt_reader)
            writer.writerow(capt_header + obj_header[1:])

            for capt_row in capt_reader:
                if capt_row[0] in filtered_frame_indices:
                    obj_row = next(obj_reader)
                    writer.writerow(capt_row + obj_row[1:])

        self.logger.info(f"Caption filtering complete for {self.video_runner_obj['video_id']}")