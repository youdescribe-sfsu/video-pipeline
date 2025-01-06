import csv
import requests
import os
from typing import Dict, Any

from pipeline_module.utils_module.utils import return_video_folder_name, CAPTION_SCORE, OBJECTS_CSV, CAPTIONS_CSV, \
    CAPTIONS_AND_OBJECTS_CSV, CAPTION_IMAGE_PAIR
from web_server_module.web_server_database import get_status_for_youtube_id, update_status, update_module_output, \
    get_module_output


class CaptionRating:
    def __init__(self, video_runner_obj: Dict[str, Any]):
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        self.caption_rating_threshold = float(os.getenv('CAPTION_RATING_THRESHOLD', '0.5'))
        self.caption_rating_service = os.getenv('CAPTION_RATING_SERVICE', '8082')

    def perform_caption_rating(self) -> bool:
        if get_status_for_youtube_id(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"]) == "done":
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

    def get_caption_rating(self, image_data: Dict[str, str]) -> str:
        token = 'VVcVcuNLTwBAaxsb2FRYTYsTnfgLdxKmdDDxMQLvh7rac959eb96BCmmCrAY7Hc3'
        multipart_form_data = {
            'token': token,
            'img_url': image_data['frame_url'],
            'caption': image_data['caption']
        }
        page = os.getenv('RATING_SERVICE_URL')

        try:
            response = requests.post(page, data=multipart_form_data)
            response.raise_for_status()  # Check for HTTP errors
            rating = response.text.lstrip("['").rstrip("']")  # Clean the response
            self.logger.info(f"Received response: {response.text}")  # Log the full response
            return rating
        except requests.RequestException as e:
            self.logger.error(f"Error in caption rating request: {str(e)}")
            return "0.0"
        except ValueError as e:
            self.logger.error(f"Invalid rating value received: {response.text}")
            return "0.0"

    def get_all_caption_rating(self) -> None:
        """
        Retrieves data from the database instead of using file-based operations.
        Saves progress in the database as well.
        """
        image_caption_csv_file = return_video_folder_name(self.video_runner_obj) + '/' + CAPTION_IMAGE_PAIR
        output_csv_file = return_video_folder_name(self.video_runner_obj) + '/' + CAPTION_SCORE

        # Fetch processed frame indices from the database
        processed_frame_indices = self.get_processed_frame_indices_from_db()
        if processed_frame_indices is None:
            processed_frame_indices = []

        if not os.path.exists(output_csv_file):
            header = ['frame_index', 'frame_url', 'caption', 'rating']
            with open(output_csv_file, 'w', newline='', encoding='utf-8') as output_csvfile:
                csv_writer = csv.writer(output_csvfile)
                csv_writer.writerow(header)

        with open(image_caption_csv_file, 'r', newline='', encoding='utf-8') as captcsvfile:
            data = csv.DictReader(captcsvfile)

            with open(output_csv_file, 'a', newline='', encoding='utf-8') as output_csvfile:
                csv_writer = csv.writer(output_csvfile)

                for image_data in data:
                    frame_index = int(image_data['frame_index'])

                    if frame_index in processed_frame_indices:
                        continue  # Skip already processed frames

                    rating = self.get_caption_rating(image_data)
                    self.logger.info(f"Rating for caption '{image_data['caption']}' is {rating}")
                    print(f"Rating for caption '{image_data['caption']}' is {rating}")

                    row = [frame_index, image_data['frame_url'], image_data['caption'], rating]
                    csv_writer.writerow(row)

                    processed_frame_indices.append(frame_index)
                    self.save_processed_frame_indices_to_db(processed_frame_indices)

        # Save progress to the database once the ratings are completed
        update_module_output(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"], 'caption_rating',
                             {"ratings": "completed"})

    def filter_captions(self) -> None:
        """
        Filters captions based on the previous behavior, while ensuring that the process is efficient.
        """
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
        # Save progress completion to the database
        update_status(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"], 'done')

    # Replaces read_value_from_file - Fetch processed frame indices from the database
    def get_processed_frame_indices_from_db(self) -> list:
        """
        Retrieves the list of processed frame indices from the database.
        """
        try:
            module_output = get_module_output(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"],
                                              'caption_rating')
            return module_output.get('processed_frame_indices', [])
        except Exception as e:
            self.logger.error(f"Error fetching processed frame indices: {str(e)}")
            return []

    # Replaces save_value_to_file - Save processed frame indices to the database
    def save_processed_frame_indices_to_db(self, processed_frame_indices: list) -> None:
        """
        Saves the updated list of processed frame indices to the database.
        """
        try:
            update_module_output(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"],
                                 'caption_rating', {
                                     'processed_frame_indices': processed_frame_indices
                                 })
        except Exception as e:
            self.logger.error(f"Error saving processed frame indices: {str(e)}")