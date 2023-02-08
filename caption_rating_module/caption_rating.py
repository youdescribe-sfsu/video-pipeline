## TODO : Implement the CaptionRating class
from utils import return_video_folder_name,return_video_frames_folder,CAPTION_IMAGE_PAIR
import csv
import requests
import os
class CaptionRating:
    def __init__(self, video_runner_obj,):
        self.video_runner_obj = video_runner_obj
    
    def get_caption_rating(self):
        # video_frames_path = return_video_frames_folder(self.video_runner_obj)
        # video_folder_path = return_video_folder_name(self.video_runner_obj)
        image_caption_csv_file = return_video_folder_name(self.video_runner_obj)+'/'+CAPTION_IMAGE_PAIR
        with open(image_caption_csv_file, 'w', newline='', encoding='utf-8') as captcsvfile:
            data = csv.DictReader(captcsvfile)
        multipart_form_data = {
            'image_captio_pair': data,
        }
        page = 'http://localhost:{}/upload'.format(os.getenv('CAPTION_RATING_SERVICE') or '8082')
        try:
            response = requests.post(page, files=multipart_form_data)
            if response.status_code != 200:
                print("Server returned status {}.".format(response.status_code))
                return []
            return response.text
        except:
            response = requests.post(page, files=multipart_form_data)
            if response.status_code != 200:
                print("Server returned status {}.".format(response.status_code))
                return []
            return response.text