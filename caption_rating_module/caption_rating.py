from utils import CAPTION_SCORE, return_video_folder_name,CAPTION_IMAGE_PAIR,OBJECTS_CSV,CAPTIONS_CSV,CAPTIONS_AND_OBJECTS_CSV
import csv
import requests
import os
class CaptionRating:
    """
    Class for rating captions based on an API and processing the data.
    """
    def __init__(self, video_runner_obj):
        """
        Initialize the CaptionRating object with the video_runner_obj.

        Parameters:
            video_runner_obj (obj): Object containing video information.
        """
        self.video_runner_obj = video_runner_obj
    
    
    def get_caption_rating(self, image_data):
        """
        Get the rating for a single caption.

        Parameters:
            image_data (dict): Dictionary containing information about a single frame and its caption.

        Returns:
            str: Rating for the given caption.
        """
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
                print("Server returned status {}.".format(response.status_code))
            return response.text.lstrip("['").rstrip("']")
        except:
            response = requests.post(page, data=multipart_form_data)
            if response.status_code != 200:
                print("Server returned status {}.".format(response.status_code))
            return response.text.lstrip("['").rstrip("']")
        

    def get_all_caption_rating(self):
        """
        This method calculates the rating for all captions in the image_caption_csv_file
        and writes the results to the output_csv_file.
        """
        output_csv = []
        image_caption_csv_file = return_video_folder_name(self.video_runner_obj)+'/'+CAPTION_IMAGE_PAIR
        with open(image_caption_csv_file, 'r', newline='', encoding='utf-8') as captcsvfile:
            data = csv.DictReader(captcsvfile)
            for image_data in data:
                rating = self.get_caption_rating(image_data)
                output_csv.append({'frame_index': image_data['frame_index'], 'frame_url': image_data['frame_url'], 
                                'caption': image_data['caption'], 'rating': rating})

        output_csv_file = return_video_folder_name(self.video_runner_obj)+'/'+CAPTION_SCORE
        with open(output_csv_file, 'w', newline='', encoding='utf-8') as captcsvfile:
            csv_writer = csv.writer(captcsvfile)
            header = ['frame_index', 'frame_url', 'caption', 'rating']
            csv_writer.writerow(header)
            for data in output_csv:
                csv_writer.writerow(data.values())
    
    def filter_captions(self):
        """
        This method filters the captions based on the rating scores, which are calculated and stored in a separate csv file,
        and outputs the filtered captions and object detections in a new csv file.

        Returns:
            None
        """
        caption_filter_csv = return_video_folder_name(self.video_runner_obj)+'/'+CAPTION_SCORE
        with open(caption_filter_csv, newline='', encoding='utf-8') as caption_filter_file:
            data = list(csv.DictReader(caption_filter_file))
            filtered_list = [x['frame_index'] for x in data if float(x['rating']) > int(os.getenv('CAPTION_RATING_THRESHOLD'))]

        objcsvpath = return_video_folder_name(self.video_runner_obj)+'/'+OBJECTS_CSV
        with open(objcsvpath, newline='', encoding='utf-8') as objcsvfile:
            reader = csv.reader(objcsvfile)
            objheader = next(reader) # skip header
            objrows = [row for row in reader]

        captcsvpath = return_video_folder_name(self.video_runner_obj)+'/'+CAPTIONS_CSV
        with open(captcsvpath, newline='', encoding='utf-8') as captcsvfile:
            reader = csv.reader(captcsvfile)
            captheader = next(reader) # skip header
            captrows = [row for row in reader if row[0] in filtered_list]

        outcsvpath = return_video_folder_name(self.video_runner_obj)+'/'+CAPTIONS_AND_OBJECTS_CSV
        with open(outcsvpath, 'w', newline='', encoding='utf-8') as outcsvfile:
            writer = csv.writer(outcsvfile)
            header = captheader + objheader[1:]
            writer.writerow(header)
            for index in range(len(objrows)):
                try:
                    new_row = captrows[index] + objrows[index][1:]
                    writer.writerow(new_row)
                except:
                    continue

