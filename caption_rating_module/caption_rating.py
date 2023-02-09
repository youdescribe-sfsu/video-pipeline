## TODO : Implement the CaptionRating class
from utils import CAPTION_SCORE, return_video_folder_name,CAPTION_IMAGE_PAIR,OBJECTS_CSV,CAPTIONS_CSV,CAPTIONS_AND_OBJECTS_CSV
import csv
import requests
import os
class CaptionRating:
    def __init__(self, video_runner_obj,):
        self.video_runner_obj = video_runner_obj
    
    
    def get_caption_rating(self,image_data):
        token = 'VVcVcuNLTwBAaxsb2FRYTYsTnfgLdxKmdDDxMQLvh7rac959eb96BCmmCrAY7Hc3'
        multipart_form_data = {
            'token': token,
            'img_url':image_data['frame_url'],
            'caption':image_data['caption']
        }
        print(multipart_form_data)
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
        output_csv = []
        image_caption_csv_file = return_video_folder_name(self.video_runner_obj)+'/'+CAPTION_IMAGE_PAIR
        with open(image_caption_csv_file, 'r', newline='', encoding='utf-8') as captcsvfile:
            data = csv.DictReader(captcsvfile)
            for image_data in data:
                    rating = self.get_caption_rating(image_data)
                    output_csv.append({'frame_index':image_data['frame_index'],'frame_url':image_data['frame_url'],'caption':image_data['caption'],'rating':rating})

        output_csv_file = return_video_folder_name(self.video_runner_obj)+'/'+CAPTION_SCORE
        print(output_csv)
        with open(output_csv_file, 'w', newline='', encoding='utf-8') as captcsvfile:
            csv_writer = csv.writer(captcsvfile) 
            count = 0
            for data in output_csv:
                if count == 0:
                    header = data.keys()
                    csv_writer.writerow(header)
                    count += 1
                csv_writer.writerow(data.values())
    
    def filter_captions(self):
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
