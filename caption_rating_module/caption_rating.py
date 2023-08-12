from utils import CAPTION_SCORE, load_progress_from_file, return_video_folder_name,CAPTION_IMAGE_PAIR,OBJECTS_CSV,CAPTIONS_CSV,CAPTIONS_AND_OBJECTS_CSV, save_progress_to_file
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
    
    def perform_caption_rating(self):
        """
        This method calls the get_all_caption_rating() and filter_captions() methods.
        """
        save_file = load_progress_from_file(video_runner_obj=self.video_runner_obj)
        if save_file['CaptionRating']['started'] == 'done':
            ## Already processed
            self.video_runner_obj["logger"].info("Already processed")
            return
        else:
            self.get_all_caption_rating()
            self.filter_captions()
        return
        
    
    
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
                self.video_runner_obj["logger"].info("Server returned status {}.".format(response.status_code))
            return response.text.lstrip("['").rstrip("']")
        except:
            response = requests.post(page, data=multipart_form_data)
            if response.status_code != 200:
                self.video_runner_obj["logger"].info("Server returned status {}.".format(response.status_code))
            return response.text.lstrip("['").rstrip("']")
        

    def get_all_caption_rating(self):
        """
        This method calculates the rating for all captions in the image_caption_csv_file
        and writes the results to the output_csv_file.
        """
        image_caption_csv_file = return_video_folder_name(self.video_runner_obj) + '/' + CAPTION_IMAGE_PAIR
        output_csv_file = return_video_folder_name(self.video_runner_obj) + '/' + CAPTION_SCORE

        self.save_file = load_progress_from_file(video_runner_obj=self.video_runner_obj)
        
        if self.save_file['CaptionRating']['get_all_caption_rating'] == 1:
            ## Already processed
            self.video_runner_obj["logger"].info("Already processed")
            return

        processed_frame_indices = self.save_file.get('CaptionRating', {}).get('processed_frame_indices', [])
        
        # Check if the output file exists, create it if not
        if not os.path.exists(output_csv_file):
            header = ['frame_index', 'frame_url', 'caption', 'rating']
            with open(output_csv_file, 'w', newline='', encoding='utf-8') as output_csvfile:
                csv_writer = csv.writer(output_csvfile)
                csv_writer.writerow(header)

        with open(image_caption_csv_file, 'r', newline='', encoding='utf-8') as captcsvfile:
            data = csv.DictReader(captcsvfile)
            
            # Open the output CSV in append mode
            with open(output_csv_file, 'a', newline='', encoding='utf-8') as output_csvfile:
                csv_writer = csv.writer(output_csvfile)
                
                for image_data in data:
                    frame_index = int(image_data['frame_index'])
                    
                    if frame_index in processed_frame_indices:
                        continue  # Skip already processed frames

                    rating = self.get_caption_rating(image_data)
                    self.video_runner_obj["logger"].info(f"Rating for caption {image_data['caption']} is {rating}")

                    row = [frame_index, image_data['frame_url'], image_data['caption'], rating]
                    csv_writer.writerow(row)
                    
                    # Update the processed_frame_indices and progress data
                    processed_frame_indices.append(frame_index)
                    self.save_file.setdefault('CaptionRating', {})['processed_frame_indices'] = processed_frame_indices
                    save_progress_to_file(video_runner_obj=self.video_runner_obj, progress_data=self.save_file)

        # Mark the processing as complete
        self.save_file.setdefault('CaptionRating', {})['get_all_caption_rating'] = 1
        save_progress_to_file(video_runner_obj=self.video_runner_obj, progress_data=self.save_file)
        return
        
    
    def filter_captions(self):
        """
        This method filters the captions based on the rating scores, which are calculated and stored in a separate csv file,
        and outputs the filtered captions and object detections in a new csv file.

        Returns:
            None
        """
        save_file = load_progress_from_file(video_runner_obj=self.video_runner_obj)
        
        
        if save_file['CaptionRating']['filter_captions'] == 1:
            ## Already processed
            self.video_runner_obj["logger"].info("Already processed")
            return
                
        
        caption_filter_csv = return_video_folder_name(self.video_runner_obj)+'/'+CAPTION_SCORE
        with open(caption_filter_csv, newline='', encoding='utf-8') as caption_filter_file:
            data = list(csv.DictReader(caption_filter_file))
            filtered_list = [x['frame_index'] for x in data if float(x['rating']) > float(os.getenv('CAPTION_RATING_THRESHOLD'))]

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
        self.video_runner_obj["logger"].info(f"Caption filtering complete for {self.video_runner_obj['video_id']}")
        save_file['CaptionRating']['filter_captions'] = 0
        save_file['CaptionRating']['started'] = 'done'
        save_progress_to_file(video_runner_obj=self.video_runner_obj, progress_data=save_file)
        return

