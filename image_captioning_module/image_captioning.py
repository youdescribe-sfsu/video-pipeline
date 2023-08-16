import csv
import os
import shutil

import requests

from timeit_decorator import timeit
from utils import (CAPTIONS_CSV,
                   FRAME_INDEX_SELECTOR, IS_KEYFRAME_SELECTOR,
                   KEY_FRAME_HEADERS, KEYFRAME_CAPTION_SELECTOR, KEYFRAMES_CSV,
                   TIMESTAMP_SELECTOR, load_progress_from_file, read_value_from_file, return_video_folder_name,
                   return_video_frames_folder,CAPTION_IMAGE_PAIR, save_progress_to_file, save_value_to_file)
import json
import socket
class ImageCaptioning:
    def __init__(self, video_runner_obj):
        """
        Initialize ImportVideo object.
        
        Parameters:
        video_runner_obj (Dict[str, int]): A dictionary that contains the information of the video.
            The keys are "video_id", "video_start_time", and "video_end_time", and their values are integers.
        """
        self.video_runner_obj = video_runner_obj
        # self.save_file = load_progress_from_file(video_runner_obj=self.video_runner_obj)
        pass
    


    def get_caption(self, filename):
        """
        Gets a caption from the server given an image filename
        """
        page = 'http://localhost:{}/upload'.format(os.getenv('GPU_LOCAL_PORT') or '8085')
        token = 'VVcVcuNLTwBAaxsb2FRYTYsTnfgLdxKmdDDxMQLvh7rac959eb96BCmmCrAY7Hc3'
        
        caption_img = ""
        fileBuffer = None
        try:
            fileBuffer = open(filename, 'rb')
            multipart_form_data = {
                'token': ('', str(token)),
                'image': (os.path.basename(filename), fileBuffer),
            }
            
            self.video_runner_obj["logger"].info(f"Running image captioning for {filename}")
            self.video_runner_obj["logger"].info(f"multipart_form_data: {multipart_form_data}")
            
            response = requests.post(page, files=multipart_form_data)
            if response.status_code == 200:
                json_obj = response.json()
                caption_img = json_obj['caption']
            else:
                self.video_runner_obj["logger"].info(f"Server returned status {response.status_code}")
        except requests.exceptions.RequestException as e:
            self.video_runner_obj["logger"].info(f"Exception occurred during the request: {str(e)}")
        finally:
            # Close the socket if it's still open
            if fileBuffer is not None:
                fileBuffer.close()
        
        self.video_runner_obj["logger"].info(f"caption: {caption_img}")
        return caption_img.strip()


    
    @timeit
    def run_image_captioning(self):
        """
        Gets a caption for each extracted frame and writes it to a csv file along with
        the frame index and a boolean indicating whether the frame is a keyframe or not
        """
        video_frames_path = return_video_frames_folder(self.video_runner_obj)
        video_folder_path = return_video_folder_name(self.video_runner_obj)
        # self.save_file = load_progress_from_file(video_runner_obj=self.video_runner_obj)
        
        # if self.save_file['ImageCaptioning']['started'] == 'done':
        if read_value_from_file(video_runner_obj=self.video_runner_obj,key="['ImageCaptioning']['started']") == 'done':
            self.video_runner_obj["logger"].info("Image captioning already done")
            return
        
        
        # if self.save_file['ImageCaptioning']['started'] == True:
        if read_value_from_file(video_runner_obj=self.video_runner_obj,key="['ImageCaptioning']['started']") == True:
            # last_processed_frame = self.save_file['ImageCaptioning']['run_image_captioning']['last_processed_frame']
            last_processed_frame = read_value_from_file(video_runner_obj=self.video_runner_obj,key="['ImageCaptioning']['run_image_captioning']['last_processed_frame']")
            # dropped_key_frames = self.save_file['ImageCaptioning']['dropped_key_frames']
            dropped_key_frames = read_value_from_file(video_runner_obj=self.video_runner_obj,key="['ImageCaptioning']['dropped_key_frames']")
        else:
            last_processed_frame = 0
            dropped_key_frames = 0
            # self.save_file['ImageCaptioning']['started'] = True
            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['ImageCaptioning']['started']", value=True)
            
            
        # step = self.save_file['video_common_values']['step']
        step = read_value_from_file(video_runner_obj=self.video_runner_obj,key="['video_common_values']['step']")
        # num_frames = self.save_file['video_common_values']['num_frames']
        num_frames = read_value_from_file(video_runner_obj=self.video_runner_obj,key="['video_common_values']['num_frames']")
        # frames_per_second = self.save_file['video_common_values']['frames_per_second']
        frames_per_second = read_value_from_file(video_runner_obj=self.video_runner_obj,key="['video_common_values']['frames_per_second']")
        
        ## Get this from first column of Keyframe.csv
        # frames_to_process = list(range(last_processed_frame + step, num_frames, step))
        with open(video_folder_path + '/'+ KEYFRAMES_CSV, newline='', encoding='utf-8') as incsvfile:
            ## Get all values of first column
            reader = csv.reader(incsvfile)
            header = next(reader) # skip header
            keyframes = [int(row[0]) for row in reader]
        frames_to_process = keyframes
        
        ## Remove all frames that have already been processed
        frames_to_process = [frame for frame in frames_to_process if frame > last_processed_frame]
        
        
        print("Frames to process: ", frames_to_process)
        
        
        # self.save_file['ImageCaptioning']['dropped_key_frames'] = dropped_key_frames
        
        
        
        # with open('{}/data.txt'.format(video_frames_path), 'r') as datafile:
        #     data = datafile.readline().split()
        #     step = int(data[0])
        #     num_frames = int(data[1])
        #     frames_per_second = float(data[2])
        video_fps = step * frames_per_second
        seconds_per_frame = 1.0/video_fps
        
        # with open(video_folder_path + '/'+ KEYFRAMES_CSV, newline='', encoding='utf-8') as incsvfile:
        #     reader = csv.reader(incsvfile)
        #     header = next(reader) # skip header
        #     keyframes = [int(row[0]) for row in reader]
        
        # start = 0
        outcsvpath = video_folder_path + '/' + CAPTIONS_CSV
        mode = 'w' if last_processed_frame == 0 else 'a'
        
        
        
        # index_start_value = 0
        # try:
        #     index_start_value = keyframes.index(start)
        # except ValueError:
        #     index_start_value = 0
        with open(outcsvpath, mode, newline='', encoding='utf-8') as outcsvfile:
            writer = csv.writer(outcsvfile)
            if last_processed_frame == 0:
                writer.writerow([KEY_FRAME_HEADERS[FRAME_INDEX_SELECTOR], KEY_FRAME_HEADERS[TIMESTAMP_SELECTOR], KEY_FRAME_HEADERS[IS_KEYFRAME_SELECTOR], KEY_FRAME_HEADERS[KEYFRAME_CAPTION_SELECTOR]])

            for frame_index in frames_to_process:
                frame_filename = '{}/frame_{}.jpg'.format(video_frames_path, frame_index)
                caption = self.get_caption(frame_filename)

                if type(caption) == str:
                    row = [frame_index, float(frame_index) * seconds_per_frame, False, caption]
                    writer.writerow(row)
                
                print("Frame index: ", frame_index, " Caption: ", caption)
                # elif frame_index in keyframes:
                #     dropped_key_frames += 1

                outcsvfile.flush()
                # self.save_file['ImageCaptioning']['run_image_captioning']['last_processed_frame'] = frame_index
                save_value_to_file(video_runner_obj=self.video_runner_obj, key="['ImageCaptioning']['run_image_captioning']['last_processed_frame']", value=frame_index)
                # self.save_file['ImageCaptioning']['dropped_key_frames'] = dropped_key_frames
                save_value_to_file(video_runner_obj=self.video_runner_obj, key="['ImageCaptioning']['dropped_key_frames']", value=dropped_key_frames)
                # save_progress_to_file(video_runner_obj=self.video_runner_obj, progress_data=self.save_file)

            # self.save_file['ImageCaptioning']['run_image_captioning']['last_processed_frame'] = frames_to_process[-1]
            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['ImageCaptioning']['run_image_captioning']['last_processed_frame']", value=frames_to_process[-1])
            # self.save_file['ImageCaptioning']['started'] = 'done'
            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['ImageCaptioning']['started']", value='done')
            # self.save_file['ImageCaptioning']['dropped_key_frames'] = dropped_key_frames
            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['ImageCaptioning']['dropped_key_frames']", value=dropped_key_frames)

            # save_progress_to_file(video_runner_obj=self.video_runner_obj, progress_data=self.save_file)
            self.video_runner_obj["logger"].info("============================================")
            # self.video_runner_obj["logger"].info('Dropped %d keyframes', dropped_key_frames)
            # self.video_runner_obj["logger"].info('Total keyframes: %d', len(keyframes))
            self.video_runner_obj["logger"].info('============================================')
        return
    
    def filter_keyframes_from_caption(self):
        # save_file = load_progress_from_file(video_runner_obj=self.video_runner_obj)
        # if(save_file['ImageCaptioning']['filter_keyframes_from_caption'] == 1):
        if read_value_from_file(video_runner_obj=self.video_runner_obj,key="['ImageCaptioning']['filter_keyframes_from_caption']") == 1:
            ## Already done, skipping step
            self.video_runner_obj["logger"].info("Filtering keyframes from caption already done, skipping step.")
            print("Filtering keyframes from caption already done, skipping step.")
            return
            
            
        video_folder_path = return_video_folder_name(self.video_runner_obj)
        with open(video_folder_path + '/'+ KEYFRAMES_CSV, newline='', encoding='utf-8') as incsvfile:
            reader = csv.reader(incsvfile)
            header = next(reader) # skip header
            
            
            keyframes = [int(row[0]) for row in reader]
            
            
            csv_file_path = video_folder_path + '/' + CAPTIONS_CSV
            updated_rows = []
            # Iterate through the rows and update 'Is Keyframe' column
            with open(csv_file_path, 'r', newline='', encoding='utf-8') as csv_file:
                csv_reader = csv.reader(csv_file)
                header = next(csv_reader)  # Skip header
                updated_rows.append(header)

                for row in csv_reader:
                    ## row[0] is frame index
                    ## row[2] is is_keyframe
                    ## row[3] is caption
                    frame_index = int(row[0])
                    is_keyframe = True if frame_index in keyframes else False
                    row[2] = is_keyframe  # Update 'Is Keyframe' column
                    updated_rows.append(row)

            # Remove rows with '<unk>' in the caption
            final_rows = [row for row in updated_rows if '<unk>' not in row[3]]  # Assuming caption is in index 3

            # Save the updated data to the same CSV file
            temp_csv_path = "temp_updated_csv_file.csv"
            with open(temp_csv_path, 'w', newline='', encoding='utf-8') as temp_csv_file:
                csv_writer = csv.writer(temp_csv_file)
                csv_writer.writerows(final_rows)

            # Overwrite the original CSV file with the updated data
            shutil.move(temp_csv_path, csv_file_path)
        # save_file['ImageCaptioning']['filter_keyframes_from_caption'] = 1
        # save_progress_to_file(video_runner_obj=self.video_runner_obj, progress_data=save_file)
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['ImageCaptioning']['filter_keyframes_from_caption']", value=1)
        return
            
    
    def combine_image_caption(self):
        """
        Outputs a csv file combining the columns of the object and caption csv files
        """
        # objcsvpath = return_video_folder_name(self.video_runner_obj)+'/'+OBJECTS_CSV
        # with open(objcsvpath, newline='', encoding='utf-8') as objcsvfile:
        #     reader = csv.reader(objcsvfile)
        #     objheader = next(reader) # skip header
        #     objrows = [row for row in reader]
        
        captcsvpath = return_video_folder_name(self.video_runner_obj)+'/'+CAPTIONS_CSV
        # with open(captcsvpath, newline='', encoding='utf-8') as captcsvfile:
        #     reader = csv.reader(captcsvfile)
        #     captheader = next(reader) # skip header
        #     captrows = [row for row in reader]
        # if(self.save_file['ImageCaptioning']['combine_image_caption'] == 0):
        if read_value_from_file(video_runner_obj=self.video_runner_obj,key="['ImageCaptioning']['combine_image_caption']") == 0:
            # return
            ## Write Image Caption Pair to CSV
            with open(captcsvpath, 'r', newline='', encoding='utf-8') as captcsvfile:
                data = csv.DictReader(captcsvfile)
                video_frames_path = return_video_frames_folder(self.video_runner_obj)
                image_caption_pairs = list(map(lambda row: {"frame_index":row[KEY_FRAME_HEADERS[FRAME_INDEX_SELECTOR]],"frame_url":'{}/frame_{}.jpg'.format(video_frames_path, row[KEY_FRAME_HEADERS[FRAME_INDEX_SELECTOR]]),"caption":row[KEY_FRAME_HEADERS[KEYFRAME_CAPTION_SELECTOR]]}, data))
                self.video_runner_obj["logger"].info(f"Writing image caption pairs to {return_video_folder_name(self.video_runner_obj)+'/'+CAPTION_IMAGE_PAIR}")
                image_caption_csv_file = return_video_folder_name(self.video_runner_obj)+'/'+CAPTION_IMAGE_PAIR
                with open(image_caption_csv_file, 'w', encoding='utf8', newline='') as output_file:
                    csvDictWriter = csv.DictWriter(output_file, fieldnames=image_caption_pairs[0].keys())
                    csvDictWriter.writeheader()
                    csvDictWriter.writerows(image_caption_pairs)
                # self.save_file['ImageCaptioning']['combine_image_caption'] = 1
                save_value_to_file(video_runner_obj=self.video_runner_obj, key="['ImageCaptioning']['combine_image_caption']", value=1)
                # save_progress_to_file(video_runner_obj=self.video_runner_obj, progress_data=self.save_file)
                ## Completed Writing Image Caption Pair to CSV
                self.video_runner_obj["logger"].info(f"Completed Writing Image Caption Pair to CSV")
                
                self.video_runner_obj["logger"].info(f"Uploading image caption pairs to server")
                print(f"Uploading image caption pairs to server")
                return
        else:
            print("Image Captioning already done")
            self.video_runner_obj["logger"].info(f"Image Captioning already done")
            return
            
            
        # outcsvpath = return_video_folder_name(self.video_runner_obj)+'/'+CAPTIONS_AND_OBJECTS_CSV
        # with open(outcsvpath, 'w', newline='', encoding='utf-8') as outcsvfile:
        #     writer = csv.writer(outcsvfile)
        #     header = captheader + objheader[1:]
        #     writer.writerow(header)
        #     for index in range(len(objrows)):
        #         try:
        #             new_row = captrows[index] + objrows[index][1:]
        #             print(captrows[index])
        #             writer.writerow(new_row)
        #         except:
        #             continue