import csv
import os

import requests

from timeit_decorator import timeit
from utils import (CAPTIONS_AND_OBJECTS_CSV, CAPTIONS_CSV,
                   FRAME_INDEX_SELECTOR, IS_KEYFRAME_SELECTOR,
                   KEY_FRAME_HEADERS, KEYFRAME_CAPTION_SELECTOR, KEYFRAMES_CSV,
                   OBJECTS_CSV, TIMESTAMP_SELECTOR, return_video_folder_name,
                   return_video_frames_folder,CAPTION_IMAGE_PAIR)
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
            
            print("multipart_form_data", multipart_form_data)
            
            response = requests.post(page, files=multipart_form_data)
            if response.status_code == 200:
                json_obj = response.json()
                caption_img = json_obj['caption']
            else:
                print("Server returned status {}.".format(response.status_code))
        except requests.exceptions.RequestException as e:
            print("Exception occurred during the request:", str(e))
        finally:
            # Close the socket if it's still open
            if fileBuffer is not None:
                fileBuffer.close()
        
        print("caption:", caption_img)
        return caption_img.strip()


    

    def run_image_captioning(self):
        """
        Gets a caption for each extracted frame and writes it to a csv file along with
        the frame index and a boolean indicating whether the frame is a keyframe or not
        """
        video_frames_path = return_video_frames_folder(self.video_runner_obj)
        video_folder_path = return_video_folder_name(self.video_runner_obj)
        dropped_key_frames = 0
        with open('{}/data.txt'.format(video_frames_path), 'r') as datafile:
            data = datafile.readline().split()
            step = int(data[0])
            num_frames = int(data[1])
            frames_per_second = float(data[2])
        video_fps = step * frames_per_second
        seconds_per_frame = 1.0/video_fps
        
        with open(video_folder_path + '/'+ KEYFRAMES_CSV, newline='', encoding='utf-8') as incsvfile:
            reader = csv.reader(incsvfile)
            header = next(reader) # skip header
            keyframes = [int(row[0]) for row in reader]
        
        start = 0
        outcsvpath = video_folder_path + '/'+ CAPTIONS_CSV
        if os.path.exists(outcsvpath) :
            if os.stat(outcsvpath).st_size > 50:
                with open(outcsvpath, 'r', newline='', encoding='utf-8') as file:
                    lines = file.readlines()
                    lines.reverse()
                    i = 0
                    last_line = lines[i].split(",")[0]
                    while not last_line.isnumeric():
                        i+= 1
                        last_line = lines[i].split(",")[0]
                    start = int(last_line)
                    file.close()

        mode = 'w'
        if start != 0:
            mode = 'a'
        index_start_value = 0
        try:
            index_start_value = keyframes.index(start)
        except ValueError:
            index_start_value = 0
        with open(outcsvpath, mode, newline='', encoding='utf-8') as outcsvfile:
            writer = csv.writer(outcsvfile)
            if start == 0:
                writer.writerow([KEY_FRAME_HEADERS[FRAME_INDEX_SELECTOR],KEY_FRAME_HEADERS[TIMESTAMP_SELECTOR],KEY_FRAME_HEADERS[IS_KEYFRAME_SELECTOR],KEY_FRAME_HEADERS[KEYFRAME_CAPTION_SELECTOR]])
            if(os.getenv('CAPTION_ONLY_KEYFRAMES') == True):
                for frame_index in keyframes[index_start_value:]:
                    frame_filename = '{}/frame_{}.jpg'.format(video_frames_path, frame_index)
                    print("frame_filename: {}".format(frame_filename))
                    caption = self.get_caption(frame_filename)
                    print(frame_index, caption)
                    if(type(caption) == str and caption.find('<unk>') == -1):
                        row = [frame_index, float(frame_index) * seconds_per_frame, frame_index in keyframes, caption]
                        writer.writerow(row)
                    elif(frame_index in keyframes):
                        dropped_key_frames += 1
                        print("Dropped keyframe: {}".format(frame_index))
                    outcsvfile.flush()
            else:
                for frame_index in range(start, num_frames, step):
                    frame_filename = '{}/frame_{}.jpg'.format(video_frames_path, frame_index)
                    print("frame_filename: {}".format(frame_filename))
                    caption = self.get_caption(frame_filename)
                    print(frame_index, caption)
                    if(type(caption) == str and caption.find('<unk>') == -1):
                        row = [frame_index, float(frame_index) * seconds_per_frame, frame_index in keyframes, caption]
                        writer.writerow(row)
                    elif(frame_index in keyframes):
                        dropped_key_frames += 1
                        print("Dropped keyframe: {}".format(frame_index))
                    outcsvfile.flush()
            print("============================================")
            print('Dropped {} keyframes'.format(dropped_key_frames))
            print('Total keyframes: {}'.format(len(keyframes)))
            print('============================================')
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
        
        ## Write Image Caption Pair to CSV
        with open(captcsvpath, 'r', newline='', encoding='utf-8') as captcsvfile:
            data = csv.DictReader(captcsvfile)
            video_frames_path = return_video_frames_folder(self.video_runner_obj)
            image_caption_pairs = list(map(lambda row: {"frame_index":row[KEY_FRAME_HEADERS[FRAME_INDEX_SELECTOR]],"frame_url":'{}/frame_{}.jpg'.format(video_frames_path, row[KEY_FRAME_HEADERS[FRAME_INDEX_SELECTOR]]),"caption":row[KEY_FRAME_HEADERS[KEYFRAME_CAPTION_SELECTOR]]}, data))
            image_caption_csv_file = return_video_folder_name(self.video_runner_obj)+'/'+CAPTION_IMAGE_PAIR
            with open(image_caption_csv_file, 'w', encoding='utf8', newline='') as output_file:
                csvDictWriter = csv.DictWriter(output_file, fieldnames=image_caption_pairs[0].keys())
                csvDictWriter.writeheader()
                csvDictWriter.writerows(image_caption_pairs)
                
            
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