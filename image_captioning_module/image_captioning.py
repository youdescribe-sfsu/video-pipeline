import csv
import os

import requests

from timeit_decorator import timeit
from utils import (CAPTIONS_AND_OBJECTS_CSV, CAPTIONS_CSV,
                   FRAME_INDEX_SELECTOR, IS_KEYFRAME_SELECTOR,
                   KEY_FRAME_HEADERS, KEYFRAME_CAPTION_SELECTOR, KEYFRAMES_CSV,
                   OBJECTS_CSV, TIMESTAMP_SELECTOR, returnVideoFolderName,
                   returnVideoFramesFolder)


class ImageCaptioning:
    def __init__(self, video_id,pagePort):
        self.video_id = video_id
        self.pagePort = pagePort
    
    def get_caption(filename):
        """
        Gets a caption from the server given an image filename
        """
        page = 'http://localhost:{}/upload'.format(os.getenv('GPU_LOCAL_PORT') or '5000')
        token = 'VVcVcuNLTwBAaxsb2FRYTYsTnfgLdxKmdDDxMQLvh7rac959eb96BCmmCrAY7Hc3'
        fileBuffer = open(filename, 'rb')
        multipart_form_data = {
            'token': ('', str(token)),
            'img_file': (os.path.basename(filename), fileBuffer),
        }
        try:
            response = requests.post(page, files=multipart_form_data)
            fileBuffer.close()
            if response.status_code != 200:
                print("Server returned status {}.".format(response.status_code))
                return []
            return response.text
        except:
            response = requests.post(page, files=multipart_form_data)
            fileBuffer.close()
            if response.status_code != 200:
                print("Server returned status {}.".format(response.status_code))
                return []
            return response.text
        
    # def get_all_captions(self,video_name):
    #     """
    #     Gets a caption for each extracted frame and returns a list of frame indices
    #     and the corresponding captions
    #     """
    #     captions = []
    #     with open('{}/data.txt'.format(video_name), 'r') as datafile:
    #         data = datafile.readline().split()
    #         step = int(data[0])
    #         num_frames = int(data[1])
        
    #     for frame_index in range(0, num_frames, step):
    #         frame_filename = '{}/frame_{}.jpg'.format(video_name, frame_index)
    #         caption = self.get_caption(frame_filename)
    #         print(frame_index, caption)
    #         captions.append((frame_index, caption))
        
    #     return captions
    

    def image_caption_to_csv(self):
        """
        Gets a caption for each extracted frame and writes it to a csv file along with
        the frame index and a boolean indicating whether the frame is a keyframe or not
        """
        video_frames_path = returnVideoFramesFolder(self.video_id)
        video_folder_path = returnVideoFolderName(self.video_id)
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
                    start = int(last_line)+step
                    file.close()

        mode = 'w'
        if start != 0:
            mode = 'a'

        with open(outcsvpath, mode, newline='', encoding='utf-8') as outcsvfile:
            writer = csv.writer(outcsvfile)
            if start == 0:
                writer.writerow([KEY_FRAME_HEADERS[FRAME_INDEX_SELECTOR],KEY_FRAME_HEADERS[TIMESTAMP_SELECTOR],KEY_FRAME_HEADERS[IS_KEYFRAME_SELECTOR],KEY_FRAME_HEADERS[KEYFRAME_CAPTION_SELECTOR]])
            for frame_index in range(start, num_frames, step):
                frame_filename = '{}/frame_{}.jpg'.format(video_frames_path, frame_index)
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
    
    def combine_captions_objects(self):
        """
        Outputs a csv file combining the columns of the object and caption csv files
        """
        objcsvpath = returnVideoFolderName(self.video_id)+'/'+OBJECTS_CSV
        with open(objcsvpath, newline='', encoding='utf-8') as objcsvfile:
            reader = csv.reader(objcsvfile)
            objheader = next(reader) # skip header
            objrows = [row for row in reader]
        
        captcsvpath = returnVideoFolderName(self.video_id)+'/'+CAPTIONS_CSV
        with open(captcsvpath, newline='', encoding='utf-8') as captcsvfile:
            reader = csv.reader(captcsvfile)
            captheader = next(reader) # skip header
            captrows = [row for row in reader]
        
        outcsvpath = returnVideoFolderName(self.video_id)+'/'+CAPTIONS_AND_OBJECTS_CSV
        with open(outcsvpath, 'w', newline='', encoding='utf-8') as outcsvfile:
            writer = csv.writer(outcsvfile)
            header = captheader + objheader[1:]
            writer.writerow(header)
            for index in range(len(objrows)):
                try:
                    new_row = captrows[index] + objrows[index][1:]
                    print(captrows[index])
                    writer.writerow(new_row)
                except:
                    continue