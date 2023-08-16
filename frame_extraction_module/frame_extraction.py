from timeit_decorator import timeit
from utils import read_value_from_file, return_video_download_location,  return_video_frames_folder,load_progress_from_file, return_video_progress_file, save_progress_to_file, save_value_to_file
import cv2
import os
import json
from typing import Dict

class FrameExtraction:
    def __init__(self, video_runner_obj: Dict[str, int], frames_per_second: int):
        self.video_runner_obj = video_runner_obj
        self.frames_per_second = frames_per_second
        self.logger = video_runner_obj.get("logger")
        # self.progress_file = load_progress_from_file(self.video_runner_obj)

    def extract_frames(self, logging=False):
        # Load progress from JSON file
        # save_progress = load_progress_from_file(self.video_runner_obj)
        # self.progress_file = load_progress_from_file(self.video_runner_obj)
        vid_name = return_video_frames_folder(self.video_runner_obj)
        
        # if(self.progress_file['FrameExtraction']['started'] == 'done'):
        if(read_value_from_file(video_runner_obj=self.video_runner_obj,key="['FrameExtraction']['started']") == 'done'):
            self.logger.info("Frames already extracted, skipping step.")
            return

        if not os.path.exists(vid_name):
            try:
                os.mkdir(vid_name)
            except OSError:
                self.logger.error("Cannot create directory for frames")
                return
        video_location = return_video_download_location(self.video_runner_obj)
        vid = cv2.VideoCapture(video_location)
            
        # if self.progress_file['FrameExtraction']['started']:
        if read_value_from_file(video_runner_obj=self.video_runner_obj,key="['FrameExtraction']['started']"):
            # fps = self.progress_file['FrameExtraction']['fps']
            fps = read_value_from_file(video_runner_obj=self.video_runner_obj,key="['FrameExtraction']['fps']")
            # frames_per_extraction = self.progress_file['FrameExtraction']['frames_per_extraction']
            frames_per_extraction = read_value_from_file(video_runner_obj=self.video_runner_obj,key="['FrameExtraction']['frames_per_extraction']")
            # num_frames = self.progress_file['FrameExtraction']['num_frames']
            num_frames = read_value_from_file(video_runner_obj=self.video_runner_obj,key="['FrameExtraction']['num_frames']")
            # actual_frames_per_second = self.progress_file['FrameExtraction']['actual_frames_per_second']
            actual_frames_per_second = read_value_from_file(video_runner_obj=self.video_runner_obj,key="['FrameExtraction']['actual_frames_per_second']")
        else:
            fps = round(vid.get(cv2.CAP_PROP_FPS))
            frames_per_extraction = round(fps / self.frames_per_second)
            num_frames = int(vid.get(cv2.CAP_PROP_FRAME_COUNT))
            actual_frames_per_second = fps / frames_per_extraction
            
            ## Get a fresh copy
            # progress_file_new = load_progress_from_file(self.video_runner_obj)
            # progress_file_new['FrameExtraction']['started'] = True
            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['started']", value=True)
            # progress_file_new['FrameExtraction']['fps'] = fps
            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['fps']", value=fps)
            # progress_file_new['FrameExtraction']['frames_per_extraction'] = frames_per_extraction
            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['frames_per_extraction']", value=frames_per_extraction)
            # progress_file_new['FrameExtraction']['num_frames'] = num_frames
            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['num_frames']", value=num_frames)
            # progress_file_new['FrameExtraction']['actual_frames_per_second'] = actual_frames_per_second
            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['actual_frames_per_second']", value=actual_frames_per_second)
            # save_progress_to_file(video_runner_obj=self.video_runner_obj, progress_data=progress_file_new)

        # last_extracted_frame = self.progress_file['FrameExtraction']['extract_frames']
        last_extracted_frame = read_value_from_file(video_runner_obj=self.video_runner_obj,key="['FrameExtraction']['extract_frames']")
        if last_extracted_frame >= num_frames:
            self.logger.info("Frames already extracted, skipping step.")
            return

        self.logger.info(f"Extracting frames from {self.video_runner_obj['video_id']} ({fps} fps, {num_frames} frames)...")

        frame_count = last_extracted_frame
        while frame_count < num_frames:
            status, frame = vid.read()
            if not status:
                self.logger.error("Error extracting frame {}.".format(frame_count))
                break
            if frame_count % frames_per_extraction == 0:
                frame_filename = "{}/frame_{}.jpg".format(vid_name, frame_count)
                cv2.imwrite(frame_filename, frame)
            self.logger.info(f"{frame_count * 100 // num_frames}% complete")
            self.logger.info("\r{}% complete  ".format((frame_count * 100) // num_frames))
            # if logging:
            #     print("\r{}% complete  ".format((frame_count * 100) // num_frames), end='')
            
            # Save progress after each frame extraction
            # self.progress_file['FrameExtraction']['extract_frames'] = frame_count
            # save_progress_to_file(video_runner_obj=self.video_runner_obj, progress_data=self.progress_file)
            

            frame_count += 1
            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['extract_frames']", value=frame_count)

        # if logging:
        self.logger.info("\r100% complete   ")

        vid.release()

        # data_file_path = '{}/data.txt'.format(vid_name)
        # self.progress_file['FrameExtraction']['extract_frames'] = frame_count
        # self.progress_file['FrameExtraction']['frames_per_extraction'] = frames_per_extraction
        # self.progress_file['FrameExtraction']['actual_frames_per_second'] = actual_frames_per_second
        
        ## Save to Common
        # progress_file_new = load_progress_from_file(self.video_runner_obj)
        # progress_file_new['video_common_values']['step']= frames_per_extraction
        # progress_file_new['video_common_values']['num_frames']= num_frames
        # progress_file_new['video_common_values']['frames_per_second']= actual_frames_per_second
        
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['video_common_values']['step']", value=frames_per_extraction)
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['video_common_values']['num_frames']", value=num_frames)
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['video_common_values']['frames_per_second']", value=actual_frames_per_second)
        
        # with open(data_file_path, 'w') as datafile:
        #     datafile.write('{} {} {}\n'.format(frames_per_extraction, frame_count, actual_frames_per_second))

        # Save progress to JSON file one last time after extraction is complete
        # self.progress_file['FrameExtraction']['extract_frames'] = frame_count
        # progress_file_new['FrameExtraction']['started'] = 'done'
        # save_progress_to_file(video_runner_obj=self.video_runner_obj, progress_data=progress_file_new)
        
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['extract_frames']", value=frame_count)
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['FrameExtraction']['started']", value='done')

        self.logger.info("Extraction complete.")
        # print("Extraction complete.")
