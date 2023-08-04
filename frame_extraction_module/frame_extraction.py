from timeit_decorator import timeit
from utils import return_video_download_location,  return_video_frames_folder
import cv2
import os
from typing import Dict

class FrameExtraction:
    def __init__(self, video_runner_obj: Dict[str, int], frames_per_second:int):
        """
        Initialize ExtractAudio object
        
        Parameters:
            video_runner_obj (Dict[str, int]): A dictionary that contains the information of the video.
                The keys are "video_id", "video_start_time", and "video_end_time", and their values are integers.
            frames_per_second (int): Number of frames to extract per second
        """
        self.video_runner_obj = video_runner_obj
        self.frames_per_second = frames_per_second
        self.logger = video_runner_obj.get("logger")


    def extract_frames(self, logging=False):
            """
            Extracts frames from a given video file and saves them in a folder with the same name as the video file (minus the extension).

            :param logging: (optional) If True, outputs the extraction progress to the console.
            """
            # Get the name of the folder to save the extracted frames
            vid_name = return_video_frames_folder(self.video_runner_obj)
            
            # If the folder doesn't exist, create it
            if not os.path.exists(vid_name):
                try:
                    os.mkdir(vid_name)
                except OSError:
                    self.logger.error("Cannot create directory for frames")
                    print("Cannot create directory for frames")
                    return

            # Open the video handler and get fps
            vid = cv2.VideoCapture(return_video_download_location(self.video_runner_obj))
            fps = round(vid.get(cv2.CAP_PROP_FPS))
            
            # Calculate the number of frames to extract per iteration
            frames_per_extraction = round(fps / self.frames_per_second)
            num_frames = int(vid.get(cv2.CAP_PROP_FRAME_COUNT))
            list = os.listdir(vid_name)

            # Check if the number of extracted frames is less than the total number of frames
            # If yes, extract the frames and save them
            if len(list) < (num_frames/frames_per_extraction):
                self.logger.info(f"Extracting frames from {self.video_runner_obj['video_id']} ({fps} fps, {num_frames} frames)...")
                if logging:
                    
                    print("Extracting frames from {} ({} fps, {} frames)...".format(self.video_runner_obj['video_id'], fps, num_frames))

                frame_count = 0
                while frame_count < num_frames:
                    status, frame = vid.read()
                    if not status:
                        self.logger.error("Error extracting frame {}.".format(frame_count))
                        print("Error extracting frame {}.".format(frame_count))
                        break
                    if frame_count % frames_per_extraction == 0:
                        # save the frame
                        cv2.imwrite("{}/frame_{}.jpg".format(vid_name, frame_count), frame)
                    self.logger.info(f"{frame_count*100//num_frames}% complete")
                    if logging:
                        print('\r{}% complete  '.format((frame_count*100)//num_frames), end='')
                    frame_count += 1
                if logging:
                    print('\r100% complete   ')
                self.logger.info(f"Extraction complete.")
                vid.release()

                # Create a data file to accompany the frames to keep track of the total
                # number of frames and the number of video frames per extracted frame
                actual_frames_per_second = fps / frames_per_extraction
                video_frames_folder = return_video_frames_folder(self.video_runner_obj)

                with open('{}/data.txt'.format(video_frames_folder), 'w') as datafile:
                    datafile.write('{} {} {}\n'.format(frames_per_extraction, frame_count, actual_frames_per_second))
            else:
                self.logger.info(f"Frames already extracted, skipping step.")
                print("Frames already extracted, skipping step.")
