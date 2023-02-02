from timeit_decorator import timeit
from utils import returnVideoDownloadLocation,  returnVideoFramesFolder
import cv2
import os


class FrameExtraction:
    def __init__(self, video_id, frames_per_second):
        """
        Initialize ExtractAudio object
        
        Parameters:
        video_id (str): YouTube video ID
        frames_per_second (int): Number of frames to extract per second
        """
        self.video_id = video_id
        self.frame_per_second = frames_per_second


    def extract_frames(self, logging=False):
            """
            Extracts frames from a given video file and saves them in a folder with the same name as the video file (minus the extension).

            :param logging: (optional) If True, outputs the extraction progress to the console.
            """
            # Get the name of the folder to save the extracted frames
            vid_name = returnVideoFramesFolder(self.video_id)
            
            # If the folder doesn't exist, create it
            if not os.path.exists(vid_name):
                try:
                    os.mkdir(vid_name)
                except OSError:
                    print("Cannot create directory for frames")
                    return

            # Open the video handler and get fps
            vid = cv2.VideoCapture(returnVideoDownloadLocation(self.video_id))
            fps = round(vid.get(cv2.CAP_PROP_FPS))
            
            # Calculate the number of frames to extract per iteration
            frames_per_extraction = round(fps / self.frames_per_second)
            num_frames = int(vid.get(cv2.CAP_PROP_FRAME_COUNT))
            list = os.listdir(vid_name)

            # Check if the number of extracted frames is less than the total number of frames
            # If yes, extract the frames and save them
            if len(list) < (num_frames/frames_per_extraction):
                if logging:
                    print("Extracting frames from {} ({} fps, {} frames)...".format(self.video_id, fps, num_frames))

                frame_count = 0
                while frame_count < num_frames:
                    status, frame = vid.read()
                    if not status:
                        print("Error extracting frame {}.".format(frame_count))
                        break
                    if frame_count % frames_per_extraction == 0:
                        # save the frame
                        cv2.imwrite("{}/frame_{}.jpg".format(vid_name, frame_count), frame)
                    if logging:
                        print('\r{}% complete  '.format((frame_count*100)//num_frames), end='')
                    frame_count += 1
                if logging:
                    print('\r100% complete   ')
                vid.release()

                # Create a data file to accompany the frames to keep track of the total
                # number of frames and the number of video frames per extracted frame
                actual_frames_per_second = fps / frames_per_extraction
                with open('{}/data.txt'.format(vid_name), 'w') as datafile:
                    datafile.write('{} {} {}\n'.format(frames_per_extraction, frame_count, actual_frames_per_second))
            else:
                print("Frames already extracted, skipping step.")
