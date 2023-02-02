from import_video.extract_frames import extract_frames
from import_video.import_youtube import import_video
from timeit_decorator import timeit
from utils import returnVideoDownloadLocation,  returnVideoFramesFolder
import ffmpeg
import cv2
import os


class ImportVideo:
    def __init__(self,video_id,video_start_time,video_end_time):
        self.video_id = video_id
        self.video_start_time = video_start_time
        self.video_end_time = video_end_time
    
    @timeit
    def download_video(self):
        try:
            print("=== DOWNLOAD VIDEO ===")
            print("start time: ",self.video_start_time)
            print("end time: ",self.video_end_time)
            import_video(self.video_id,self.video_start_time,self.video_end_time)
            print("=== EXTRACT FRAMES ===")
            extract_frames(self.video_id, 10, True)
            return True
        except Exception as e:
            print("IMPORT VIDEO ERROR: ",e)
            return False
    
    def extract_audio(self):
        """
        Extracts audio from the video file and saves it as a FLAC file.
        The FLAC file will have the same name as the video file, with .flac as its extension.
        """
        # Define the input and output file paths
        input_file = returnVideoDownloadLocation(self.video_id)
        output_file = input_file.replace(".mp4", ".flac")
        # Use ffmpeg to extract the audio and save it as a FLAC file
        ffmpeg.input(input_file).output(output_file).run()
        

    def extract_frames(self, frames_per_second, logging=False):
        """
        Extracts frames from a given video file and saves them in a folder with the same name as the video file (minus the extension).

        :param frames_per_second: target number of frames per second to extract. This value is rounded to be an exact divisor of the video frame rate.
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
        frames_per_extraction = round(fps / frames_per_second)
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
