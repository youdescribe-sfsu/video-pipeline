import ffmpeg
from typing import Dict
from utils import return_video_download_location
import os
class ExtractAudio:
    def __init__(self, video_runner_obj: Dict[str, int]):
        """
        Initialize ExtractAudio object.
        
        Parameters:
        video_runner_obj (Dict[str, int]): A dictionary that contains the information of the video.
            The keys are "video_id", "video_start_time", and "video_end_time", and their values are integers.
        """
        self.video_runner_obj = video_runner_obj
        
    def extract_audio(self):
        """
        Extracts audio from the video file and saves it as a FLAC file.
        The FLAC file will have the same name as the video file, with .flac as its extension.
        """
        # Define the input and output file paths
        input_file = return_video_download_location(self.video_runner_obj)
        output_file = input_file.replace(".mp4", ".flac")
        
        # Check if the output file already exists
        if not os.path.exists(output_file):
            # Use ffmpeg to extract the audio and save it as a FLAC file
            ffmpeg.input(input_file).output(output_file).run()
        
        return
