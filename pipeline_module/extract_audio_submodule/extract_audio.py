from logging import Logger
import ffmpeg
from typing import Dict
from utils_module.utils import read_value_from_file, return_video_download_location, load_progress_from_file, save_progress_to_file, save_value_to_file
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
        # self.progress_file = load_progress_from_file(video_runner_obj)
        
    def extract_audio(self):
        """
        Extracts audio from the video file and saves it as a FLAC file.
        The FLAC file will have the same name as the video file, with .flac as its extension.
        """
        
        
        
        # Define the input and output file paths
        input_file = return_video_download_location(self.video_runner_obj)
        output_file = input_file.replace(".mp4", ".flac")
        logger:Logger = self.video_runner_obj.get("logger")
        # self.progress_file = load_progress_from_file(self.video_runner_obj)
        # if(self.progress_file['ExtractAudio']['extract_audio']):
        if(read_value_from_file(video_runner_obj=self.video_runner_obj,key="['ExtractAudio']['extract_audio']")):
            ## Audio already extracted, skipping step
            logger.info("Audio already extracted, skipping step.")
            return
        # Check if the output file already exists
        if not os.path.exists(output_file):
            # Use ffmpeg to extract the audio and save it as a FLAC file
            logger.info(f"Extracting audio from {input_file} and saving it as {output_file}")
            ffmpeg.input(input_file).output(output_file).run()
        
        # progress_file_new = load_progress_from_file(self.video_runner_obj)
        # progress_file_new['ExtractAudio']['extract_audio'] = True
        # save_progress_to_file(video_runner_obj=self.video_runner_obj, progress_data=progress_file_new)
        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['ExtractAudio']['extract_audio']", value=True)
        logger.info(f"Audio extraction completed.")
        return
