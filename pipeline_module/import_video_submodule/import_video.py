from logging import Logger
from ..utils_module.timeit_decorator import timeit
import json
import yt_dlp as ydl
from ..utils_module.utils import read_value_from_file, return_video_download_location, return_video_folder_name, \
    save_value_to_file
from datetime import timedelta
import ffmpeg
import os
from typing import Dict, Union


class ImportVideo:
    def __init__(self, video_runner_obj: Dict[str, Union[int, str]]):
        """
        Initialize ImportVideo object.

        Parameters:
        video_runner_obj (Dict[str, Union[int, str]]): A dictionary that contains the information of the video.
            The keys are "video_id", "video_start_time", and "video_end_time", and their values are integers or strings.
        """
        self.video_runner_obj = video_runner_obj
        self.logger: Logger = video_runner_obj.get("logger")

    @timeit
    def download_video(self):
        """
        Download the video from YouTube

        Returns:
        None
        """
        video_id = self.video_runner_obj.get("video_id")
        video_start_time = self.video_runner_obj.get("video_start_time", None)
        video_end_time = self.video_runner_obj.get("video_end_time", None)

        if read_value_from_file(video_runner_obj=self.video_runner_obj, key="['ImportVideo']['download_video']"):
            # Video already downloaded, skipping step
            self.logger.info("Video already downloaded, skipping step.")
            return

        try:
            ydl_opts = {'outtmpl': return_video_download_location(self.video_runner_obj), "format": "best"}
            with ydl.YoutubeDL(ydl_opts) as ydl_instance:
                vid = ydl_instance.extract_info(f'https://www.youtube.com/watch?v={video_id}', download=True)

            # Get Video Duration
            duration = vid.get('duration')

            # Get Video Title
            title = vid.get('title')
            self.logger.info(f"Video Title: {title}")

            # Save metadata to json file
            with open(return_video_folder_name(self.video_runner_obj) + '/metadata.json', 'w') as f:
                json.dump({'duration': duration, 'title': title}, f)

            if video_start_time and video_end_time:
                self.trim_video(video_start_time, video_end_time)

            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['ImportVideo']['download_video']",
                               value=True)
            self.logger.info(f"Video downloaded to {return_video_download_location(self.video_runner_obj)}")

        except Exception as e:
            self.logger.error(f"Error downloading video: {str(e)}")
            raise

    def trim_video(self, start_time: str, end_time: str):
        """
        Trim the downloaded video based on start and end times.

        Parameters:
        start_time (str): Start time in seconds
        end_time (str): End time in seconds
        """
        try:
            # Convert start and end time to timedelta
            start = timedelta(seconds=int(start_time))
            end = timedelta(seconds=int(end_time))

            self.logger.info(f"Trimming video from {start} to {end}")

            input_file = return_video_download_location(self.video_runner_obj)
            output_file = return_video_folder_name(self.video_runner_obj) + '/trimmed.mp4'

            # Trim video and audio based on start and end time
            input_stream = ffmpeg.input(input_file)
            vid = (
                input_stream.video
                .trim(start=start_time, end=end_time)
                .setpts('PTS-STARTPTS')
            )
            aud = (
                input_stream.audio
                .filter_('atrim', start=start_time, end=end_time)
                .filter_('asetpts', 'PTS-STARTPTS')
            )

            # Join trimmed video and audio
            joined = ffmpeg.concat(vid, aud, v=1, a=1).node
            output = ffmpeg.output(joined[0], joined[1], output_file)
            ffmpeg.run(output, overwrite_output=True)

            # Delete original video
            if os.path.exists(input_file):
                os.remove(input_file)

            # Rename trimmed video to original name
            os.rename(output_file, input_file)

            self.logger.info(f"Video trimmed successfully")

        except Exception as e:
            self.logger.error(f"Error trimming video: {str(e)}")
            raise