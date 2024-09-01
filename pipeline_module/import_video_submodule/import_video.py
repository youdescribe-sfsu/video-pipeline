import os
import json
import yt_dlp as ydl
from typing import Dict, Union, Optional
from logging import Logger
from ..utils_module.utils import read_value_from_file, return_video_download_location, return_video_folder_name, save_value_to_file
from ..utils_module.timeit_decorator import timeit
from datetime import timedelta
import ffmpeg

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
    def download_video(self) -> bool:
        """
        Download the video from YouTube

        Returns:
        bool: True if download was successful, False otherwise
        """
        video_id = self.video_runner_obj.get("video_id")
        video_start_time = self.video_runner_obj.get("video_start_time", None)
        video_end_time = self.video_runner_obj.get("video_end_time", None)

        if read_value_from_file(video_runner_obj=self.video_runner_obj, key="['ImportVideo']['download_video']"):
            self.logger.info("Video already downloaded, skipping step.")
            return True

        try:
            ydl_opts = {
                'outtmpl': return_video_download_location(self.video_runner_obj),
                "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
                'progress_hooks': [self.progress_hook],
            }
            with ydl.YoutubeDL(ydl_opts) as ydl_instance:
                self.logger.info(f"Downloading video: {video_id}")
                vid = ydl_instance.extract_info(f'https://www.youtube.com/watch?v={video_id}', download=True)

            # Get Video Duration
            duration = vid.get('duration')

            # Get Video Title
            title = vid.get('title')
            self.logger.info(f"Video Title: {title}")

            # Save metadata to json file
            with open(return_video_folder_name(self.video_runner_obj) + '/metadata.json', 'w') as f:
                json.dump({'duration': duration, 'title': title}, f)

            if not self.check_video_format():
                raise ValueError("Downloaded video is not in the expected format.")

            if video_start_time and video_end_time:
                self.trim_video(video_start_time, video_end_time)

            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['ImportVideo']['download_video']", value=True)
            self.logger.info(f"Video downloaded to {return_video_download_location(self.video_runner_obj)}")

            return True

        except ydl.DownloadError as e:
            self.logger.error(f"Error downloading video: {str(e)}")
            return False
        except ValueError as e:
            self.logger.error(str(e))
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error downloading video: {str(e)}")
            return False

    def progress_hook(self, d: Dict[str, Union[str, int, float]]) -> None:
        """
        Progress hook for yt-dlp to track download progress.

        Parameters:
        d (Dict[str, Union[str, int, float]]): Dictionary containing download information
        """
        if d['status'] == 'downloading':
            percent = d['_percent_str']
            speed = d['_speed_str']
            eta = d['_eta_str']
            self.logger.info(f"Downloading: {percent} complete, Speed: {speed}, ETA: {eta}")
        elif d['status'] == 'finished':
            self.logger.info('Download completed. Now converting...')

    def check_video_format(self) -> bool:
        """
        Check if the downloaded video is in the expected format (MP4).

        Returns:
        bool: True if the video is in MP4 format, False otherwise
        """
        video_path = return_video_download_location(self.video_runner_obj)
        try:
            probe = ffmpeg.probe(video_path)
            video_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'video'), None)
            if video_stream and video_stream['codec_name'] == 'h264':
                self.logger.info("Video format check passed: MP4 with H.264 codec")
                return True
            else:
                self.logger.warning("Video is not in the expected format (MP4 with H.264 codec)")
                return False
        except ffmpeg.Error as e:
            self.logger.error(f"Error checking video format: {str(e)}")
            return False

    def trim_video(self, start_time: str, end_time: str) -> None:
        """
        Trim the downloaded video based on start and end times.

        Parameters:
        start_time (str): Start time in seconds
        end_time (str): End time in seconds
        """
        try:
            start = timedelta(seconds=int(start_time))
            end = timedelta(seconds=int(end_time))

            self.logger.info(f"Trimming video from {start} to {end}")

            input_file = return_video_download_location(self.video_runner_obj)
            output_file = return_video_folder_name(self.video_runner_obj) + '/trimmed.mp4'

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

            joined = ffmpeg.concat(vid, aud, v=1, a=1).node
            output = ffmpeg.output(joined[0], joined[1], output_file)
            ffmpeg.run(output, overwrite_output=True)

            # Delete original video
            if os.path.exists(input_file):
                os.remove(input_file)

            # Rename trimmed video to original name
            os.rename(output_file, input_file)

            self.logger.info(f"Video trimmed successfully")

        except ffmpeg.Error as e:
            self.logger.error(f"FFmpeg error occurred during video trimming: {e.stderr.decode()}")
            raise
        except Exception as e:
            self.logger.error(f"An unexpected error occurred during video trimming: {str(e)}")
            raise

    def get_video_metadata(self) -> Optional[Dict[str, Union[int, str]]]:
        """
        Retrieves metadata about the downloaded video.

        Returns:
        Optional[Dict[str, Union[int, str]]]: A dictionary containing video metadata, or None if retrieval fails
        """
        video_file = return_video_download_location(self.video_runner_obj)

        try:
            probe = ffmpeg.probe(video_file)
            video_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'video'), None)

            if video_stream:
                return {
                    'width': int(video_stream['width']),
                    'height': int(video_stream['height']),
                    'duration': float(probe['format']['duration']),
                    'format': video_stream['codec_name'],
                    'fps': eval(video_stream['avg_frame_rate'])
                }
            else:
                self.logger.warning("No video stream found in the file.")
                return None

        except ffmpeg.Error as e:
            self.logger.error(f"FFmpeg error occurred while getting video metadata: {e.stderr.decode()}")
            return None
        except Exception as e:
            self.logger.error(f"An unexpected error occurred while getting video metadata: {str(e)}")
            return None