import os
import json
import yt_dlp as ydl
from typing import Dict, Union, Optional, Any
from logging import Logger
from ..utils_module.utils import read_value_from_file, return_video_download_location, return_video_folder_name, save_value_to_file
from ..utils_module.timeit_decorator import timeit
from datetime import timedelta
import ffmpeg

class ImportVideo:
    def __init__(self, video_runner_obj: Dict[str, Any]):
        print("Initializing ImportVideo")
        self.video_runner_obj = video_runner_obj
        self.logger: Logger = video_runner_obj.get("logger")
        if not self.logger:
            print("Warning: Logger not provided in video_runner_obj")
        print(f"ImportVideo initialized with video_runner_obj: {video_runner_obj}")

    @timeit
    @timeit
    def download_video(self) -> bool:
        print("Starting download_video method")
        video_id = self.video_runner_obj.get("video_id")
        video_start_time = self.video_runner_obj.get("video_start_time", None)
        video_end_time = self.video_runner_obj.get("video_end_time", None)
        print(f"Video ID: {video_id}, Start time: {video_start_time}, End time: {video_end_time}")

        if not video_id:
            print("Error: video_id is missing from video_runner_obj")
            return False

        try:
            if read_value_from_file(video_runner_obj=self.video_runner_obj, key="['ImportVideo']['download_video']"):
                print("Video already downloaded, skipping step.")
                return True

            ydl_opts = {
                'outtmpl': return_video_download_location(self.video_runner_obj),
                "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
                'progress_hooks': [self.progress_hook],
            }
            print(f"ydl_opts: {ydl_opts}")

            with ydl.YoutubeDL(ydl_opts) as ydl_instance:
                print(f"Downloading video: {video_id}")
                vid = ydl_instance.extract_info(f'https://www.youtube.com/watch?v={video_id}', download=True)

            if not vid:
                print("Error: Failed to extract video info")
                return False

            print("Video download completed")

            # Get Video Duration and Title
            duration = vid.get('duration')
            title = vid.get('title')
            if not duration or not title:
                print("Error: Failed to get video duration or title")
                return False
            print(f"Video Title: {title}, Duration: {duration}")

            # Save metadata to json file
            metadata_file = return_video_folder_name(self.video_runner_obj) + '/metadata.json'
            with open(metadata_file, 'w') as f:
                json.dump({'duration': duration, 'title': title}, f)
            print(f"Metadata saved to {metadata_file}")

            if video_start_time and video_end_time:
                self.trim_video(video_start_time, video_end_time)

            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['ImportVideo']['download_video']",
                               value=str(True))
            print(f"Video downloaded to {return_video_download_location(self.video_runner_obj)}")

            return True

        except ydl.DownloadError as e:
            print(f"Error downloading video: {str(e)}")
            self.logger.error(f"Error downloading video: {str(e)}")
            return False
        except ValueError as e:
            print(str(e))
            self.logger.error(str(e))
            return False
        except Exception as e:
            print(f"Unexpected error downloading video: {str(e)}")
            self.logger.error(f"Unexpected error downloading video: {str(e)}")
            return False

    def progress_hook(self, d: Dict[str, Union[str, int, float]]) -> None:
        if d['status'] == 'downloading':
            percent = d['_percent_str']
            speed = d['_speed_str']
            eta = d['_eta_str']
            print(f"Downloading: {percent} complete, Speed: {speed}, ETA: {eta}")
        elif d['status'] == 'finished':
            print('Download completed. Now converting...')

    def check_video_format(self) -> bool:
        print("Checking video format")
        video_path = return_video_download_location(self.video_runner_obj)
        try:
            probe = ffmpeg.probe(video_path)
            video_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'video'), None)
            if video_stream and video_stream['codec_name'] == 'h264':
                print("Video format check passed: MP4 with H.264 codec")
                return True
            else:
                print("Video is not in the expected format (MP4 with H.264 codec)")
                return False
        except ffmpeg.Error as e:
            print(f"Error checking video format: {str(e)}")
            return False

    def trim_video(self, start_time: str, end_time: str) -> None:
        print(f"Trimming video from {start_time} to {end_time}")
        try:
            start = timedelta(seconds=int(start_time))
            end = timedelta(seconds=int(end_time))

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

            if os.path.exists(input_file):
                os.remove(input_file)
            os.rename(output_file, input_file)

            print(f"Video trimmed successfully")

        except ffmpeg.Error as e:
            print(f"FFmpeg error occurred during video trimming: {e.stderr.decode()}")
            raise
        except Exception as e:
            print(f"An unexpected error occurred during video trimming: {str(e)}")
            raise

    def get_video_metadata(self) -> Optional[Dict[str, Union[int, str]]]:
        print("Getting video metadata")
        video_file = return_video_download_location(self.video_runner_obj)

        try:
            probe = ffmpeg.probe(video_file)
            video_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'video'), None)

            if video_stream:
                metadata = {
                    'width': int(video_stream['width']),
                    'height': int(video_stream['height']),
                    'duration': float(probe['format']['duration']),
                    'format': video_stream['codec_name'],
                    'fps': eval(video_stream['avg_frame_rate'])
                }
                print(f"Video metadata: {metadata}")
                return metadata
            else:
                print("No video stream found in the file.")
                return None

        except ffmpeg.Error as e:
            print(f"FFmpeg error occurred while getting video metadata: {e.stderr.decode()}")
            return None
        except Exception as e:
            print(f"An unexpected error occurred while getting video metadata: {str(e)}")
            return None