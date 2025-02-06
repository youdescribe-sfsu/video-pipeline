import os
import subprocess

import ffmpeg
from typing import Dict, Any, Optional
from logging import Logger
from web_server_module.web_server_database import get_status_for_youtube_id, update_status
from ..utils_module.utils import (
    return_video_download_location,
    return_audio_file_name,
    return_video_folder_name
)
from ..utils_module.timeit_decorator import timeit

class ExtractAudio:
    def __init__(self, video_runner_obj: Dict[str, Any]):
        print("Initializing ExtractAudio")
        self.video_runner_obj = video_runner_obj
        self.logger: Logger = video_runner_obj.get("logger")
        if not self.logger:
            print("Warning: Logger not provided in video_runner_obj")
        print(f"ExtractAudio initialized with video_runner_obj: {video_runner_obj}")

    @timeit
    def extract_audio(self) -> bool:
        """
        Extracts audio from video file and converts to FLAC format.
        Uses ffmpeg with progress monitoring and proper error handling.
        Returns True if successful, False if any errors occur.
        """
        print("Starting extract_audio method")
        input_file = return_video_download_location(self.video_runner_obj)
        output_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            return_audio_file_name(self.video_runner_obj)
        )
        print(f"Input file: {input_file}")
        print(f"Output file: {output_file}")

        # Check database status first
        video_id = self.video_runner_obj.get("video_id")
        ai_user_id = self.video_runner_obj.get("AI_USER_ID")
        if get_status_for_youtube_id(video_id, ai_user_id) == "done":
            print("Audio already extracted, skipping step.")
            return True

        try:
            # Validate input file existence
            if not os.path.exists(input_file):
                print(f"Input video file not found: {input_file}")
                raise FileNotFoundError(f"Input video file not found: {input_file}")

            print(f"Extracting audio from {input_file} and saving it as {output_file}")

            # Run ffmpeg with progress monitoring using subprocess
            process = subprocess.Popen(
                [
                    'ffmpeg',
                    '-i', input_file,
                    '-acodec', 'flac',
                    '-ac', '2',
                    '-ar', '48000',
                    '-y',  # Overwrite output file if exists
                    output_file
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )

            # Monitor ffmpeg progress in real-time
            while True:
                output = process.stderr.readline()
                if output == '' and process.poll() is not None:
                    break
                if output:
                    # Log progress to help diagnose any issues
                    print(output.strip())
                    if self.video_runner_obj.get("logger"):
                        self.video_runner_obj["logger"].info(output.strip())

            # Check if ffmpeg completed successfully
            if process.returncode != 0:
                error_output = process.stderr.read()
                print(f"FFmpeg process failed with return code {process.returncode}")
                print(f"Error output: {error_output}")
                raise RuntimeError(f"FFmpeg failed with return code {process.returncode}")

            # Verify output file was created
            if not os.path.exists(output_file):
                print(f"Failed to create output audio file: {output_file}")
                raise RuntimeError(f"Failed to create output audio file: {output_file}")

            # Update database status on success
            update_status(video_id, ai_user_id, "done")
            print("Audio extraction completed successfully.")
            return True

        except subprocess.SubprocessError as e:
            print(f"Subprocess error occurred during ffmpeg execution: {str(e)}")
            return False
        except FileNotFoundError as e:
            print(str(e))
            return False
        except RuntimeError as e:
            print(str(e))
            return False
        except Exception as e:
            print(f"An unexpected error occurred during audio extraction: {str(e)}")
            if self.video_runner_obj.get("logger"):
                self.video_runner_obj["logger"].error(f"Unexpected error: {str(e)}")
            return False

    def get_audio_metadata(self) -> Optional[Dict[str, Any]]:
        print("Starting get_audio_metadata method")
        audio_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            return_audio_file_name(self.video_runner_obj)
        )
        print(f"Audio file: {audio_file}")

        try:
            if not os.path.exists(audio_file):
                print(f"Audio file not found: {audio_file}")
                raise FileNotFoundError(f"Audio file not found: {audio_file}")

            probe = ffmpeg.probe(audio_file)
            audio_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'audio'), None)

            if audio_stream:
                metadata = {
                    'sample_rate': audio_stream.get('sample_rate'),
                    'channels': audio_stream.get('channels'),
                    'duration': probe.get('format', {}).get('duration'),
                    'codec': audio_stream.get('codec_name'),
                    'bit_rate': audio_stream.get('bit_rate')
                }
                print(f"Audio metadata: {metadata}")
                return metadata
            else:
                print("No audio stream found in the file.")
                return None

        except FileNotFoundError as e:
            print(str(e))
            return None
        except ffmpeg.Error as e:
            print(f"FFmpeg error occurred while getting audio metadata: {e.stderr.decode()}")
            return None
        except Exception as e:
            print(f"An unexpected error occurred while getting audio metadata: {str(e)}")
            return None

    def check_audio_format(self) -> bool:
        print("Starting check_audio_format method")
        audio_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            return_audio_file_name(self.video_runner_obj)
        )
        print(f"Audio file: {audio_file}")

        try:
            if not os.path.exists(audio_file):
                print(f"Audio file not found: {audio_file}")
                raise FileNotFoundError(f"Audio file not found: {audio_file}")

            probe = ffmpeg.probe(audio_file)
            audio_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'audio'), None)

            if audio_stream and audio_stream['codec_name'] == 'flac':
                print("Audio format check passed: FLAC codec")
                return True
            else:
                print("Audio is not in the expected format (FLAC)")
                return False

        except FileNotFoundError as e:
            print(str(e))
            return False
        except ffmpeg.Error as e:
            print(f"Error checking audio format: {e.stderr.decode()}")
            return False
        except Exception as e:
            print(f"An unexpected error occurred while checking audio format: {str(e)}")
            return False