import os
import traceback
import json
from typing import Dict, Any, Optional
from logging import Logger
from web_server_module.web_server_database import get_status_for_youtube_id, update_status
from ..utils_module.utils import (
    return_video_download_location,
    return_audio_file_name,
    return_video_folder_name
)
from ..utils_module.timeit_decorator import timeit

# Version identifier for tracking which code is running
VERSION = "DIAGNOSTIC-20250225-V1"

# Attempt to import ffmpeg with error tracking
try:
    import ffmpeg

    FFMPEG_IMPORT_SUCCESS = True
    print(f"ffmpeg-python imported successfully (version: {getattr(ffmpeg, '__version__', 'unknown')})")
except Exception as e:
    FFMPEG_IMPORT_SUCCESS = False
    print(f"ERROR IMPORTING FFMPEG: {str(e)}")
    print(traceback.format_exc())


class ExtractAudio:
    def __init__(self, video_runner_obj: Dict[str, Any]):
        print(f"Initializing ExtractAudio [{VERSION}]")
        self.video_runner_obj = video_runner_obj
        self.logger: Logger = video_runner_obj.get("logger")
        if not self.logger:
            print("Warning: Logger not provided in video_runner_obj")
        else:
            self.logger.info(f"ExtractAudio [{VERSION}] initialized")

        print(f"ExtractAudio initialized with video_runner_obj: {video_runner_obj}")

    @timeit
    def extract_audio(self) -> bool:
        print("Starting direct subprocess audio extraction")
        input_file = return_video_download_location(self.video_runner_obj)
        output_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            return_audio_file_name(self.video_runner_obj)
        )

        # Check database status
        video_id = self.video_runner_obj.get("video_id")
        ai_user_id = self.video_runner_obj.get("AI_USER_ID")

        try:
            import subprocess
            cmd = [
                'ffmpeg',
                '-i', input_file,
                '-acodec', 'flac',
                '-ac', '2',
                '-ar', '48000',
                '-y',
                output_file
            ]
            print(f"Running command: {' '.join(cmd)}")

            # Run with smaller timeout
            process = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=180,  # 3 minute timeout (below the 4 minute kill time)
                check=False
            )

            if process.returncode == 0:
                # Update database on success
                update_status(video_id, ai_user_id, "done")
                print("Audio extraction completed successfully")
                return True
            else:
                print(f"FFmpeg error: {process.stderr.decode()}")
                return False

        except subprocess.TimeoutExpired:
            print("FFmpeg process timed out")
            return False
        except Exception as e:
            print(f"Error: {str(e)}")
            return False

    def _extract_audio_subprocess(self, input_file, output_file, video_id, ai_user_id) -> bool:
        """Fallback method using direct subprocess call with timeout."""
        try:
            import subprocess
            print("Using direct subprocess call with timeout")

            cmd = [
                'ffmpeg',
                '-i', input_file,
                '-acodec', 'flac',
                '-ac', '2',
                '-ar', '48000',
                '-y',
                output_file
            ]
            print(f"Running subprocess command: {' '.join(cmd)}")

            # Execute with timeout
            process = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=300,  # 5 minute timeout
                check=False,
                text=True
            )

            # Check result
            if process.returncode != 0:
                print(f"FFmpeg subprocess failed with code {process.returncode}")
                print(f"Error output: {process.stderr}")
                if self.logger:
                    self.logger.error(f"FFmpeg subprocess error: {process.stderr}")
                return False

            print("FFmpeg subprocess completed successfully")

            # Verify output file
            if not os.path.exists(output_file):
                print(f"Failed to create output audio file: {output_file}")
                return False

            # Update database
            update_status(video_id, ai_user_id, "done")
            print("Audio extraction completed successfully (via subprocess)")
            return True

        except subprocess.TimeoutExpired:
            print("FFmpeg subprocess timed out after 5 minutes")
            if self.logger:
                self.logger.error("FFmpeg subprocess timed out after 5 minutes")
            return False
        except Exception as e:
            print(f"Error in subprocess method: {str(e)}")
            print(traceback.format_exc())
            if self.logger:
                self.logger.error(f"Error in subprocess method: {str(e)}")
            return False

    def get_audio_metadata(self) -> Optional[Dict[str, Any]]:
        print(f"Starting get_audio_metadata method [{VERSION}]")
        audio_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            return_audio_file_name(self.video_runner_obj)
        )
        print(f"Audio file: {audio_file}")

        try:
            if not os.path.exists(audio_file):
                print(f"Audio file not found: {audio_file}")
                raise FileNotFoundError(f"Audio file not found: {audio_file}")

            if not FFMPEG_IMPORT_SUCCESS:
                print("WARNING: Using fallback subprocess for metadata due to ffmpeg import failure")
                return self._get_metadata_subprocess(audio_file)

            print("About to call ffmpeg.probe...")
            probe = ffmpeg.probe(audio_file)
            print("ffmpeg.probe completed successfully")

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
            error_msg = e.stderr.decode() if hasattr(e, 'stderr') else str(e)
            print(f"FFmpeg error occurred while getting audio metadata: {error_msg}")
            return None
        except Exception as e:
            print(f"An unexpected error occurred while getting audio metadata: {str(e)}")
            print(traceback.format_exc())
            return None

    def _get_metadata_subprocess(self, audio_file) -> Optional[Dict[str, Any]]:
        """Fallback method to get metadata using direct subprocess call."""
        try:
            import subprocess
            import json

            cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json',
                   '-show_format', '-show_streams', audio_file]

            process = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=30,
                check=False,
                text=True
            )

            if process.returncode != 0:
                print(f"FFprobe error: {process.stderr}")
                return None

            probe = json.loads(process.stdout)
            audio_stream = next((stream for stream in probe['streams']
                                 if stream['codec_type'] == 'audio'), None)

            if audio_stream:
                metadata = {
                    'sample_rate': audio_stream.get('sample_rate'),
                    'channels': audio_stream.get('channels'),
                    'duration': probe.get('format', {}).get('duration'),
                    'codec': audio_stream.get('codec_name'),
                    'bit_rate': audio_stream.get('bit_rate')
                }
                return metadata
            return None

        except Exception as e:
            print(f"Error in metadata subprocess: {str(e)}")
            return None

    def check_audio_format(self) -> bool:
        print(f"Starting check_audio_format method [{VERSION}]")
        audio_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            return_audio_file_name(self.video_runner_obj)
        )
        print(f"Audio file: {audio_file}")

        try:
            if not os.path.exists(audio_file):
                print(f"Audio file not found: {audio_file}")
                raise FileNotFoundError(f"Audio file not found: {audio_file}")

            if not FFMPEG_IMPORT_SUCCESS:
                print("WARNING: Using fallback subprocess for format check due to ffmpeg import failure")
                return self._check_format_subprocess(audio_file)

            print("About to call ffmpeg.probe for format check...")
            probe = ffmpeg.probe(audio_file)
            print("ffmpeg.probe completed successfully")

            audio_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'audio'), None)

            if audio_stream and audio_stream['codec_name'] == 'flac':
                print("Audio format check passed: FLAC codec")
                return True
            else:
                codec = audio_stream['codec_name'] if audio_stream else "no audio stream"
                print(f"Audio is not in the expected format (found: {codec}, expected: flac)")
                return False

        except FileNotFoundError as e:
            print(str(e))
            return False
        except ffmpeg.Error as e:
            error_msg = e.stderr.decode() if hasattr(e, 'stderr') else str(e)
            print(f"Error checking audio format: {error_msg}")
            return False
        except Exception as e:
            print(f"An unexpected error occurred while checking audio format: {str(e)}")
            print(traceback.format_exc())
            return False

    def _check_format_subprocess(self, audio_file) -> bool:
        """Fallback method to check audio format using direct subprocess."""
        try:
            import subprocess
            import json

            cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json',
                   '-show_streams', audio_file]

            process = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=30,
                check=False,
                text=True
            )

            if process.returncode != 0:
                print(f"FFprobe error in format check: {process.stderr}")
                return False

            probe = json.loads(process.stdout)
            audio_stream = next((stream for stream in probe['streams']
                                 if stream['codec_type'] == 'audio'), None)

            return audio_stream is not None and audio_stream['codec_name'] == 'flac'

        except Exception as e:
            print(f"Error in format check subprocess: {str(e)}")
            return False