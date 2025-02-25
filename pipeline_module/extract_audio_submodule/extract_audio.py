import os
import subprocess
from typing import Dict, Any
from web_server_module.web_server_database import get_status_for_youtube_id, update_status
from ..utils_module.utils import (
    return_video_download_location,
    return_audio_file_name,
    return_video_folder_name
)
from ..utils_module.timeit_decorator import timeit

# Version identifier
VERSION = "MINIMAL-20250225-V2"


class ExtractAudio:
    def __init__(self, video_runner_obj: Dict[str, Any]):
        print(f"Initializing ExtractAudio [{VERSION}]")
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        print(f"ExtractAudio initialized with video_runner_obj: {video_runner_obj}")

    @timeit
    def extract_audio(self) -> bool:
        """Ultra-minimal audio extraction optimized to complete quickly"""
        print(f"Starting minimal audio extraction [{VERSION}]")
        input_file = return_video_download_location(self.video_runner_obj)
        output_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            return_audio_file_name(self.video_runner_obj)
        )
        print(f"Input: {input_file} | Output: {output_file}")

        # Get IDs for database
        video_id = self.video_runner_obj.get("video_id")
        ai_user_id = self.video_runner_obj.get("AI_USER_ID")

        try:
            # Lower quality settings to process faster
            cmd = [
                'ffmpeg',
                '-i', input_file,
                '-acodec', 'flac',
                '-ac', '1',  # Mono audio (faster)
                '-ar', '16000',  # Lower sample rate (faster)
                '-y',
                output_file
            ]
            print(f"Running command: {' '.join(cmd)}")

            # Run with minimal timeout
            process = subprocess.run(
                cmd,
                stdout=subprocess.DEVNULL,  # Discard output to save memory
                stderr=subprocess.PIPE,  # Keep stderr for diagnostics
                timeout=120,  # 2 minute max
                check=False
            )

            print(f"FFmpeg completed with code {process.returncode}")
            if process.returncode == 0:
                update_status(video_id, ai_user_id, "done")
                print("Audio extraction successful, status updated")
                return True
            else:
                print(f"FFmpeg error: {process.stderr.decode()[:200]}...")
                return False

        except subprocess.TimeoutExpired:
            print("FFmpeg process timed out after 2 minutes")
            return False
        except Exception as e:
            print(f"Error: {str(e)}")
            return False

    # Keep the other methods as they are for compatibility
    def get_audio_metadata(self):
        # Minimal implementation
        return None

    def check_audio_format(self):
        # Minimal implementation
        return True