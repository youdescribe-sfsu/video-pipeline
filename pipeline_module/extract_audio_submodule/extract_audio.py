import os
import ffmpeg
from typing import Dict, Any, Optional
from logging import Logger
from ..utils_module.utils import (
    read_value_from_file,
    return_video_download_location,
    return_audio_file_name,
    return_video_folder_name,
    save_value_to_file
)
from ..utils_module.timeit_decorator import timeit

class ExtractAudio:
    def __init__(self, video_runner_obj: Dict[str, Any]):
        """
        Initialize ExtractAudio object.

        Parameters:
        video_runner_obj (Dict[str, Any]): A dictionary that contains the information of the video.
            The keys are "video_id", "video_start_time", and "video_end_time", and their values are integers.
        """
        self.video_runner_obj = video_runner_obj
        self.logger: Logger = video_runner_obj.get("logger")

    @timeit
    def extract_audio(self) -> bool:
        """
        Extracts audio from the video file and saves it as a FLAC file.
        The FLAC file will have the same name as the video file, with .flac as its extension.

        Returns:
        bool: True if extraction was successful, False otherwise
        """
        input_file = return_video_download_location(self.video_runner_obj)
        output_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            return_audio_file_name(self.video_runner_obj)
        )

        if read_value_from_file(video_runner_obj=self.video_runner_obj, key="['ExtractAudio']['extract_audio']"):
            self.logger.info("Audio already extracted, skipping step.")
            return True

        try:
            if not os.path.exists(input_file):
                raise FileNotFoundError(f"Input video file not found: {input_file}")

            self.logger.info(f"Extracting audio from {input_file} and saving it as {output_file}")

            # Extract audio using ffmpeg
            (
                ffmpeg
                .input(input_file)
                .output(output_file, acodec='flac', ac=2, ar='48k')
                .overwrite_output()
                .run(capture_stdout=True, capture_stderr=True)
            )

            if not os.path.exists(output_file):
                raise RuntimeError(f"Failed to create output audio file: {output_file}")

            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['ExtractAudio']['extract_audio']", value=True)
            self.logger.info("Audio extraction completed successfully.")
            return True

        except ffmpeg.Error as e:
            self.logger.error(f"FFmpeg error occurred: {e.stderr.decode()}")
            return False
        except FileNotFoundError as e:
            self.logger.error(str(e))
            return False
        except RuntimeError as e:
            self.logger.error(str(e))
            return False
        except Exception as e:
            self.logger.error(f"An unexpected error occurred during audio extraction: {str(e)}")
            return False

    def get_audio_metadata(self) -> Optional[Dict[str, Any]]:
        """
        Retrieves metadata about the extracted audio file.

        Returns:
        Optional[Dict[str, Any]]: A dictionary containing audio metadata, or None if retrieval fails
        """
        audio_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            return_audio_file_name(self.video_runner_obj)
        )

        try:
            if not os.path.exists(audio_file):
                raise FileNotFoundError(f"Audio file not found: {audio_file}")

            probe = ffmpeg.probe(audio_file)
            audio_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'audio'), None)

            if audio_stream:
                return {
                    'sample_rate': audio_stream.get('sample_rate'),
                    'channels': audio_stream.get('channels'),
                    'duration': probe.get('format', {}).get('duration'),
                    'codec': audio_stream.get('codec_name'),
                    'bit_rate': audio_stream.get('bit_rate')
                }
            else:
                self.logger.warning("No audio stream found in the file.")
                return None

        except FileNotFoundError as e:
            self.logger.error(str(e))
            return None
        except ffmpeg.Error as e:
            self.logger.error(f"FFmpeg error occurred while getting audio metadata: {e.stderr.decode()}")
            return None
        except Exception as e:
            self.logger.error(f"An unexpected error occurred while getting audio metadata: {str(e)}")
            return None

    def check_audio_format(self) -> bool:
        """
        Check if the extracted audio is in the expected format (FLAC).

        Returns:
        bool: True if the audio is in FLAC format, False otherwise
        """
        audio_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            return_audio_file_name(self.video_runner_obj)
        )

        try:
            if not os.path.exists(audio_file):
                raise FileNotFoundError(f"Audio file not found: {audio_file}")

            probe = ffmpeg.probe(audio_file)
            audio_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'audio'), None)

            if audio_stream and audio_stream['codec_name'] == 'flac':
                self.logger.info("Audio format check passed: FLAC codec")
                return True
            else:
                self.logger.warning("Audio is not in the expected format (FLAC)")
                return False

        except FileNotFoundError as e:
            self.logger.error(str(e))
            return False
        except ffmpeg.Error as e:
            self.logger.error(f"Error checking audio format: {e.stderr.decode()}")
            return False
        except Exception as e:
            self.logger.error(f"An unexpected error occurred while checking audio format: {str(e)}")
            return False

if __name__ == "__main__":
    # For testing purposes
    video_runner_obj = {
        "video_id": "test_video",
        "logger": print  # Use print as a simple logger for testing
    }
    audio_extractor = ExtractAudio(video_runner_obj)
    success = audio_extractor.extract_audio()
    print(f"Audio extraction {'succeeded' if success else 'failed'}")
    if success:
        metadata = audio_extractor.get_audio_metadata()
        print("Audio metadata:", metadata)
        format_check = audio_extractor.check_audio_format()
        print(f"Audio format check {'passed' if format_check else 'failed'}")