from logging import Logger
import ffmpeg
from typing import Dict
from ..utils_module.utils import read_value_from_file, return_video_download_location, \
    return_audio_file_name, return_video_folder_name, save_value_to_file
import os
from ..utils_module.timeit_decorator import timeit


class ExtractAudio:
    def __init__(self, video_runner_obj: Dict[str, int]):
        """
        Initialize ExtractAudio object.

        Parameters:
        video_runner_obj (Dict[str, int]): A dictionary that contains the information of the video.
            The keys are "video_id", "video_start_time", and "video_end_time", and their values are integers.
        """
        self.video_runner_obj = video_runner_obj
        self.logger: Logger = video_runner_obj.get("logger")

    @timeit
    def extract_audio(self):
        """
        Extracts audio from the video file and saves it as a FLAC file.
        The FLAC file will have the same name as the video file, with .flac as its extension.
        """
        # Define the input and output file paths
        input_file = return_video_download_location(self.video_runner_obj)
        output_file = os.path.join(return_video_folder_name(self.video_runner_obj),
                                   return_audio_file_name(self.video_runner_obj))

        if read_value_from_file(video_runner_obj=self.video_runner_obj, key="['ExtractAudio']['extract_audio']"):
            # Audio already extracted, skipping step
            self.logger.info("Audio already extracted, skipping step.")
            return

        try:
            # Check if the output file already exists
            if not os.path.exists(output_file):
                # Use ffmpeg to extract the audio and save it as a FLAC file
                self.logger.info(f"Extracting audio from {input_file} and saving it as {output_file}")

                (
                    ffmpeg
                    .input(input_file)
                    .output(output_file, acodec='flac', ac=2, ar='48k')
                    .overwrite_output()
                    .run(capture_stdout=True, capture_stderr=True)
                )

            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['ExtractAudio']['extract_audio']",
                               value=True)
            self.logger.info(f"Audio extraction completed.")

        except ffmpeg.Error as e:
            self.logger.error(f"FFmpeg error occurred: {e.stderr.decode()}")
            raise
        except Exception as e:
            self.logger.error(f"An unexpected error occurred during audio extraction: {str(e)}")
            raise

    def get_audio_metadata(self):
        """
        Retrieves metadata about the extracted audio file.
        """
        audio_file = os.path.join(return_video_folder_name(self.video_runner_obj),
                                  return_audio_file_name(self.video_runner_obj))

        try:
            probe = ffmpeg.probe(audio_file)
            audio_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'audio'), None)

            if audio_stream:
                return {
                    'sample_rate': audio_stream.get('sample_rate'),
                    'channels': audio_stream.get('channels'),
                    'duration': probe.get('format', {}).get('duration')
                }
            else:
                self.logger.warning("No audio stream found in the file.")
                return None

        except ffmpeg.Error as e:
            self.logger.error(f"FFmpeg error occurred while getting audio metadata: {e.stderr.decode()}")
            raise
        except Exception as e:
            self.logger.error(f"An unexpected error occurred while getting audio metadata: {str(e)}")
            raise