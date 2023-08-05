from utils import TRANSCRIPTS, load_progress_from_file, save_progress_to_file
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "tts_cloud_key.json"
import audio_metadata
from google.cloud import speech_v1p1beta1 as speech
from google.cloud import storage
from timeit_decorator import timeit
from typing import Dict
from utils import return_audio_file_name,return_video_folder_name

## Make Speech to Text Module
class SpeechToText:
    """
    A class to convert speech in a video to text

    Attributes:
        video_id (str): The identifier of the video to be transcribed
        audio_file (str): The path to the audio file
    """

    def __init__(self, video_runner_obj: Dict[str, int]):
        """
        Initialize SpeechToText object.
        
        Parameters:
        video_runner_obj (Dict[str, int]): A dictionary that contains the information of the video.
            The keys are "video_id", "video_start_time", and "video_end_time", and their values are integers.
        """
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        self.progress_file = load_progress_from_file(video_runner_obj)
    
    @timeit
    def get_speech_from_audio(self):
        """
        Extracts speech from an audio file associated with a video and returns the transcription.

        This function uploads the audio file to a Google Cloud Storage bucket, performs speech recognition using
        Google Cloud Speech-to-Text API, deletes the data from the bucket, and saves the transcription to a file.

        Returns:
            None.
        """
        audio_file_name = return_audio_file_name(self.video_runner_obj)
        filepath = return_video_folder_name(self.video_runner_obj) + "/"
        file_name = filepath + audio_file_name

        frame_rate, channels = self.frame_rate_channel(file_name)

        bucket_name = "ydx"
        source_file_name = filepath + audio_file_name
        destination_blob_name = audio_file_name
        self.logger.info(f"Uploading {source_file_name} to {destination_blob_name}")
        self.upload_blob(bucket_name, source_file_name, destination_blob_name)
        self.progress_file['SpeechToText']['upload_blob'] = True
        save_progress_to_file(video_runner_obj=self.video_runner_obj, progress_data=self.progress_file)

        gcs_uri = "gs://" + bucket_name + "/" + audio_file_name

        client = speech.SpeechClient()
        audio = speech.RecognitionAudio(uri=gcs_uri)

        config = speech.RecognitionConfig(
            encoding=speech.RecognitionConfig.AudioEncoding.FLAC,
            audio_channel_count=channels,
            sample_rate_hertz=frame_rate,
            use_enhanced=True,
            enable_speaker_diarization=True,
            language_code="en-US",
        )
        if(self.progress_file['SpeechToText']['getting_speech_from_audio'] == 0):
            operation = client.long_running_recognize(config=config, audio=audio)
            response = operation.result(timeout=10000)
            response = type(response).to_json(response)
            with open(
            return_video_folder_name(self.video_runner_obj) + "/" + TRANSCRIPTS, "w"
        ) as outfile:
                outfile.write(response)
                self.progress_file['SpeechToText']['getting_speech_from_audio'] = 1
                save_progress_to_file(video_runner_obj=self.video_runner_obj, progress_data=self.progress_file)
        
        self.logger.info(f"Deleting {destination_blob_name} from {bucket_name}")
        self.delete_blob(bucket_name, destination_blob_name)
        self.progress_file['SpeechToText']['delete_blob'] = True
        save_progress_to_file(video_runner_obj=self.video_runner_obj, progress_data=self.progress_file)
        # with open(
        #     return_video_folder_name(self.video_runner_obj) + "/" + TRANSCRIPTS, "w"
        # ) as outfile:
        #     outfile.write(response)
        return

    def upload_blob(self, bucket_name, source_file_name, destination_blob_name):
        """
        Uploads a file to the specified Google Cloud Storage bucket.

        Parameters:
            bucket_name (str): The name of the GCS bucket
            source_file_name (str): The file path of the source file to be uploaded
            destination_blob_name (str): The name of the destination blob in the bucket
        """
        if(self.progress_file['SpeechToText']['upload_blob']):
            ## Audio already uploaded, skipping step
            self.logger.info("Audio already uploaded, skipping step.")
            return
        self.logger.info(f"Uploading {source_file_name} to {destination_blob_name}")
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        self.logger.info(f"File {source_file_name} uploaded to {destination_blob_name}")
        return

    def delete_blob(self, bucket_name, blob_name):
        """
        Deletes a blob from the specified Google Cloud Storage bucket.

        Parameters:
            bucket_name (str): The name of the GCS bucket
            blob_name (str): The name of the blob to be deleted
        """
        if(self.progress_file['SpeechToText']['delete_blob']):
            ## Audio already deleted, skipping step
            self.logger.info("Audio already deleted, skipping step.")
            return
        self.logger.info(f"Deleting {blob_name} from {bucket_name}")
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()
        self.logger.info(f"File {blob_name} deleted from {bucket_name}")
        return

    def frame_rate_channel(self, audio_file_name):
        """
        Extracts the frame rate and number of channels of an audio file.

        Parameters:
            audio_file_name (str): The file path of the audio file

        Returns:
            tuple: A tuple of integers, representing the frame rate and number of channels of the audio file
        """
        self.logger.info(f"Extracting Audio metadata from {audio_file_name}")
        wave_file = audio_metadata.load(audio_file_name)
        frame_rate = wave_file["streaminfo"].sample_rate
        channels = wave_file["streaminfo"].channels
        self.logger.info(f"Audio frame_rate={frame_rate} and channels={channels}")
        return frame_rate, channels
