from utils import returnVideoFolderName, returnAudioFileName, TRANSCRIPTS
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
    
    @timeit
    def get_speech_from_audio(self):
        audio_file_name = return_audio_file_name(self.video_runner_obj)
        filepath = return_video_folder_name(self.video_runner_obj) + "/"
        file_name = filepath + audio_file_name

        frame_rate, channels = self.frame_rate_channel(file_name)

        bucket_name = "ydx"
        source_file_name = filepath + audio_file_name
        destination_blob_name = audio_file_name
        print("Uploading to Google Bucket")
        self.upload_blob(bucket_name, source_file_name, destination_blob_name)

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
        operation = client.long_running_recognize(config=config, audio=audio)
        response = operation.result(timeout=10000)
        response = type(response).to_json(response)
        print("Deleting Data from Bucket")
        self.delete_blob(bucket_name, destination_blob_name)
        with open(
            return_video_folder_name(self.video_runner_obj) + "/" + TRANSCRIPTS, "w"
        ) as outfile:
            outfile.write(response)
        return response

    def upload_blob(self, bucket_name, source_file_name, destination_blob_name):
        """
        Uploads a file to the specified Google Cloud Storage bucket.

        Parameters:
            bucket_name (str): The name of the GCS bucket
            source_file_name (str): The file path of the source file to be uploaded
            destination_blob_name (str): The name of the destination blob in the bucket
        """
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        return

    def delete_blob(self, bucket_name, blob_name):
        """
        Deletes a blob from the specified Google Cloud Storage bucket.

        Parameters:
            bucket_name (str): The name of the GCS bucket
            blob_name (str): The name of the blob to be deleted
        """
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()
        return

    def frame_rate_channel(self, audio_file_name):
        """
        Extracts the frame rate and number of channels of an audio file.

        Parameters:
            audio_file_name (str): The file path of the audio file

        Returns:
            tuple: A tuple of integers, representing the frame rate and number of channels of the audio file
        """
        print("Extracting Audio metadata")
        wave_file = audio_metadata.load(audio_file_name)
        frame_rate = wave_file["streaminfo"].sample_rate
        channels = wave_file["streaminfo"].channels
        print("Audio frame_rate={} and channels={}".format(frame_rate, channels))
        return frame_rate, channels
