import os
import json
from google.cloud import speech_v1p1beta1 as speech
from google.cloud import storage
from typing import Dict, Any, List, Optional
import audio_metadata
from ..utils_module.utils import TRANSCRIPTS, read_value_from_file, save_value_to_file, return_audio_file_name, \
    return_video_folder_name
from ..utils_module.timeit_decorator import timeit


class SpeechToText:
    def __init__(self, video_runner_obj: Dict[str, Any]):
        print("Initializing SpeechToText")
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        self.bucket_name = "ydx"
        self.client = speech.SpeechClient()
        self.storage_client = storage.Client()
        print(f"SpeechToText initialized with video_runner_obj: {video_runner_obj}")

    @timeit
    def get_speech_from_audio(self) -> bool:
        print("Starting get_speech_from_audio method")
        audio_file_name = return_audio_file_name(self.video_runner_obj)
        filepath = return_video_folder_name(self.video_runner_obj)
        file_name = os.path.join(filepath, audio_file_name)
        print(f"Audio file: {file_name}")

        if read_value_from_file(video_runner_obj=self.video_runner_obj,
                                key="['SpeechToText']['getting_speech_from_audio']") == 1:
            print("Speech to text already completed, skipping step.")
            return True

        try:
            frame_rate, channels = self.get_audio_metadata(file_name)

            print(f"Uploading {file_name} to cloud storage")
            gcs_uri = self.upload_blob(file_name, audio_file_name)

            print("Starting speech recognition")
            response = self.recognize_speech(gcs_uri, frame_rate, channels)
            print("Resposne ", response)

            self.save_transcript(response)

            print(f"Deleting {audio_file_name} from cloud storage")
            self.delete_blob(audio_file_name)

            save_value_to_file(video_runner_obj=self.video_runner_obj,
                               key="['SpeechToText']['getting_speech_from_audio']", value=1)
            print("Speech to text completed successfully")

            return True

        except Exception as e:
            print(f"Error in speech to text: {str(e)}")
            return False

    def upload_blob(self, source_file_name: str, destination_blob_name: str) -> str:
        print(f"Uploading {source_file_name} to {destination_blob_name}")
        bucket = self.storage_client.get_bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        print(f"File {source_file_name} uploaded to {destination_blob_name}")
        return f"gs://{self.bucket_name}/{destination_blob_name}"

    def delete_blob(self, blob_name: str) -> None:
        print(f"Deleting blob {blob_name}")
        bucket = self.storage_client.get_bucket(self.bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()
        print(f"Blob {blob_name} deleted.")

    def get_audio_metadata(self, audio_file_name: str) -> tuple:
        print(f"Extracting Audio metadata from {audio_file_name}")
        try:
            wave_file = audio_metadata.load(audio_file_name)
            frame_rate = wave_file["streaminfo"].sample_rate
            channels = wave_file["streaminfo"].channels
            print(f"Audio frame_rate={frame_rate} and channels={channels}")
            return frame_rate, channels
        except Exception as e:
            print(f"Error extracting audio metadata: {str(e)}")
            raise

    def recognize_speech(self, gcs_uri: str, frame_rate: int, channels: int) -> Dict[str, Any]:
        print(f"Recognizing speech from {gcs_uri}")
        audio = speech.RecognitionAudio(uri=gcs_uri)
        config = speech.RecognitionConfig(
            encoding=speech.RecognitionConfig.AudioEncoding.FLAC,
            sample_rate_hertz=frame_rate,
            audio_channel_count=channels,
            language_code="en-US",
            enable_word_time_offsets=True,
            enable_automatic_punctuation=True,
            use_enhanced=True,
            model="video"
        )

        operation = self.client.long_running_recognize(config=config, audio=audio)
        print("Waiting for speech recognition operation to complete...")
        response = operation.result(timeout=600)  # 10 minutes timeout
        print("Speech recognition completed")

        # Convert the response to a dictionary
        response_dict = speech.LongRunningRecognizeResponse.to_dict(response)
        return response_dict

    def save_transcript(self, response: Dict[str, Any]) -> None:
        transcript_file = os.path.join(return_video_folder_name(self.video_runner_obj), TRANSCRIPTS)
        print(f"Attempting to save transcript to {transcript_file}")

        try:
            with open(transcript_file, "w") as outfile:
                json.dump(response, outfile, indent=2)

            print(f"Transcript successfully saved to {transcript_file}")

            with open(transcript_file, "r") as infile:
                content = infile.read()
                print(f"File content (first 1000 characters): {content[:1000]}")

        except Exception as e:
            print(f"Error in save_transcript: {str(e)}")

            # Try to save whatever we can
            try:
                with open(transcript_file, "w") as outfile:
                    json.dump({"error": str(e), "response": str(response)}, outfile, indent=2)
                print(f"Error information saved to {transcript_file}")
            except Exception as inner_e:
                print(f"Could not save error information: {str(inner_e)}")

    def batch_recognize(self, gcs_uri: str, frame_rate: int, channels: int) -> List[Dict[str, Any]]:
        print(f"Starting batch recognition for {gcs_uri}")
        audio = speech.RecognitionAudio(uri=gcs_uri)
        config = speech.RecognitionConfig(
            encoding=speech.RecognitionConfig.AudioEncoding.FLAC,
            sample_rate_hertz=frame_rate,
            audio_channel_count=channels,
            language_code="en-US",
            enable_word_time_offsets=True,
            enable_automatic_punctuation=True,
            use_enhanced=True,
            model="video"
        )

        operation = self.client.long_running_recognize(config=config, audio=audio)
        response = operation.result(timeout=600)  # 10 minutes timeout

        results = []
        for result in response.results:
            results.append({
                'transcript': result.alternatives[0].transcript,
                'confidence': result.alternatives[0].confidence,
                'words': [{'word': word.word, 'start_time': word.start_time.total_seconds(),
                           'end_time': word.end_time.total_seconds()} for word in result.alternatives[0].words]
            })

        print(f"Batch recognition completed. Found {len(results)} results.")
        return results

    def process_long_audio(self, gcs_uri: str, frame_rate: int, channels: int) -> List[Dict[str, Any]]:
        print(f"Starting long audio processing for {gcs_uri}")
        audio = speech.RecognitionAudio(uri=gcs_uri)
        config = speech.RecognitionConfig(
            encoding=speech.RecognitionConfig.AudioEncoding.FLAC,
            sample_rate_hertz=frame_rate,
            audio_channel_count=channels,
            language_code="en-US",
            enable_word_time_offsets=True,
            enable_automatic_punctuation=True,
            use_enhanced=True,
            model="video"
        )

        operation = self.client.long_running_recognize(config=config, audio=audio)

        response = operation.result(timeout=1800)  # 30 minutes timeout

        results = []
        for result in response.results:
            results.append({
                'transcript': result.alternatives[0].transcript,
                'confidence': result.alternatives[0].confidence,
                'words': [{'word': word.word, 'start_time': word.start_time.total_seconds(),
                           'end_time': word.end_time.total_seconds()} for word in result.alternatives[0].words]
            })

        print(f"Long audio processing completed. Found {len(results)} results.")
        return results