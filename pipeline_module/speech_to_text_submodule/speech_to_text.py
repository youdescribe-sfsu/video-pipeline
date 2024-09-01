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
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        self.bucket_name = "ydx"
        self.client = speech.SpeechClient()
        self.storage_client = storage.Client()

    @timeit
    def get_speech_from_audio(self) -> bool:
        audio_file_name = return_audio_file_name(self.video_runner_obj)
        filepath = return_video_folder_name(self.video_runner_obj)
        file_name = os.path.join(filepath, audio_file_name)

        if read_value_from_file(video_runner_obj=self.video_runner_obj,
                                key="['SpeechToText']['getting_speech_from_audio']") == 1:
            self.logger.info("Speech to text already completed, skipping step.")
            return True

        try:
            frame_rate, channels = self.get_audio_metadata(file_name)

            self.logger.info(f"Uploading {file_name} to cloud storage")
            gcs_uri = self.upload_blob(file_name, audio_file_name)

            self.logger.info("Starting speech recognition")
            response = self.recognize_speech(gcs_uri, frame_rate, channels)

            self.save_transcript(response)

            self.logger.info(f"Deleting {audio_file_name} from cloud storage")
            self.delete_blob(audio_file_name)

            save_value_to_file(video_runner_obj=self.video_runner_obj,
                               key="['SpeechToText']['getting_speech_from_audio']", value=1)
            self.logger.info("Speech to text completed successfully")

            return True

        except Exception as e:
            self.logger.error(f"Error in speech to text: {str(e)}")
            return False

    def upload_blob(self, source_file_name: str, destination_blob_name: str) -> str:
        bucket = self.storage_client.get_bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        self.logger.info(f"File {source_file_name} uploaded to {destination_blob_name}")
        return f"gs://{self.bucket_name}/{destination_blob_name}"

    def delete_blob(self, blob_name: str) -> None:
        bucket = self.storage_client.get_bucket(self.bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()
        self.logger.info(f"Blob {blob_name} deleted.")

    def get_audio_metadata(self, audio_file_name: str) -> tuple:
        self.logger.info(f"Extracting Audio metadata from {audio_file_name}")
        try:
            wave_file = audio_metadata.load(audio_file_name)
            frame_rate = wave_file["streaminfo"].sample_rate
            channels = wave_file["streaminfo"].channels
            self.logger.info(f"Audio frame_rate={frame_rate} and channels={channels}")
            return frame_rate, channels
        except Exception as e:
            self.logger.error(f"Error extracting audio metadata: {str(e)}")
            raise

    def recognize_speech(self, gcs_uri: str, frame_rate: int, channels: int) -> Dict[str, Any]:
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
        self.logger.info("Waiting for speech recognition operation to complete...")
        response = operation.result(timeout=600)  # 10 minutes timeout
        return response

    def save_transcript(self, response: Dict[str, Any]) -> None:
        transcript_file = os.path.join(return_video_folder_name(self.video_runner_obj), TRANSCRIPTS)
        with open(transcript_file, "w") as outfile:
            json.dump(speech.RecognizeResponse.to_dict(response), outfile, indent=2)
        self.logger.info(f"Transcript saved to {transcript_file}")

    def batch_recognize(self, gcs_uri: str, frame_rate: int, channels: int) -> List[Dict[str, Any]]:
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

        self.logger.info("Starting batch recognition...")
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

        return results

    def process_long_audio(self, gcs_uri: str, frame_rate: int, channels: int) -> List[Dict[str, Any]]:
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

        self.logger.info("Starting long audio processing...")
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

        return results


if __name__ == "__main__":
    # For testing purposes
    video_runner_obj = {
        "video_id": "test_video",
        "logger": print  # Use print as a simple logger for testing
    }
    speech_to_text = SpeechToText(video_runner_obj)
    success = speech_to_text.get_speech_from_audio()
    print(f"Speech to text conversion {'succeeded' if success else 'failed'}")