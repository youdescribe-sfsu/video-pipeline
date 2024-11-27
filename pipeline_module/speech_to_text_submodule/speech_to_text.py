import os
import json
from typing import Dict, Any, Optional
import audio_metadata
from web_server_module.web_server_database import get_status_for_youtube_id, update_status
from ..utils_module.utils import TRANSCRIPTS, return_audio_file_name, return_video_folder_name
from ..utils_module.timeit_decorator import timeit
from ..utils_module.google_services import (
    service_manager,
    GoogleServiceError
)


class SpeechToText:
    def __init__(self, video_runner_obj: Dict[str, Any]):
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        self.bucket_name = service_manager.bucket_name

        try:
            # Initialize clients through service manager
            self.client = service_manager.speech_client
            self.storage_client = service_manager.storage_client
            self.logger.info("Speech-to-Text service initialized successfully")
        except GoogleServiceError as e:
            self.logger.error(f"Failed to initialize Speech-to-Text service: {str(e)}")
            raise

    @timeit
    def get_speech_from_audio(self) -> bool:
        """Process audio file and generate transcription."""
        try:
            # Step 1: Get file paths
            audio_file_name = return_audio_file_name(self.video_runner_obj)
            filepath = return_video_folder_name(self.video_runner_obj)
            file_name = os.path.join(filepath, audio_file_name)

            self.logger.info(f"Looking for audio file at: {file_name}")
            if not os.path.exists(file_name):
                self.logger.error(f"Audio file not found at: {file_name}")
                return False

            self.logger.info(f"Audio file found, size: {os.path.getsize(file_name)} bytes")

            # Step 2: Get audio metadata
            try:
                frame_rate, channels = self.get_audio_metadata(file_name)
                self.logger.info(f"Audio metadata extracted: {frame_rate}Hz, {channels} channels")
            except Exception as e:
                self.logger.error(f"Failed to extract audio metadata: {str(e)}")
                return False

            # Step 3: Upload to GCS
            try:
                gcs_uri = self.upload_blob(file_name, audio_file_name)
                self.logger.info(f"Audio uploaded to: {gcs_uri}")
            except Exception as e:
                self.logger.error(f"Failed to upload to GCS: {str(e)}")
                return False

            # Step 4: Process audio
            try:
                response = self.recognize_speech(gcs_uri, frame_rate, channels)
                self.logger.info("Speech recognition completed successfully")
            except Exception as e:
                self.logger.error(f"Speech recognition failed: {str(e)}")
                return False

            # Step 5: Save results
            try:
                self.save_transcript(response)
                self.logger.info("Transcript saved successfully")
            except Exception as e:
                self.logger.error(f"Failed to save transcript: {str(e)}")
                return False

            # Step 6: Cleanup
            try:
                self.delete_blob(audio_file_name)
                self.logger.info("GCS cleanup completed")
            except Exception as e:
                self.logger.error(f"Failed to cleanup GCS: {str(e)}")
                # Don't fail the overall process for cleanup failure

            return True

        except Exception as e:
            self.logger.error(f"Unexpected error in speech-to-text processing: {str(e)}")
            return False

    def get_audio_metadata(self, audio_file: str) -> tuple:
        """Extract audio metadata."""
        try:
            self.logger.info(f"Extracting metadata from: {audio_file}")
            wave_file = audio_metadata.load(audio_file)
            frame_rate = wave_file["streaminfo"].sample_rate
            channels = wave_file["streaminfo"].channels
            self.logger.info(f"Audio metadata: {frame_rate}Hz, {channels} channels")
            return frame_rate, channels
        except Exception as e:
            self.logger.error(f"Failed to extract audio metadata: {str(e)}")
            raise

    def upload_blob(self, source_file: str, destination_blob: str) -> str:
        """Upload file to Google Cloud Storage."""
        try:
            bucket = self.storage_client.get_bucket(self.bucket_name)
            blob = bucket.blob(destination_blob)
            blob.upload_from_filename(source_file)
            self.logger.info(f"File {source_file} uploaded to {destination_blob}")
            return f"gs://{self.bucket_name}/{destination_blob}"
        except Exception as e:
            self.logger.error(f"Failed to upload file to GCS: {str(e)}")
            raise

    def recognize_speech(self, gcs_uri: str, frame_rate: int, channels: int) -> Dict:
        """Perform speech recognition."""
        try:
            # Get recognition config from service manager
            config = service_manager.get_speech_config(frame_rate, channels)

            # Create recognition audio object
            audio = {"uri": gcs_uri}

            # Start recognition
            self.logger.info("Starting speech recognition")
            operation = self.client.long_running_recognize(
                config=config,
                audio=audio
            )

            # Wait for completion
            self.logger.info("Waiting for operation to complete...")
            response = operation.result(timeout=600)

            self.logger.info("Speech recognition completed successfully")
            return response.to_dict()
        except Exception as e:
            self.logger.error(f"Speech recognition failed: {str(e)}")
            raise

    def delete_blob(self, blob_name: str) -> None:
        """Delete file from Google Cloud Storage."""
        try:
            bucket = self.storage_client.get_bucket(self.bucket_name)
            blob = bucket.blob(blob_name)
            blob.delete()
            self.logger.info(f"Deleted {blob_name} from GCS")
        except Exception as e:
            self.logger.error(f"Failed to delete blob: {str(e)}")
            # Don't raise - this is cleanup

    def save_transcript(self, response: Dict) -> None:
        """Save transcription results."""
        transcript_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            TRANSCRIPTS
        )

        try:
            with open(transcript_file, "w") as outfile:
                json.dump(response, outfile, indent=2)
            self.logger.info(f"Transcript saved to: {transcript_file}")

            # Verify file was written
            with open(transcript_file, "r") as infile:
                content = infile.read()
                self.logger.info(f"Transcript file size: {len(content)} bytes")
        except Exception as e:
            self.logger.error(f"Failed to save transcript: {str(e)}")
            # Try to save error information
            try:
                with open(transcript_file, "w") as outfile:
                    json.dump({
                        "error": str(e),
                        "response": str(response)
                    }, outfile, indent=2)
                self.logger.info("Error information saved")
            except Exception as inner_e:
                self.logger.error(f"Failed to save error information: {str(inner_e)}")