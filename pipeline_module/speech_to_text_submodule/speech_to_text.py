import os
import json
import datetime
from typing import Dict, Any, Optional
import audio_metadata
from web_server_module.web_server_database import get_status_for_youtube_id, update_status
from ..utils_module.utils import TRANSCRIPTS, return_audio_file_name, return_video_folder_name
from ..utils_module.timeit_decorator import timeit
from ..utils_module.google_services import (
    google_service_manager,
    GoogleServiceError
)

class SpeechToText:
    def __init__(self, video_runner_obj: Dict[str, Any]):
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        self.bucket_name = google_service_manager.bucket_name

        try:
            # Initialize clients through service manager
            self.client = google_service_manager.speech_client
            self.storage_client = google_service_manager.storage_client
            self.logger.info("Speech-to-Text service initialized successfully")
        except GoogleServiceError as e:
            self.logger.error(f"Failed to initialize Speech-to-Text service: {str(e)}")
            raise

    @timeit
    def get_speech_from_audio(self) -> bool:
        """Process audio file and generate transcription."""
        audio_file_name = return_audio_file_name(self.video_runner_obj)
        filepath = return_video_folder_name(self.video_runner_obj)
        file_name = os.path.join(filepath, audio_file_name)
        self.logger.info(f"Processing audio file: {file_name}")

        # Check if already processed
        if get_status_for_youtube_id(self.video_runner_obj.get("video_id"),
                                     self.video_runner_obj.get("AI_USER_ID")) == "done":
            self.logger.info("Speech to text already completed")
            return True

        try:
            # Get audio metadata
            frame_rate, channels = self.get_audio_metadata(file_name)

            # Upload to GCS
            gcs_uri = self.upload_blob(file_name, audio_file_name)
            self.logger.info(f"Audio uploaded to: {gcs_uri}")

            # Process audio
            response = self.recognize_speech(gcs_uri, frame_rate, channels)
            self.logger.info("Speech recognition completed")

            # Save results
            self.save_transcript(response)

            # Cleanup
            self.delete_blob(audio_file_name)

            # Update status
            update_status(self.video_runner_obj.get("video_id"),
                          self.video_runner_obj.get("AI_USER_ID"),
                          "done")

            self.logger.info("Speech-to-Text processing completed successfully")
            return True

        except Exception as e:
            self.logger.error(f"Speech-to-Text processing failed: {str(e)}")
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

    def recognize_speech(self, gcs_uri: str, frame_rate: int, channels: int) -> Dict:
        """Perform speech recognition."""
        try:
            # Get recognition config from service manager
            config = google_service_manager.get_speech_config(frame_rate, channels)

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

            # Convert response to dictionary format manually
            results_dict = {
                'results': [{
                    'alternatives': [{
                        'transcript': result.alternatives[0].transcript,
                        'confidence': result.alternatives[0].confidence,
                        'words': [{
                            'word': word.word,
                            'start_time': word.start_time.total_seconds(),
                            'end_time': word.end_time.total_seconds(),
                        } for word in result.alternatives[0].words]
                    } for alt in result.alternatives]
                } for result in response.results]
            }

            self.logger.info("Speech recognition completed successfully")
            return results_dict
        except Exception as e:
            self.logger.error(f"Speech recognition failed: {str(e)}")
            raise

    def save_transcript(self, response: Dict) -> None:
        """Save transcription results."""
        transcript_file = os.path.join(
            return_video_folder_name(self.video_runner_obj),
            TRANSCRIPTS
        )

        try:
            # Add metadata to the response
            response['metadata'] = {
                'processed_at': datetime.datetime.now().isoformat(),
                'audio_file': return_audio_file_name(self.video_runner_obj),
                'status': 'completed'
            }

            # Save with proper JSON formatting
            with open(transcript_file, "w", encoding='utf-8') as outfile:
                json.dump(response, outfile, indent=2, ensure_ascii=False)

            self.logger.info(f"Transcript saved to: {transcript_file}")

            # Verify file was written
            file_size = os.path.getsize(transcript_file)
            self.logger.info(f"Transcript file size: {file_size} bytes")

            if file_size == 0:
                raise ValueError("Transcript file is empty")

        except Exception as e:
            self.logger.error(f"Failed to save transcript: {str(e)}")
            # Try to save error information
            try:
                error_info = {
                    'error': str(e),
                    'timestamp': datetime.datetime.now().isoformat(),
                    'response_summary': str(response)[:1000]  # First 1000 chars only
                }
                with open(transcript_file, "w") as outfile:
                    json.dump(error_info, outfile, indent=2)
                self.logger.info("Error information saved")
            except Exception as inner_e:
                self.logger.error(f"Failed to save error information: {str(inner_e)}")

