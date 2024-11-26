import os
import json
from typing import Optional, Dict, Any
from functools import lru_cache
from google.cloud import speech_v1p1beta1, vision, storage
from web_server_module.custom_logger import setup_logger

logger = setup_logger()


class GoogleServiceError(Exception):
    """Custom exception for Google service errors"""
    pass


class GoogleServiceManager:
    """Manages Google Cloud service clients and credentials"""
    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(GoogleServiceManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self._credentials_path = os.getenv('GOOGLE_CREDENTIALS_PATH')
            self._speech_client = None
            self._vision_client = None
            self._storage_client = None
            self._bucket_name = os.getenv('GOOGLE_BUCKET_NAME', 'ydx')
            self._initialized = True
            logger.info("GoogleServiceManager initialized")

    def validate_credentials(self):
        """Public method to validate all credentials and environment setup"""
        try:
            # Validate environment variables
            self._validate_env()

            # Validate credential files
            required_creds = {
                'speech': os.path.join(self._credentials_path, 'speech-to-text.json'),
                'vision': os.path.join(self._credentials_path, 'vision-api.json'),
                'tts': os.path.join(self._credentials_path, 'text-to-speech.json'),
                'youtube': os.path.join(self._credentials_path, 'youtube-api.json')
            }

            for service, path in required_creds.items():
                if not self._validate_credentials_file(path):
                    raise GoogleServiceError(f"Invalid credentials file for {service}: {path}")

            logger.info("All credentials validated successfully")
            return True

        except Exception as e:
            logger.error(f"Credential validation failed: {str(e)}")
            raise GoogleServiceError(f"Credential validation failed: {str(e)}")

    def _validate_env(self):
        """Validate required environment variables"""
        required_vars = [
            'GOOGLE_CREDENTIALS_PATH',
            'GOOGLE_SPEECH_CREDENTIALS',
            'GOOGLE_VISION_CREDENTIALS',
            'GOOGLE_TTS_CREDENTIALS',
            'GOOGLE_YOUTUBE_CREDENTIALS'
        ]
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            raise GoogleServiceError(f"Missing environment variables: {', '.join(missing)}")

    def _validate_credentials_file(self, path: str) -> bool:
        """Validate credential file exists and is valid JSON"""
        try:
            if not os.path.exists(path):
                raise GoogleServiceError(f"Credentials file not found: {path}")
            with open(path, 'r') as f:
                json.load(f)  # Validate JSON format
            return True
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in credentials file: {path}")
            return False
        except Exception as e:
            logger.error(f"Error validating credentials file: {str(e)}")
            return False

    # Rest of the methods remain the same...
    @property
    @lru_cache(maxsize=1)
    def speech_client(self) -> speech_v1p1beta1.SpeechClient:
        """Get Speech-to-Text client with proper credentials"""
        if self._speech_client is None:
            creds_path = os.getenv('GOOGLE_SPEECH_CREDENTIALS')
            self._validate_credentials_file(creds_path)
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = creds_path
            self._speech_client = speech_v1p1beta1.SpeechClient()
            logger.info("Speech-to-Text client initialized")
        return self._speech_client

    @property
    def bucket_name(self) -> str:
        """Get configured bucket name"""
        return self._bucket_name

    def cleanup(self):
        """Reset all clients - useful for testing"""
        self._speech_client = None
        self._vision_client = None
        self._storage_client = None
        logger.info("All clients reset")


# Global instance for easy access
service_manager = GoogleServiceManager()