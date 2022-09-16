import sys

import yt_dlp as ydl
import requests
from utils import returnVideoDownloadLocation,returnVideoFolderName,returnAudioFileName,TRANSCRIPTS
import os
# from __future__ import unicode_literals
from youtube_dl import YoutubeDL
import json

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="tts_cloud_key.json"
import audio_metadata

# en-US

# en-US

from google.cloud import speech_v1p1beta1 as speech
from google.cloud import storage

filepath = returnVideoFolderName("upSnt11tngE")+ "/"

def getAudioFromVideo(videoId):
    print("--Downloading Audio from youtube--")
    ydl_opts = {
        'outtmpl': returnVideoFolderName(videoId)+"/%(id)s.%(ext)s",
        'format': 'raw/bestaudio/best',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'flac',
        }],
    }
    YoutubeDL(ydl_opts).extract_info("http://www.youtube.com/watch?v="+videoId, download=True)
    print("--Audio from youtube downloaded--")
    return

def frame_rate_channel(audio_file_name):
    print("--Extracting Audio metadata--")
    wave_file = audio_metadata.load(audio_file_name)
    frame_rate = wave_file['streaminfo'].sample_rate
    channels = wave_file['streaminfo'].channels
    print("--Audio frame_rate={} and channels={}--".format(frame_rate,channels))
    return frame_rate,channels

def google_transcribe(videoId):
    audio_file_name = returnAudioFileName(videoId)
    filepath = returnVideoFolderName(videoId)+"/"
    file_name = filepath + audio_file_name

    # The name of the audio file to transcribe
    print("===========")
    print(file_name)
    print("============")
    frame_rate, channels = frame_rate_channel(file_name)
    
    bucket_name = 'ydx'
    source_file_name = filepath + audio_file_name
    destination_blob_name = audio_file_name
    print("===========")
    print("Uploading to Google Bucket")
    print("============")
    upload_blob(bucket_name, source_file_name, destination_blob_name)
    
    gcs_uri = 'gs://'+bucket_name+'/' + audio_file_name
    transcript = ''
    
    client = speech.SpeechClient()
    audio = speech.RecognitionAudio(uri=gcs_uri)

    config = speech.RecognitionConfig(
    encoding=speech.RecognitionConfig.AudioEncoding.FLAC,
    audio_channel_count= channels,
    sample_rate_hertz=frame_rate,
    use_enhanced=True,
    enable_speaker_diarization=True,
    language_code='en-US')

    # Detects speech in the audio file
    print("===========")
    print("Uploading to Speech to text")
    print("============")
    operation = client.long_running_recognize(config=config, audio=audio)
    print('==========opertaion')
    print(operation)
    print('=============')
    response = operation.result(timeout=10000)
    response = type(response).to_json(response)
    print("===========")
    print("Response from Speech to text")
    print(response)
    print(type(response))
    print("============")
    print("===========")
    print("Deleting Data from Bucket")
    print("============")
    delete_blob(bucket_name, destination_blob_name)
    print("===========")
    print("Deleted Data from Bucket")
    print("============")
    print("===========")
    print("Transcripts")
    print(transcript)
    print("============")
    f= open(returnVideoFolderName(videoId)+'/'+TRANSCRIPTS,"w")
    f.write(response)
    f.close() 
    return response

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

def delete_blob(bucket_name, blob_name):
    """Deletes a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)

    blob.delete()

if __name__ == '__main__':
    videoId = "upSnt11tngE"
    transcript = google_transcribe(videoId)