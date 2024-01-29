from typing import List
from pydantic import BaseModel
import logging

class DialogueTimestamp(BaseModel):
    sequence_num: int
    start_time: float
    end_time: float
    duration: float

class AudioClip(BaseModel):
    start_time: float
    text: str
    scene_number: int
    type: str

class UploadToYDXData(BaseModel):
    youtube_id: str
    audio_clips: List[AudioClip]
    video_length: int
    video_name: str
    dialogue_timestamps: List[DialogueTimestamp]
    aiUserId: str

class VideoRunnerObj(BaseModel):
    video_id: str
    video_start_time: str
    video_end_time: str
    logger: logging.Logger
    ydx_server: str
    AI_USER_ID: str
    
    

class PipelineRunnerConfig(BaseModel):
    video_id: str
    video_start_time: str
    video_end_time: str
    upload_to_server: bool
    tasks: List[str]
    ydx_server: str
    ydx_app_host: str
    userId: str
    AI_USER_ID: str