from audio_transcription.speechToText import google_transcribe,getAudioFromVideo
from timeit_decorator import timeit

class AudioTranscription:
    def __init__(self,video_id):
        self.video_id = video_id
    
    @timeit
    def run_audio_transcription(self):
        print("+ Running Audio Transcription")
        getAudioFromVideo(self.video_id)
        google_transcribe(self.video_id)