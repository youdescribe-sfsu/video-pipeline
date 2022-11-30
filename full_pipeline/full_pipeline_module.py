import os
from timeit_decorator import timeit
from utils import returnVideoFolderName
from ocr_extraction.ocr_extraction_module import OcrExtraction
from import_video.import_video_module import ImportVideo
from object_tracking.object_tracking_module import ObjectTracking
from audio_transcription.audio_transcription_module import AudioTranscription
from generate_YDX_captions.generate_YDX_captions_module import GenerateYDXCaptions
from multiprocessing import Process


class FullPipeline:
    def __init__(self,video_id,pagePort,video_start_time,video_end_time):
        self.video_id = video_id
        self.pagePort = pagePort
        self.video_start_time = video_start_time
        self.video_end_time = video_end_time

    def run_full_pipeline(self):
        print("=== Running Full Pipeline ===")
        print("Make Directory if not exists")
        os.makedirs(returnVideoFolderName(self.video_id), exist_ok=True)
        import_video = ImportVideo(self.video_id,self.video_start_time,self.video_end_time)
        import_video.download_and_extract_frames()
        ocr_extraction = OcrExtraction(self.video_id,self.video_start_time,self.video_end_time)
        ocr_extraction.run_ocr_detection()
        objectTracking = ObjectTracking(self.video_id,self.pagePort)
        objectTracking.run_object_detection_and_get_captions()
        if(self.video_start_time == None and self.video_end_time == None):
            audio_trancription = AudioTranscription(self.video_id)
            audio_trancription.run_audio_transcription()
            generateYDXCaption = GenerateYDXCaptions(self.video_id)
            generateYDXCaption.uploadAndGenerateCaptions()
        print("=== DONE! ===")
    
    @timeit
    def downloadAndGetAudioTranscription(self):
        import_video = ImportVideo(self.video_id,self.video_start_time,self.video_end_time)
        audio_trancription = AudioTranscription(self.video_id)
        jobs = []
        jobs.append(Process(target=import_video.download_and_extract_frames))
        jobs.append(Process(target=audio_trancription.run_audio_transcription))
        for j in jobs:
            j.start()

        # Ensure all of the processes have finished
        for j in jobs:
            j.join()
        return
    
    @timeit
    def runOCRandObjectDetection(self):
        ocr_extraction = OcrExtraction(self.video_id,self.video_start_time,self.video_end_time)
        objectTracking = ObjectTracking(self.video_id,self.pagePort)
        jobs = []
        jobs.append(Process(target=ocr_extraction.run_ocr_detection))
        jobs.append(Process(target=objectTracking.run_object_detection_and_get_captions))
        for j in jobs:
            j.start()

        # Ensure all of the processes have finished
        for j in jobs:
            j.join()
        return

    def generateYDXCaptions(self):
        generateYDXCaption = GenerateYDXCaptions(self.video_id)
        generateYDXCaption.uploadAndGenerateCaptions()
        return