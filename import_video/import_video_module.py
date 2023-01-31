from import_video.extract_frames import extract_frames
from import_video.import_youtube import import_video
from timeit_decorator import timeit

class ImportVideo:
    def __init__(self,video_id,video_start_time,video_end_time):
        self.video_id = video_id
        self.video_start_time = video_start_time
        self.video_end_time = video_end_time
    
    @timeit
    def download_and_extract_frames(self):
        try:
            print("=== DOWNLOAD VIDEO ===")
            print("start time: ",self.video_start_time)
            print("end time: ",self.video_end_time)
            import_video(self.video_id,self.video_start_time,self.video_end_time)
            print("=== EXTRACT FRAMES ===")
            extract_frames(self.video_id, 10, True)
            return True
        except Exception as e:
            print("IMPORT VIDEO ERROR: ",e)
            return False