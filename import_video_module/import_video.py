from timeit_decorator import timeit
import json
import yt_dlp as ydl
from utils import returnVideoDownloadLocation, returnVideoFolderName
from datetime import timedelta
import ffmpeg
import os

class ImportVideo:
    def __init__(self, video_id, video_start_time=None, video_end_time=None):
        """
        Initialize ImportVideo object
        
        Parameters:
        video_id (str): YouTube video ID
        video_start_time (int, optional): Start time of the video (in seconds). Defaults to None.
        video_end_time (int, optional): End time of the video (in seconds). Defaults to None.
        """
        self.video_id = video_id
        self.video_start_time = video_start_time
        self.video_end_time = video_end_time
    
    @timeit
    def download_video(self):
        """
        Download the video from YouTube
        
        Returns:
        None
        """
        # Download video from YouTube
        print("Downloading video from YouTube")
        
        ydl_opts = {'outtmpl': returnVideoDownloadLocation(self.video_id), "format": "best" }
        vid = ydl.YoutubeDL(ydl_opts).extract_info(
            url='https://www.youtube.com/watch?v=' + self.video_id, download=True)

        # Get Video Duration
        duration = vid.get('duration')

        # Get Video Title
        title = vid.get('title')
        print("Video Title: ", title)

        # Save metadata to json file
        with open(returnVideoFolderName(self.video_id) + '/metadata.json', 'w') as f:
            f.write(json.dumps({'duration': duration, 'title': title}))

        if self.video_start_time and self.video_end_time:
            # Convert start and end time to timedelta
            start_time = timedelta(seconds=int(self.video_start_time))
            end_time = timedelta(seconds=int(self.video_end_time))
            print("start time: ", start_time)
            print("end time: ", end_time)

            # Trim video and audio based on start and end time
            input_stream = ffmpeg.input(returnVideoDownloadLocation(self.video_id))
            vid = (
                input_stream.video
                .trim(start=self.video_start_time, end=self.video_end_time)
                .setpts('PTS-STARTPTS')
            )
            aud = (
                input_stream.audio
                .filter_('atrim', start=self.video_start_time, end=self.video_end_time)
                .filter_('asetpts', 'PTS-STARTPTS')
            )

            # Join trimmed video and audio
            joined = ffmpeg.concat(vid, aud, v=1, a=1).node

            # Output trimmed video
            output = ffmpeg.output(joined[0], joined[1], returnVideoFolderName(self.video_id) + '/trimmed.mp4')
            output.run(overwrite_output=True)

            # Delete original video
            if os.path.exists(returnVideoDownloadLocation(self.video_id)):
                os.remove(returnVideoDownloadLocation(self.video_id))

            # Rename trimmed video to original name
            os.rename(returnVideoFolderName(self.video_id) + '/trimmed.mp4', returnVideoDownloadLocation(self.video_id))
            
        return