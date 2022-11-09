import subprocess
import sys

import yt_dlp as ydl
from utils import returnVideoDownloadLocation
from yt_dlp.utils import download_range_func
from datetime import timedelta
from timeit_decorator import timeit

@timeit
def import_video(videoId,video_start_time,video_end_time):
    print("Downloading video from youtube")
    if(video_start_time != None and video_end_time != None):
        ydl_opts = {'outtmpl': returnVideoDownloadLocation(videoId), "format": "best" }
        vid = ydl.YoutubeDL(ydl_opts).extract_info(
            url='https://www.youtube.com/watch?v=' + videoId, download=False)
        start_time = timedelta(seconds=int(video_start_time))
        end_time = timedelta(seconds=int(video_end_time))
        print("start time: ",start_time)
        print("end time: ",end_time)
        command = ['ffmpeg', '-y', 
                    '-ss', str(start_time), 
                    '-i', vid['url'],
                    '-t', str(end_time - start_time),
                    '-c', 'copy', returnVideoDownloadLocation(videoId)]
        print(subprocess.run(command))
    else:
        ydl_opts = {'outtmpl': returnVideoDownloadLocation(videoId), "format": "best" }
        vid = ydl.YoutubeDL(ydl_opts).extract_info(
            url='https://www.youtube.com/watch?v=' + videoId, download=True)
    print("Video downloaded from youtube")
    return

if __name__ == "__main__":
    import_video(sys.argv[1])
