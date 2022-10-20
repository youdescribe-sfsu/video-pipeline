import sys

import yt_dlp as ydl
from utils import returnVideoDownloadLocation
from yt_dlp.utils import download_range_func


def import_video(videoId,start_time=None,end_time=None):
    ydl_opts = {'outtmpl': returnVideoDownloadLocation(videoId), "format": "best" }
    if(start_time != None and end_time != None):
        ydl_opts["download_ranges"]:download_range_func(None, [(start_time, end_time)])
    vid = ydl.YoutubeDL(ydl_opts).extract_info(
        url='https://www.youtube.com/watch?v=' + videoId, download=True)

    duration = vid["duration"]
    title = vid["title"]
    return

if __name__ == "__main__":
    import_video(sys.argv[1])
