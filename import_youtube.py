import sys

import yt_dlp as ydl
from utils import returnVideoDownloadLocation
from yt_dlp.utils import download_range_func

def import_video(videoId):
    ydl_opts = {'outtmpl': returnVideoDownloadLocation(videoId), "format": "best","download_ranges":download_range_func(None, [(0, 60)]) }
    vid = ydl.YoutubeDL(ydl_opts).extract_info(
        url='https://www.youtube.com/watch?v=' + videoId, download=True)

    duration = vid["duration"]
    title = vid["title"]
    return

if __name__ == "__main__":
    import_video(sys.argv[1])
