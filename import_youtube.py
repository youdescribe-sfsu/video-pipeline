import sys

import yt_dlp as ydl
import requests


def import_video(videoId):
    ydl_opts = {'outtmpl': './'+videoId, "format": "best", }
    vid = ydl.YoutubeDL(ydl_opts).extract_info(
        url='https://www.youtube.com/watch?v=' + videoId, download=True)

    duration = vid["duration"]
    title = vid["title"]

    scene_data_URL = "http://localhost:5001/vi/uploadVideo?videoid=" + \
        videoId + "&duration=" + str(duration) + "&title="+title
    # r = requests.get(url = scene_data_URL)
    return
