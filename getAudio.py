import json
import sys
import numpy as np
import requests
from youtube_dl import YoutubeDL
from utils import returnVideoFolderName

def getAudioFromVideo(videoId):
    ydl_opts = {
        'outtmpl': returnVideoFolderName("upSnt11tngE")+"/%(id)s.%(ext)s",
        'format': 'raw/bestaudio/best',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'flac',
        }],
    }
    YoutubeDL(ydl_opts).extract_info("http://www.youtube.com/watch?v=upSnt11tngE")
    return

if __name__ == '__main__':
    getAudioFromVideo(sys.argv[1])