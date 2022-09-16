import json
import sys
import numpy as np
import requests
import yt_dlp as ydl
from utils import returnVideoFolderName,SUMMARIZED_SCENES,OCR_FILTER_REMOVE_SIMILAR,TRANSCRIPTS,DIALOGS

def generateYDXCaption(videoId):
    data = {
      "userId" : "a00206bf-e550-4429-97a5-011a2b63db0b",
      "youtubeVideoId" : videoId
    }
    url = 'https://ydx.youdescribe.org/api/create-user-links/create-new-user-ad'
    headers = {"Content-Type": "application/json; charset=utf-8"}
    response = requests.post(url, data=json.dumps(data), headers=headers)
    data = response.json()
    finalUrl = data["message"].split()[3]
    finalResponse = requests.get(finalUrl)

if __name__ == '__main__':
    print(sys.argv[1])
    generateYDXCaption(sys.argv[1])
