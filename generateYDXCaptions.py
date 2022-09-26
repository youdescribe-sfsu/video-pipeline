import json
import sys
import numpy as np
import requests
import yt_dlp as ydl
from utils import returnVideoFolderName,SUMMARIZED_SCENES,OCR_FILTER_REMOVE_SIMILAR,TRANSCRIPTS,DIALOGS

def generateYDXCaption(videoId):
    data = {
      "userId" : "65c433f7-ceb2-495d-ae01-994388ce56f5",
      "youtubeVideoId" : videoId
    }
    url = 'http://3.101.130.10:4000/api/create-user-links/create-new-user-ad'
    headers = {"Content-Type": "application/json; charset=utf-8"}
    response = requests.post(url, data=json.dumps(data), headers=headers)
    data = response.json()
    print(data)
    finalUrl = data["message"].split()[3]
    print(finalUrl)
    finalResponse = requests.get(data["url"])

if __name__ == '__main__':
    print(sys.argv[1])
    generateYDXCaption(sys.argv[1])
