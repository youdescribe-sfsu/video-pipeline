import json
import sys
import numpy as np
import requests
import yt_dlp as ydl

def generateYDXCaption(videoId):
    data = {
      "userId" : "65c433f7-ceb2-495d-ae01-994388ce56f5",
      "youtubeVideoId" : videoId
    }
    url = 'http://3.101.130.10:4000/api/create-user-links/create-new-user-ad'
    headers = {"Content-Type": "application/json; charset=utf-8"}
    response = requests.post(url, data=json.dumps(data), headers=headers)
    data = response.json()
    if(response.status_code == 200):
        print("Success")
        requests.get(data['url'])
    else:
      print("Failure in generating YDX Caption")

if __name__ == '__main__':
    print(sys.argv[1])
    generateYDXCaption(sys.argv[1])
