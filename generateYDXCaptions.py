import json
import sys
import requests
import os

def generateYDXCaption(videoId):
    userId = os.getenv('YDX_USER_ID')
    if(userId == None):
      userId = "65c433f7-ceb2-495d-ae01-994388ce56f5"
    data = {
      "userId" : userId,
      "youtubeVideoId" : videoId
    }
    ydx_server = os.getenv('YDX_WEB_SERVER')
    if(ydx_server == None):
        ydx_server = 'http://3.101.130.10:4000'
    url = '{}/api/create-user-links/create-new-user-ad'.format(ydx_server)
    headers = {"Content-Type": "application/json; charset=utf-8"}
    response = requests.post(url, data=json.dumps(data), headers=headers)
    data = response.json()
    if(response.status_code == 200):
        print("Success")
        requests.get(data['url'])
    else:
      print("Failure in generating YDX Caption")
      print(data.get('message'))

if __name__ == '__main__':
    print(sys.argv[1])
    generateYDXCaption(sys.argv[1])
