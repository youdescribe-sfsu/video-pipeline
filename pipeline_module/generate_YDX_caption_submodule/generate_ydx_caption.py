import json
import os
from typing import Dict
import requests

class GenerateYDXCaption:
    def __init__(self, video_runner_obj: Dict[str, int]):
        self.video_runner_obj = video_runner_obj
    
    def generateYDXCaption(self,ydx_server=None,aiUserId=None,userId=None,ydx_app_host=None):
        
        
        if(ydx_server == None):
            ydx_server = os.getenv('YDX_WEB_SERVER')

        if aiUserId == None:
            aiUserId = os.getenv('YDX_AI_USER_ID')
        
        if(userId == None):
            userId = os.getenv('YDX_USER_ID')
            
        
        if ydx_app_host == None:
            ydx_app_host = os.getenv('YDX_APP_HOST')
        
        
        # userId = os.getenv('YDX_USER_ID')
        # aiUserId = os.getenv('YDX_AI_USER_ID')
        # if(userId == None):
        #     userId = "65c433f7-ceb2-495d-ae01-994388ce56f5"
        data = {
        "userId" : userId,
        "youtubeVideoId" : self.video_runner_obj.get("video_id"),
        "ydx_app_host" : ydx_app_host,
        # Change AI ID to the ID of the AI you want to use
        "aiUserId": aiUserId
        }
        # ydx_server = os.getenv('YDX_WEB_SERVER')
        # if(ydx_server == None):
        #     ydx_server = 'http://3.101.130.10:4000'
        url = '{}/api/create-user-links/generate-audio-desc-gpu'.format(ydx_server)
        headers = {"Content-Type": "application/json; charset=utf-8"}
        self.video_runner_obj["logger"].info("===== UPLOADING DATA to {} =====".format(url))
        response = requests.post(url, data=json.dumps(data), headers=headers)
        self.video_runner_obj["logger"].info("===== RESPONSE =====")
        self.video_runner_obj["logger"].info(response.text)
        data = response.json()
        if(response.status_code == 200):
            print("Success in generating YDX Caption")
            self.video_runner_obj["logger"].info("Success")
            self.video_runner_obj["logger"].info(data)
            # requests.get(data['url'])
        else:
            self.video_runner_obj["logger"].info("Failure in generating YDX Caption")
            self.video_runner_obj["logger"].info(data.get('message'))
            print("Failure in generating YDX Caption")
            print(data.get('message'))
        
        return