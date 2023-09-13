import json
from logging import Logger
import os
from typing import Dict
import requests

class GenerateYDXCaption:
    def __init__(self, video_runner_obj: Dict[str, int]):
        self.video_runner_obj = video_runner_obj
    
    def generateYDXCaption(self,ydx_server=None,AI_USER_ID=None,userId=None,ydx_app_host=None,logger:Logger=None):
        
        
        if(ydx_server == None):
            ydx_server = os.getenv('YDX_WEB_SERVER')

        if AI_USER_ID == None:
            AI_USER_ID = os.getenv('YDX_AI_USER_ID')
        
        if(userId == None):
            userId = os.getenv('YDX_USER_ID')
            
        
        if ydx_app_host == None:
            ydx_app_host = os.getenv('YDX_APP_HOST')
        
        
        # userId = os.getenv('YDX_USER_ID')
        # AI_USER_ID = os.getenv('YDX_AI_USER_ID')
        # if(userId == None):
        #     userId = "65c433f7-ceb2-495d-ae01-994388ce56f5"
        data = {
        "userId" : userId,
        "youtubeVideoId" : self.video_runner_obj.get("video_id"),
        "ydx_app_host" : ydx_app_host,
        # Change AI ID to the ID of the AI you want to use
        "AI_USER_ID": AI_USER_ID
        }
        # ydx_server = os.getenv('YDX_WEB_SERVER')
        # if(ydx_server == None):
        #     ydx_server = 'http://3.101.130.10:4000'
        url = '{}/api/create-user-links/generate-audio-desc-gpu'.format(ydx_server)
        headers = {"Content-Type": "application/json; charset=utf-8"}
        logger.info("===== UPLOADING DATA to {} =====".format(url))
        response = requests.post(url, data=json.dumps(data), headers=headers)
        logger.info("===== RESPONSE =====")
        logger.info(response.text)
        data = response.json()
        if(response.status_code == 200):
            print("Success in generating YDX Caption")
            logger.info("Success")
            logger.info(data)
        else:
            logger.info("Failure in generating YDX Caption")
            logger.info(data.get('message'))
        return