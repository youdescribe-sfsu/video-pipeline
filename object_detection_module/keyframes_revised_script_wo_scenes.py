'''
version: Python3.7
'''
"""
Unused Python File
------------------
This Python file contains code that is currently not being used anywhere in the project.
It is kept for reference purposes or potential future use.

Date: August 12, 2023
"""

# Inserting the extracted keyframes in db (replace csv file path and video id)
import requests
import pandas as pd
from utils import FRAME_INDEX_SELECTOR, KEY_FRAME_HEADERS,KEYFRAMES_CSV,TIMESTAMP_SELECTOR,IS_KEYFRAME_SELECTOR,KEYFRAME_CAPTION_SELECTOR


df = pd.read_csv("./qN8DRJ8OMcA_data.csv")
df = df[KEY_FRAME_HEADERS[FRAME_INDEX_SELECTOR],KEY_FRAME_HEADERS[TIMESTAMP_SELECTOR],KEY_FRAME_HEADERS[IS_KEYFRAME_SELECTOR],KEY_FRAME_HEADERS[KEYFRAME_CAPTION_SELECTOR]]

video_id = "qN8DRJ8OMcA"
kf_url = "https://dev.youdescribe.org/keyframes/"+video_id+"/"
URL = "http://localhost:5001/keyframe/addKeyframe" # API to store kf info in youdescribex db

for index, row in df.iterrows():
    if "<unk>" in row['Caption']: 
#         print(row['Caption'])
        continue
    if row['Is Keyframe']:
        kf_num = row[KEY_FRAME_HEADERS[FRAME_INDEX_SELECTOR]]
        kf_id = video_id + "_" + str(kf_num)
        
        body = {
            "keyframeId":kf_id,
            "videoId": video_id,
            "keyframeNum": int(kf_num),
            "keyframeURL": kf_url + "frame_" + str(kf_num) + ".jpg",
            "timestamp": row[KEY_FRAME_HEADERS[TIMESTAMP_SELECTOR]],
            "caption": row[KEY_FRAME_HEADERS[KEYFRAME_CAPTION_SELECTOR]]
        }
#         print(body)
        r = requests.post(url = URL, data=body)
        print(r)