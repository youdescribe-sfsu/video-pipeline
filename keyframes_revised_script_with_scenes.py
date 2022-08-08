# Inserting the extracted keyframes in db (replace csv file path and video id)
import requests
import pandas as pd
import json
import sys

def fetchSceneData(video_id):

    # Fetching scene data of video for scene_ids
    scene_data_URL = "http://localhost:5001/videoSceneData?videoid=" + video_id
    r = requests.get(url = scene_data_URL)
    scene_arr = r.text
    scene_arr = json.loads(scene_arr)
    scene_arr.sort(key=lambda x:x['scene_num'])
    return scene_arr

def fetchSceneId(timestamp, scene_arr):
    
    #TODO - change the below algorithm with binary search to improve time complexity
    j=0
    while j<len(scene_arr) and scene_arr[j]['start_time'] <= timestamp: j+=1
        
    return scene_arr[j-1]['scene_id']


def insert_key_frames(video_id) :
    df = pd.read_csv("./Captions.csv")
    df = df[['Frame Index', 'Timestamp', 'Is Keyframe', 'Caption']]

    kf_url = "https://dev.youdescribe.org/keyframes/"+video_id+"/"
    URL = "http://localhost:5001/keyframe/addKeyframe" # API to store kf info in youdescribex db
    #scene_arr = fetchSceneData(video_id)
   
    for index, row in df.iterrows():
        try:
            if "<unk>" in row['Caption']: 
    #             print(row['Caption'])
                continue
        except:
            continue
        if row['Is Keyframe']:
            kf_num = row["Frame Index"]
            kf_id = video_id + "_" + str(kf_num)
            timestamp = row["Timestamp"]
            #scene_id = fetchSceneId(timestamp, scene_arr)
            
            body = {
                "keyframeId":kf_id,
                "videoId": video_id,
                "keyframeNum": int(kf_num),
                "keyframeURL": kf_url + "frame_" + str(kf_num) + ".jpg",
                "timestamp": timestamp,
                "caption": row["Caption"],
                #"sceneId": scene_id
            }
    #         print(body)
            r = requests.post(url = URL, data=body)
            print(r)