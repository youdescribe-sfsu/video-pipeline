'''
version: Python3.7
'''

# Inserting the extracted keyframes in db (replace csv file path and video id)
import requests
import pandas as pd


df = pd.read_csv("./qN8DRJ8OMcA_data.csv")
df = df[['Frame Index', 'Timestamp', 'Is Keyframe', 'Caption']]

video_id = "qN8DRJ8OMcA"
kf_url = "https://dev.youdescribe.org/keyframes/"+video_id+"/"
URL = "http://localhost:5001/keyframe/addKeyframe" # API to store kf info in youdescribex db

for index, row in df.iterrows():
    if "<unk>" in row['Caption']: 
#         print(row['Caption'])
        continue
    if row['Is Keyframe']:
        kf_num = row["Frame Index"]
        kf_id = video_id + "_" + str(kf_num)
        
        body = {
            "keyframeId":kf_id,
            "videoId": video_id,
            "keyframeNum": int(kf_num),
            "keyframeURL": kf_url + "frame_" + str(kf_num) + ".jpg",
            "timestamp": row["Timestamp"],
            "caption": row["Caption"]
        }
#         print(body)
        r = requests.post(url = URL, data=body)
        print(r)