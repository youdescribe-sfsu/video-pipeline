import json
import sys
import requests
import yt_dlp as ydl
from utils import returnVideoFolderName,SUMMARIZED_SCENES,OCR_FILTER_REMOVE_SIMILAR,TRANSCRIPTS,DIALOGS,OCR_HEADERS,TIMESTAMP_SELECTOR,OCR_TEXT_SELECTOR
import os
import csv
import string

def transformStringAndCheckIfEmpty(row_text):
    text_len = len(row_text)
    if(len(row_text) > 1 or len(row_text.split(" ")) > 1):
        if(row_text[0] == "\n"):
            row_text[0] == ""
        if(row_text[text_len - 1] == "\n"):
            row_text[text_len - 1] == ""
        normal_string=row_text.translate(str.maketrans('', '', string.punctuation))
        to_insert = len(normal_string.split(" ")) > 1
        return (to_insert,row_text)
    else:
        return (False,'')

def upload_data(videoId):
    vid = ydl.YoutubeDL().extract_info(
        url='https://www.youtube.com/watch?v=' + videoId, download=False)
    dialogue_timestamps = []
    sequence_num = 0

    f = open(returnVideoFolderName(videoId)+'/'+TRANSCRIPTS)
    # print(f)
    dialogue = json.load(f)
    f.close()
    for i in dialogue["results"]:
        key_array = i.keys()
        if("alternatives" in key_array and "resultEndTime" in key_array):
            clip = {}
            clip["sequence_num"] = sequence_num
            clip["start_time"] = round(float(i["alternatives"][0]['words'][0]["startTime"][:-1]),2)
            clip["end_time"] = round(float(i["resultEndTime"][:-1]),2)
            clip["duration"] = round(float(clip["end_time"]) - float(clip["start_time"]),2)
            dialogue_timestamps.append(clip)
            sequence_num += 1
    # print(dialogue_timestamps)
    f = open(returnVideoFolderName(videoId)+'/'+SUMMARIZED_SCENES)
    scene_data = json.load(f)
    f.close()
    audio_clips = []
    scene = 1
    for i in scene_data:
        i["type"] = "Visual"

        if i["scene_number"] == scene:
            audio_clips.append(i)
            scene += 1
            
    with open(returnVideoFolderName(videoId)+'/'+OCR_FILTER_REMOVE_SIMILAR) as file:
        entry = {}
        csvReader = csv.DictReader(file) 
        for row in csvReader:
            if(len(row)==3):
                if(len(entry.keys()) != 0):
                    audio_clips.append(entry)
                    entry = {}
                row_text = row[OCR_HEADERS[OCR_TEXT_SELECTOR]]
                # Remove Special Characters and check if empty
                to_insert,text_to_insert = transformStringAndCheckIfEmpty(row_text)
                if(to_insert):
                    entry = {
                        "start_time": row[OCR_HEADERS[TIMESTAMP_SELECTOR]],
                        "text": text_to_insert,
                        "type": "Text on Screen"
                    }
            else:
                entry["text"] += row[OCR_HEADERS[TIMESTAMP_SELECTOR]]

    for clip in audio_clips:
        try:
            clip['start_time'] = str(float(clip['start_time']) + 1)
            if(isinstance(clip["text"], list)):
                clip["text"] = ("\n").join(clip["text"])
            else:
                clip["text"].replace('\n', '.')
        except:
            continue
    aiUserId = os.getenv('YDX_AI_USER_ID')

    data = {
        "youtube_id": videoId,
        "audio_clips": audio_clips,
        "video_length": vid["duration"],
        "video_name": vid["title"],
        "dialogue_timestamps": dialogue_timestamps,
        # AI USER ID
        "aiUserId": aiUserId
    }
    print("===== UPLOADING DATA =====")
    # print(data)
    with open(returnVideoFolderName(videoId)+'/'+DIALOGS, mode='w') as f:
        f.write(json.dumps(dialogue_timestamps))
    with open(returnVideoFolderName(videoId)+'/'+"final_data.json", mode='w') as f:
        f.write(json.dumps(data, indent=4))
    print("===== UPLOADING DATA =====")
    # send data to wherever db is
    # ydx_server = os.getenv('YDX_WEB_SERVER')
    # if(ydx_server == None):
    #     ydx_server = 'http://3.101.130.10:4000'
    # url = '{}/api/audio-descriptions/newaidescription/'.format(ydx_server)
    # headers = {"Content-Type": "application/json; charset=utf-8"}
    # try:
    #     r = requests.post(url, data=json.dumps(data), headers=headers)
    #     print("===== RESPONSE =====")
    #     print(r.text)
    #     r.close()
    # except:
    #     r = requests.post(url, data=json.dumps(data), headers=headers)
    #     print(r.text)
    #     r.close()


def generateYDXCaption(videoId):
    userId = os.getenv('YDX_USER_ID')
    aiUserId = os.getenv('YDX_AI_USER_ID')
    if(userId == None):
      userId = "65c433f7-ceb2-495d-ae01-994388ce56f5"
    data = {
      "userId" : userId,
      "youtubeVideoId" : videoId,
      # Change AI ID to the ID of the AI you want to use
      "aiUserId": aiUserId
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
    upload_data(sys.argv[1])
