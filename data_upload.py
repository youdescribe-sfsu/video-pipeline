import json
import sys
import numpy as np
import requests
import yt_dlp as ydl
from utils import returnVideoFolderName,SUMMARIZED_SCENES,OCR_FILTER_REMOVE_SIMILAR,TRANSCRIPTS,DIALOGS
import os
from dotenv import load_dotenv
load_dotenv()



def mergeIntervals(audio_clips):
    # Sort the array on the basis of start values of intervals.
    stack = []
    # insert first interval into stack
    stack.append(audio_clips[0])
    for audio_clip in audio_clips[1:]:
        # Check for overlapping interval,
        # if interval overlap
        if abs(float(audio_clip["start_time"]) - float(stack[-1]["start_time"])) < 5:
            stack[-1]['text'] += ' \n ' + audio_clip['text']
        else:
            stack.append(audio_clip)
    return stack

def upload_data(videoId):
    vid = ydl.YoutubeDL().extract_info(
        url='https://www.youtube.com/watch?v=' + videoId, download=False)
    dialogue_timestamps = []
    sequence_num = 0

    f = open(returnVideoFolderName(videoId)+'/'+TRANSCRIPTS)
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
        lines = file.readlines()[1:]

        entry = {}
        for line in lines:
            split = line.split(",")
            if len(split) == 3:
                if len(entry.keys()) != 0:
                    audio_clips.append(entry)
                entry = {
                    "start_time": split[1],
                    "text": split[2],
                    "type": "Text on Screen"
                }

            else:
                entry["text"] += split[0]

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
    audio_clips.sort(key=lambda x: float(x['start_time']))
    # for audio_clip in audio_clips:
    #     audio_clip['text'] = audio_clip['text'].replace('\n', ' and ')
    
    visual_audio_clips = list(filter(lambda x: x['type'] == 'Visual', audio_clips))
    
    text_on_screen_audio_clips = list(filter(lambda x: x['type'] == 'Text on Screen', audio_clips))
    text_on_screen_audio_clips = mergeIntervals(text_on_screen_audio_clips)
    
    visual_audio_clips.extend(text_on_screen_audio_clips)
    audio_clips = visual_audio_clips
    audio_clips.sort(key=lambda x: float(x['start_time']))

    data = {
        "youtube_id": videoId,
        "audio_clips": audio_clips,
        "video_length": vid["duration"],
        "video_name": vid["title"],
        "dialogue_timestamps": dialogue_timestamps,
        # AI USER ID
        "aiUserId": aiUserId
    }
    # print(data)

    f = open(returnVideoFolderName(videoId)+'/'+DIALOGS, mode='w')
    f.writelines(str(dialogue_timestamps))
    f.close()
    with open(returnVideoFolderName(videoId)+'/'+"final_data.json", mode='w') as f:
        f.write(json.dumps(data))
    f = open(returnVideoFolderName(videoId)+'/'+DIALOGS, mode='w')
    f.writelines(str(dialogue_timestamps))
    # send data to wherever db is
    # ydx_server = os.getenv('YDX_WEB_SERVER')
    # if(ydx_server == None):
    #     ydx_server = 'http://3.101.130.10:4000'
    # url = '{}/api/audio-descriptions/newaidescription/'.format(ydx_server)
    # headers = {"Content-Type": "application/json; charset=utf-8"}
    # try:
    #     r = requests.post(url, data=json.dumps(data), headers=headers)
    #     print("===== RESPONSE =====")
    #     print(r)
    #     r.close()
    # except:
    #     r = requests.post(url, data=json.dumps(data), headers=headers)
    #     print(r)
    #     r.close()

if __name__ == '__main__':
    upload_data('ll8cTIg2rwM')
