import json
import sys
import numpy as np
import requests
import yt_dlp as ydl


def upload_data(videoId):
    vid = ydl.YoutubeDL().extract_info(
        url='https://www.youtube.com/watch?v=' + videoId, download=False)
    dialogue_timestamps = []
    sequence_num = 0

    # f = open("transcripts.json")
    # dialogue = json.load(f)
    # f.close()
    # for i in dialogue["results"]:
    #     clip = {}
    #     clip["sequence_num"] = sequence_num
    #     clip["start_time"] = i["alternatives"][0]['words'][0]["startTime"][:-1]
    #     clip["end_time"] = i["resultEndTime"][:-1]
    #     clip["duration"] = float(clip["end_time"]) - float(clip["start_time"])
    #     dialogue_timestamps.append(clip)
    #     sequence_num += 1

    f = open("summarized_scenes.json")
    scene_data = json.load(f)
    f.close()
    audio_clips = []
    scene = 1
    for i in scene_data:
        i["type"] = "Visual"

        if i["scene_number"] == scene:
            audio_clips.append(i)
            scene += 1

    with open("OCR Filter Remove Sim.csv") as file:
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
        if(isinstance(clip["text"], list)):
            clip["text"] = ("\n").join(clip["text"])

    print(audio_clips)

    data = {
        "youtube_id": videoId,
        "audio_clips": audio_clips,
        "video_length": vid["duration"],
        "video_name": vid["title"],
        "dialogue_timestamps": dialogue_timestamps
    }

    f = open("dialogs", mode='w')
    f.writelines(str(dialogue_timestamps))
    # send data to wherever db is
    #url = 'http://localhost:3000/api/audio-descriptions/newaidescription/'
    headers = {"Content-Type": "application/json; charset=utf-8"}
    x = requests.post(url, data=json.dumps(data), headers=headers)

    print(x.text)


if __name__ == '__main__':
    upload_data(sys.argv[1])