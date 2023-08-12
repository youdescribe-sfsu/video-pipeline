import json
import requests
from utils import (
    load_progress_from_file,
    return_video_folder_name,
    SUMMARIZED_SCENES,
    OCR_FILTER_REMOVE_SIMILAR,
    TRANSCRIPTS,
    DIALOGS,
    OCR_HEADERS,
    TIMESTAMP_SELECTOR,
    OCR_TEXT_SELECTOR,
    save_progress_to_file,
)
import os
import csv
import string


class UploadToYDX:
    def __init__(self, video_runner_obj, upload_to_server=True):
        self.video_runner_obj = video_runner_obj
        self.upload_to_server = upload_to_server

    def mergeIntervals(self,audio_clips):
        # Sort the array on the basis of start values of intervals.
        stack = []
        # insert first interval into stack
        if(len(audio_clips) == 0):
            return []
        stack.append(audio_clips[0])
        for audio_clip in audio_clips[1:]:
            # Check for overlapping interval,
            # if interval overlap
            if (
                abs(float(audio_clip["start_time"]) - float(stack[-1]["start_time"]))
                < 5
            ):
                stack[-1]["text"] += " \n " + audio_clip["text"]
            else:
                stack.append(audio_clip)
        return stack

    def transformStringAndCheckIfEmpty(self,row_text):
        text_len = len(row_text)
        if len(row_text) > 1 or len(row_text.split(" ")) > 1:
            if row_text[0] == "\n":
                row_text[0] == ""
            if row_text[text_len - 1] == "\n":
                row_text[text_len - 1] == ""
            normal_string = row_text.translate(
                str.maketrans("", "", string.punctuation)
            )
            to_insert = len(normal_string.split(" ")) > 1
            return (to_insert, row_text)
        else:
            return (False, "")

    def upload_to_ydx(self):
        save_file = load_progress_from_file(video_runner_obj=self.video_runner_obj)
        if(save_file["UploadToYDX"]['started'] == 'done'):
            ## Already uploaded to YDX
            self.video_runner_obj["logger"].info("Already uploaded to YDX")
            return
        self.video_runner_obj["logger"].info("Uploading to YDX")
        dialogue_timestamps = []
        sequence_num = 0

        f = open(return_video_folder_name(self.video_runner_obj) + "/" + TRANSCRIPTS)
        # print(f)
        dialogue = json.load(f)
        f.close()
        for i in dialogue["results"]:
            key_array = i.keys()
            if "alternatives" in key_array and "resultEndTime" in key_array:
                clip = {}
                clip["sequence_num"] = sequence_num
                clip["start_time"] = round(
                    float(i["alternatives"][0]["words"][0]["startTime"][:-1]), 2
                )
                clip["end_time"] = round(float(i["resultEndTime"][:-1]), 2)
                clip["duration"] = round(
                    float(clip["end_time"]) - float(clip["start_time"]), 2
                )
                dialogue_timestamps.append(clip)
                sequence_num += 1
        # print(dialogue_timestamps)
        f = open(return_video_folder_name(self.video_runner_obj) + "/" + SUMMARIZED_SCENES)
        scene_data = json.load(f)
        f.close()
        audio_clips = []
        scene = 1
        for i in scene_data:
            i["type"] = "Visual"

            ##TODO: Workaround for Multiple scenes remove once it's fixed
            audio_clips.append(i)
            scene += 1

        with open(
            return_video_folder_name(self.video_runner_obj) + "/" + OCR_FILTER_REMOVE_SIMILAR
        ) as file:
            entry = {}
            csvReader = csv.DictReader(file)
            for row in csvReader:
                ##TODO: Check the CSV generated is correct len(row) == 3
                ##TODO Remove if not reqd
                if len(row) == 3:
                    if len(entry.keys()) != 0:
                        audio_clips.append(entry)
                        entry = {}
                    row_text = row[OCR_HEADERS[OCR_TEXT_SELECTOR]]
                    # Remove Special Characters and check if empty
                    to_insert, text_to_insert = self.transformStringAndCheckIfEmpty(
                        row_text
                    )
                    if to_insert:
                        entry = {
                            "start_time": row[OCR_HEADERS[TIMESTAMP_SELECTOR]],
                            "text": text_to_insert,
                            "type": "Text on Screen",
                        }
                else:
                    entry["text"] += row[OCR_HEADERS[TIMESTAMP_SELECTOR]]

        for clip in audio_clips:
            try:
                clip["start_time"] = str(float(clip["start_time"]) + 1)
                if isinstance(clip["text"], list):
                    clip["text"] = ("\n").join(clip["text"])
                else:
                    clip["text"].replace("\n", ".")
            except:
                continue
        aiUserId = os.getenv("YDX_AI_USER_ID")
        ##TODO Check and remove this if not required
        audio_clips.sort(key=lambda x: float(x["start_time"]))
        # for audio_clip in audio_clips:
        #     audio_clip['text'] = audio_clip['text'].replace('\n', ' and ')

        visual_audio_clips = list(filter(lambda x: x["type"] == "Visual", audio_clips))

        text_on_screen_audio_clips = list(
            filter(lambda x: x["type"] == "Text on Screen", audio_clips)
        )
        text_on_screen_audio_clips = self.mergeIntervals(text_on_screen_audio_clips)

        visual_audio_clips.extend(text_on_screen_audio_clips)
        audio_clips = visual_audio_clips
        audio_clips.sort(key=lambda x: float(x["start_time"]))

        metadata = {}

        with open(return_video_folder_name(self.video_runner_obj) + "/metadata.json", "r") as f:
            metadata = json.load(f)
        data = {
            "youtube_id": self.video_runner_obj['video_id'],
            "audio_clips": audio_clips,
            "video_length": metadata["duration"],
            "video_name": metadata["title"],
            "dialogue_timestamps": dialogue_timestamps,
            # AI USER ID
            "aiUserId": aiUserId,
        }
        print("===== UPLOADING DATA =====")
        print(data)
        self.video_runner_obj["logger"].info("===== UPLOADING DATA =====")
        self.video_runner_obj["logger"].info(data)
        with open(return_video_folder_name(self.video_runner_obj) + "/" + DIALOGS, mode="w") as f:
            f.write(json.dumps(dialogue_timestamps))
        with open(
            return_video_folder_name(self.video_runner_obj) + "/" + "final_data.json", mode="w"
        ) as f:
            f.write(json.dumps(data, indent=4))
        if(self.upload_to_server):
            print("===== UPLOADING DATA =====")
            # send data to wherever db is
            ydx_server = os.getenv("YDX_WEB_SERVER")
            if ydx_server == None:
                ydx_server = "http://3.101.130.10:4000"
            url = "{}/api/audio-descriptions/newaidescription/".format(ydx_server)
            headers = {"Content-Type": "application/json; charset=utf-8"}
            self.video_runner_obj["logger"].info("===== UPLOADING DATA to {} =====".format(url))
            try:
                r = requests.post(url, data=json.dumps(data), headers=headers)
                print("===== RESPONSE =====")
                print(r.text)
                self.video_runner_obj["logger"].info("===== RESPONSE =====")
                self.video_runner_obj["logger"].info(r.text)
                r.close()
            except:
                r = requests.post(url, data=json.dumps(data), headers=headers)
                self.video_runner_obj["logger"].info("===== RESPONSE =====")
                self.video_runner_obj["logger"].info(r.text)
                print(r.text)
                r.close()
        save_file["UploadToYDX"]['started'] = 'done'
        save_progress_to_file(video_runner_obj=self.video_runner_obj, progress_data=save_file)
        return
