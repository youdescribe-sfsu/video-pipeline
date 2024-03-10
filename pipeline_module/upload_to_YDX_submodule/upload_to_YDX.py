import json
import requests
from ..utils_module.utils import (
    # load_progress_from_file,
    read_value_from_file,
    return_video_folder_name,
    SUMMARIZED_SCENES,
    OCR_FILTER_REMOVE_SIMILAR,
    TRANSCRIPTS,
    DIALOGS,
    OCR_HEADERS,
    TIMESTAMP_SELECTOR,
    OCR_TEXT_SELECTOR,
    save_value_to_file,
    # save_progress_to_file,
)
import os
import csv
import string
from web_server_module.web_server_database import return_all_user_data_for_youtube_id_ai_user_id
from typing import Deque, List, Optional, Tuple

from types_submodule import AudioClip,UploadToYDXData


class UploadToYDX:
    def __init__(self, video_runner_obj, upload_to_server: bool = False):
        self.video_runner_obj = video_runner_obj
        self.upload_to_server = upload_to_server

    def merge_intervals(self, audio_clips: List[AudioClip]) -> List[AudioClip]:
        stack = []
        if not audio_clips:
            return []
        stack.append(audio_clips[0])
        for audio_clip in audio_clips[1:]:
            if abs(float(audio_clip.start_time) - float(stack[-1].start_time)) < 5:
                stack[-1].text += " \n " + audio_clip.text
            else:
                stack.append(audio_clip)
        return stack

    def transform_string_and_check_if_empty(self, row_text: str) -> Tuple[bool, str]:
        text_len = len(row_text)
        if len(row_text) > 1 or len(row_text.split(" ")) > 1:
            if row_text[0] == "\n":
                row_text[0] == ""
            if row_text[text_len - 1] == "\n":
                row_text[text_len - 1] == ""
            normal_string = row_text.translate(str.maketrans("", "", string.punctuation))
            to_insert = len(normal_string.split(" ")) > 1
            return to_insert, row_text
        else:
            return False, ""

    def upload_to_ydx(self, ydx_server: Optional[str] = None, AI_USER_ID: Optional[str] = None):
        if read_value_from_file(video_runner_obj=self.video_runner_obj, key="['UploadToYDX']['started']") == 'done':
            self.video_runner_obj["logger"].info("Already uploaded to YDX")
            return

        self.video_runner_obj["logger"].info("Uploading to YDX")
        dialogue_timestamps = []
        sequence_num = 0

        with open(return_video_folder_name(self.video_runner_obj) + "/" + TRANSCRIPTS) as f:
            dialogue = json.load(f)

        for i in dialogue["results"]:
            key_array = i.keys()
            if "alternatives" in key_array and "resultEndTime" in key_array:
                clip = AudioClip(
                    sequence_num=sequence_num,
                    start_time=round(float(i["alternatives"][0]["words"][0]["startTime"][:-1]), 2),
                    end_time=round(float(i["resultEndTime"][:-1]), 2),
                    duration=round(float(i["resultEndTime"][:-1]) - float(i["alternatives"][0]["words"][0]["startTime"][:-1]), 2)
                )
                dialogue_timestamps.append(clip)
                sequence_num += 1

        with open(return_video_folder_name(self.video_runner_obj) + "/" + SUMMARIZED_SCENES) as f:
            scene_data = json.load(f)

        audio_clips = [AudioClip(**i, type="Visual") for i in scene_data]
        audio_clips.append(AudioClip(**i, type="Text on Screen") for i in scene_data)

        with open(return_video_folder_name(self.video_runner_obj) + "/" + OCR_FILTER_REMOVE_SIMILAR) as file:
            entry = {}
            csvReader = csv.DictReader(file)
            for row in csvReader:
                if len(row) == 3:
                    if len(entry.keys()) != 0:
                        audio_clips.append(entry)
                        entry = {}

                    row_text = row[OCR_HEADERS[OCR_TEXT_SELECTOR]]
                    to_insert, text_to_insert = self.transform_string_and_check_if_empty(row_text)
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
                clip.start_time = str(float(clip.start_time) + 1)
                if isinstance(clip.text, list):
                    clip.text = ". ".join(clip.text)
                else:
                    clip.text.replace("\n", ".")
            except:
                continue

        audio_clips.sort(key=lambda x: float(x.start_time))

        visual_audio_clips = [clip for clip in audio_clips if clip.type == "Visual"]
        text_on_screen_audio_clips = [clip for clip in audio_clips if clip.type == "Text on Screen"]
        text_on_screen_audio_clips = self.merge_intervals(text_on_screen_audio_clips)
        visual_audio_clips.extend(text_on_screen_audio_clips)
        audio_clips = visual_audio_clips
        audio_clips.sort(key=lambda x: float(x.start_time))

        metadata = {}

        with open(return_video_folder_name(self.video_runner_obj) + "/metadata.json", "r") as f:
            metadata = json.load(f)

        data = UploadToYDXData(
            youtube_id=self.video_runner_obj['video_id'],
            audio_clips=audio_clips,
            video_length=metadata["duration"],
            video_name=metadata["title"],
            dialogue_timestamps=dialogue_timestamps,
            aiUserId=AI_USER_ID,
        )

        self.video_runner_obj["logger"].info("===== UPLOADING DATA =====")
        self.video_runner_obj["logger"].info(data.model_dump())

        with open(return_video_folder_name(self.video_runner_obj) + "/" + DIALOGS, mode="w") as f:
            f.write(json.dumps(dialogue_timestamps))
        with open(
            return_video_folder_name(self.video_runner_obj) + "/" + "final_data.json", mode="w"
        ) as f:
            f.write(json.dumps(data, indent=4))
        if self.upload_to_server:
            print("===== UPLOADING DATA =====")
            # send data to wherever the database is

            ydx_server = self.video_runner_obj.get('ydx_server')
            
            if ydx_server is None:
                ydx_server = os.getenv("YDX_WEB_SERVER")
            
            url = "{}/api/audio-descriptions/newaidescription/".format("https://ydx-dev-api.youdescribe.org")
            headers = {"Content-Type": "application/json; charset=utf-8"}
            
            self.video_runner_obj["logger"].info("===== UPLOADING DATA to {} =====".format(url))
            
            try:
                r = requests.post(url, data=json.dumps(data), headers=headers)
                # self.video_runner_obj["logger"].info("===== RESPONSE =====")
                # self.video_runner_obj["logger"].info(r.text)
                print("response")
                print(r.status_code)
                print(r.json())
                json_response = json.loads(r.text)
                self.video_runner_obj["logger"].info("json_response",json_response)
                print(json_response)
                r.close()
                if(r.status_code != 500 and json_response['_id']):
                    self.video_runner_obj["logger"].info("===== RESPONSE =====")
                    self.video_runner_obj["logger"].info(json_response)
                    ## Get req
                    generateAudioClips = "{}/api/audio-clips/processAllClipsInDB/{}".format("https://ydx-dev-api.youdescribe.org",json_response['_id'])
                    print("========= generateAudioClips =======")
                    print(generateAudioClips)
                    r = requests.get(generateAudioClips)
                    print("========= generateAudioClips response=======")
                    print(r.status_code)
                    print(r.text)
                    if(r.status_code == 200):
                        self.video_runner_obj["logger"].info("Processed all clips in DB")
                        self.video_runner_obj["logger"].info(r.text)
                        data = return_all_user_data_for_youtube_id_ai_user_id(
            ai_user_id=AI_USER_ID,
            youtube_id=self.video_runner_obj['video_id']
                        )
                        if(len(data) == 0):
                            self.video_runner_obj["logger"].info("No data found")
                            return
                            # exit()
                        post_obj = {
                            "youtube_id": self.video_runner_obj['video_id'],
                            "ai_user_id": AI_USER_ID,
                            "ydx_app_host":data[0]['ydx_app_host'],
                            "audio_description_id":json_response['_id']
                        }
                        user_ids = []
                        for userData in data:
                            user_ids.append(userData['user_id'])
                            
                        
                        post_obj['user_ids'] = user_ids
                        notifyEmails = "{}/api/utils/notify/aidescriptions".format(ydx_server)
                        
                        ## post request to notify emails
                        r = requests.post(notifyEmails, data=json.dumps(post_obj), headers=headers)
                        if(r.status_code == 200):
                            self.video_runner_obj["logger"].info("Notified emails")
                            self.video_runner_obj["logger"].info(r.text)
                        else:
                            self.video_runner_obj["logger"].error("Error notifying emails")
                            self.video_runner_obj["logger"].error(r.text)
                        
                    r.close()
                    self.video_runner_obj["logger"].info("===== RESPONSE =====")
                    self.video_runner_obj["logger"].info(r.text)
                    
                    
                
                
                self.video_runner_obj["logger"].info(r.status_code)
                self.video_runner_obj["logger"].info("===== RESPONSE =====")
                self.video_runner_obj["logger"].info(r.text)
                
                
                # Save the completion status only if the request was successful
                save_value_to_file(video_runner_obj=self.video_runner_obj, key="['UploadToYDX']['started']", value='done')
            except Exception as e:
                print("Error during request:", str(e))
                self.video_runner_obj["logger"].error("Error during request: %s", str(e))
                notifyForError = "{}/api/utils/notify".format(ydx_server)
                post_obj = {
                    "email": "vishalsharma1907@gmail.com",
                    "subject": "Error in generating YDX Caption",
                    "message": str(e)
                }
                r = requests.post(notifyForError, data=json.dumps(post_obj), headers=headers)
                if(r.status_code == 200):
                    self.video_runner_obj["logger"].info("Notified emails")
                    self.video_runner_obj["logger"].info(r.text)
                else:
                    self.video_runner_obj["logger"].error("Error notifying emails")
                    self.video_runner_obj["logger"].error(r.text)
            # You may want to handle the exception or log the error as needed

        return
