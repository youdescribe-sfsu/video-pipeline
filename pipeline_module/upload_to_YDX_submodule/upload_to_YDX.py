import json
import requests
import os
import csv
import string
import traceback
from ..utils_module.utils import (
    return_video_folder_name,
    SUMMARIZED_SCENES,
    OCR_FILTER_REMOVE_SIMILAR,
    TRANSCRIPTS,
    DIALOGS,
    OCR_HEADERS,
    TIMESTAMP_SELECTOR,
    OCR_TEXT_SELECTOR,
)
from web_server_module.web_server_database import get_status_for_youtube_id, update_status, update_module_output, \
    return_all_user_data_for_youtube_id_ai_user_id


class UploadToYDX:
    def __init__(self, video_runner_obj, upload_to_server=False):
        self.video_runner_obj = video_runner_obj
        self.upload_to_server = upload_to_server
        self.logger = video_runner_obj.get("logger")

    def mergeIntervals(self, audio_clips):
        stack = []
        if len(audio_clips) == 0:
            return []
        stack.append(audio_clips[0])
        for audio_clip in audio_clips[1:]:
            if abs(float(audio_clip["start_time"]) - float(stack[-1]["start_time"])) < 5:
                stack[-1]["text"] += " \n " + audio_clip["text"]
            else:
                stack.append(audio_clip)
        return stack

    def transformStringAndCheckIfEmpty(self, row_text):
        text_len = len(row_text)
        if len(row_text) > 1 or len(row_text.split(" ")) > 1:
            if row_text[0] == "\n":
                row_text = row_text[1:]
            if row_text[-1] == "\n":
                row_text = row_text[:-1]
            normal_string = row_text.translate(str.maketrans("", "", string.punctuation))
            to_insert = len(normal_string.split(" ")) > 1
            return (to_insert, row_text)
        else:
            return (False, "")

    def upload_to_ydx(self, ydx_server, AI_USER_ID):
        # Check if upload is already done via the database
        if get_status_for_youtube_id(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"]) == 'done':
            self.logger.info("Already uploaded to YDX")
            return

        self.logger.info("Uploading to YDX")

        try:
            dialogue_timestamps = self.prepare_dialogue_timestamps()
            audio_clips = self.prepare_audio_clips()
            metadata = self.load_metadata()

            data = self.prepare_upload_data(dialogue_timestamps, audio_clips, metadata, AI_USER_ID)

            self.logger.info("===== UPLOADING DATA =====")
            self.logger.info(data)

            self.save_data_to_files(dialogue_timestamps, data)

            if self.upload_to_server:
                self.send_data_to_server(data, ydx_server, AI_USER_ID)

            # Mark the upload as done in the database
            update_status(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"], 'done')

            # Store upload details in the database for future reference
            update_module_output(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"], 'upload_to_YDX', {"upload_data": data})

        except Exception as e:
            self.logger.error(f"Error in upload_to_ydx: {str(e)}")
            self.logger.error(traceback.format_exc())
            self.notify_error(ydx_server, str(e))
            raise

    def prepare_dialogue_timestamps(self):
        dialogue_timestamps = []
        sequence_num = 0

        with open(return_video_folder_name(self.video_runner_obj) + "/" + TRANSCRIPTS) as f:
            dialogue = json.load(f)

        for i in dialogue["results"]:
            key_array = i.keys()
            if "alternatives" in key_array and "resultEndTime" in key_array:
                clip = {
                    "sequence_num": sequence_num,
                    "start_time": round(float(i["alternatives"][0]["words"][0]["startTime"][:-1]), 2),
                    "end_time": round(float(i["resultEndTime"][:-1]), 2),
                    "duration": round(
                        float(i["resultEndTime"][:-1]) - float(i["alternatives"][0]["words"][0]["startTime"][:-1]), 2)
                }
                dialogue_timestamps.append(clip)
                sequence_num += 1

        return dialogue_timestamps

    def prepare_audio_clips(self):
        audio_clips = []
        scene = 1

        # Load scene data
        with open(return_video_folder_name(self.video_runner_obj) + "/" + SUMMARIZED_SCENES) as f:
            scene_data = json.load(f)

        for i in scene_data:
            i["type"] = "Visual"
            audio_clips.append(i)
            scene += 1

        # Load OCR data
        with open(return_video_folder_name(self.video_runner_obj) + "/" + OCR_FILTER_REMOVE_SIMILAR) as file:
            csvReader = csv.DictReader(file)
            entry = {}
            for row in csvReader:
                if len(row) == 3:
                    if len(entry.keys()) != 0:
                        audio_clips.append(entry)
                        entry = {}
                    row_text = row[OCR_HEADERS[OCR_TEXT_SELECTOR]]
                    to_insert, text_to_insert = self.transformStringAndCheckIfEmpty(row_text)
                    if to_insert:
                        entry = {
                            "start_time": row[OCR_HEADERS[TIMESTAMP_SELECTOR]],
                            "text": text_to_insert,
                            "type": "Text on Screen",
                        }
                else:
                    entry["text"] += row[OCR_HEADERS[TIMESTAMP_SELECTOR]]

        # Process audio clips
        for clip in audio_clips:
            clip["start_time"] = str(float(clip["start_time"]) + 1)
            if isinstance(clip["text"], list):
                clip["text"] = ". ".join(clip["text"])
            else:
                clip["text"] = clip["text"].replace("\n", ".")

        # Sort and merge audio clips
        audio_clips.sort(key=lambda x: float(x["start_time"]))
        visual_audio_clips = [clip for clip in audio_clips if clip["type"] == "Visual"]
        text_on_screen_audio_clips = [clip for clip in audio_clips if clip["type"] == "Text on Screen"]
        text_on_screen_audio_clips = self.mergeIntervals(text_on_screen_audio_clips)
        audio_clips = visual_audio_clips + text_on_screen_audio_clips
        audio_clips.sort(key=lambda x: float(x["start_time"]))

        return audio_clips

    def load_metadata(self):
        with open(return_video_folder_name(self.video_runner_obj) + "/metadata.json", "r") as f:
            return json.load(f)

    def prepare_upload_data(self, dialogue_timestamps, audio_clips, metadata, AI_USER_ID):
        return {
            "youtube_id": self.video_runner_obj['video_id'],
            "audio_clips": audio_clips,
            "video_length": metadata["duration"],
            "video_name": metadata["title"],
            "dialogue_timestamps": dialogue_timestamps,
            "aiUserId": AI_USER_ID,
        }

    def save_data_to_files(self, dialogue_timestamps, data):
        with open(return_video_folder_name(self.video_runner_obj) + "/" + DIALOGS, mode="w") as f:
            f.write(json.dumps(dialogue_timestamps))
        with open(return_video_folder_name(self.video_runner_obj) + "/" + "final_data.json", mode="w") as f:
            f.write(json.dumps(data, indent=4))

    def send_data_to_server(self, data, ydx_server, AI_USER_ID):
        ydx_server = ydx_server or os.getenv("YDX_WEB_SERVER")
        url = f"{ydx_server}/api/audio-descriptions/newaidescription/"
        headers = {"Content-Type": "application/json; charset=utf-8"}

        self.logger.info(f"===== UPLOADING DATA to {url} =====")

        try:
            r = requests.post(url, data=json.dumps(data), headers=headers)
            r.raise_for_status()  # Raises an HTTPError for bad responses

            json_response = r.json()
            self.logger.info("json_response", json_response)

            if json_response.get('_id'):
                self.process_successful_upload(json_response, ydx_server, AI_USER_ID)

        except requests.RequestException as e:
            self.logger.error(f"Error during request: {str(e)}")
            raise

    def process_successful_upload(self, json_response, ydx_server, AI_USER_ID):
        self.logger.info("===== RESPONSE =====")
        self.logger.info(json_response)

        generateAudioClips = f"{ydx_server}/api/audio-clips/processAllClipsInDB/{json_response['_id']}"
        r = requests.get(generateAudioClips)

        if r.status_code == 200:
            self.logger.info("Processed all clips in DB")
            self.logger.info(r.text)
            self.notify_users(ydx_server, AI_USER_ID, json_response['_id'])

    def notify_users(self, ydx_server, AI_USER_ID, audio_description_id):
        data = return_all_user_data_for_youtube_id_ai_user_id(
            ai_user_id=AI_USER_ID,
            youtube_id=self.video_runner_obj['video_id']
        )

        if not data:
            self.logger.info("No data found")
            return

        post_obj = {
            "youtube_id": self.video_runner_obj['video_id'],
            "ai_user_id": AI_USER_ID,
            "ydx_app_host": data[0]['ydx_app_host'],
            "audio_description_id": audio_description_id,
            "user_ids": [userData['user_id'] for userData in data]
        }

        notifyEmails = f"{ydx_server}/api/utils/notify/aidescriptions"

        r = requests.post(notifyEmails, data=json.dumps(post_obj), headers={"Content-Type": "application/json"})

        if r.status_code == 200:
            self.logger.info("Notified emails")
            self.logger.info(r.text)
        else:
            self.logger.error("Error notifying emails")
            self.logger.error(r.text)

    def notify_error(self, ydx_server, error_message):
        notifyForError = f"{ydx_server}/api/utils/notify"
        post_obj = {
            "email": "smirani1@mail.sfsu.edu",
            "subject": "Error in generating YDX Caption",
            "message": error_message
        }
        try:
            r = requests.post(notifyForError, data=json.dumps(post_obj), headers={"Content-Type": "application/json"})
            if r.status_code == 200:
                self.logger.info("Notified about error")
                self.logger.info(r.text)
            else:
                self.logger.error("Error notifying about error")
                self.logger.error(r.text)
        except requests.RequestException as e:
            self.logger.error(f"Failed to send error notification: {str(e)}")