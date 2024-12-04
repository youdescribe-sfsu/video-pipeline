import base64
import csv
import os
import json
import requests
import traceback
from typing import Dict, Any, List
from web_server_module.web_server_database import update_status, update_module_output, get_module_output
from ..utils_module.timeit_decorator import timeit
from ..utils_module.utils import (
    CAPTIONS_CSV, FRAME_INDEX_SELECTOR, IS_KEYFRAME_SELECTOR,
    KEY_FRAME_HEADERS, KEYFRAME_CAPTION_SELECTOR, KEYFRAMES_CSV,
    TIMESTAMP_SELECTOR, return_video_folder_name,
    return_video_frames_folder, CAPTION_IMAGE_PAIR
)
from .service_agents import ServiceAgents

import vertexai
from vertexai.generative_models import GenerativeModel, Part
from PIL import Image
import cv2
import io

class GptCaptioning:
    def __init__(self, video_runner_obj: Dict[str, Any]):
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        self.service_agents = ServiceAgents()  # Initialize service agents


        # Set GOOGLE_APPLICATION_CREDENTIALS from environment variable
        gac_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if not gac_path:
            raise EnvironmentError("GOOGLE_APPLICATION_CREDENTIALS is not set in .env")

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gac_path

        # # Load GCP credentials and fetch project details
        # gcp_key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
        # if not gcp_key_path or not os.path.exists(gcp_key_path):
        #     self.logger.error("Google Application Credentials file not found or not set.")
        #     raise FileNotFoundError("Google Application Credentials file is missing.")

        with open(gac_path, "r") as key_file:
            gcp_credentials = json.load(key_file)
            self.GCP_PROJECT_ID = gcp_credentials.get("project_id", "default-gcp-project-id")
            self.GCP_REGION = gcp_credentials.get("region", "us-central1")  # Default to 'us-central1' if not set

        self.logger.info(f"Initialized with Project ID: {self.GCP_PROJECT_ID}, Region: {self.GCP_REGION}")
    
    @staticmethod
    def print_image_resolution(image_path: str):
        try:
            # Open the image file
            with Image.open(image_path) as img:
                # Get image resolution
                width, height = img.size
                print(f"Resolution of the image: {width}x{height}")
        except Exception as e:
            print(f"Error occurred: {e}")

    @staticmethod
    def resize_image_for_gpt(image_path: str, target_width: int = 1280, target_height: int = 640) -> str:
        try:
            img = Image.open(image_path)
            img = img.resize((target_width, target_height))
            buffer = io.BytesIO()
            img.save(buffer, format="JPEG")
            return base64.b64encode(buffer.getvalue()).decode('utf-8')
        except Exception as e:
            print(f"Error occurred: {e}")
            return ""


    @timeit
    def run_image_captioning(self) -> bool:
        video_frames_path = return_video_frames_folder(self.video_runner_obj)
        video_folder_path = return_video_folder_name(self.video_runner_obj)

        self.logger.info(f"Video frames path: {video_frames_path}")
        self.logger.info(f"Video folder path: {video_folder_path}")

        try:
            # Retrieve frame extraction data from the database
            frame_extraction_data = get_module_output(self.video_runner_obj["video_id"],
                                                    self.video_runner_obj["AI_USER_ID"], 'frame_extraction')
            if not frame_extraction_data:
                self.logger.warning("No frame extraction data found in the database. Using default values.")
                frame_extraction_data = {
                    'steps': 5,
                    'frames_extracted': len(os.listdir(video_frames_path)),
                    'adaptive_fps': 25.0
                }

            step = int(frame_extraction_data['steps'])
            num_frames = int(frame_extraction_data['frames_extracted'])
            frames_per_second = float(frame_extraction_data['adaptive_fps'])

            self.logger.info(f"num_frames: {num_frames}")

            video_fps = step * frames_per_second
            seconds_per_frame = 1.0 / video_fps

            keyframes = self.load_keyframes(video_folder_path)

            outcsvpath = os.path.join(video_folder_path, CAPTIONS_CSV)
            self.logger.info(f"Processing captions in: {outcsvpath}")

            # Read existing rows
            with open(outcsvpath, 'r', encoding='utf-8') as incsvfile:
                reader = csv.reader(incsvfile)
                header = next(reader)
                existing_rows = list(reader)
                
            self.logger.info(f"existing_rows: {existing_rows}")
            return True
    
            # Write updated rows
            with open(outcsvpath, 'w', newline='', encoding='utf-8') as outcsvfile:
                writer = csv.writer(outcsvfile)
                writer.writerow([KEY_FRAME_HEADERS[FRAME_INDEX_SELECTOR], KEY_FRAME_HEADERS[TIMESTAMP_SELECTOR],
                                 KEY_FRAME_HEADERS[IS_KEYFRAME_SELECTOR], KEY_FRAME_HEADERS[KEYFRAME_CAPTION_SELECTOR], 
                                 "GPT4_Caption"])  # Add new columns
                
                for i in range(0, min(num_frames, len(existing_rows))):
                    frame_index = 5 * i
                    frame_filename = f'{video_frames_path}/frame_{frame_index}.jpg'
                    gs_frame_filename = f"gs://datasets_prod/OJUNlr0NFj8_files_650506db3ff1c2140ea10ece/frames/frame_{frame_index}.jpg"
                    
                    existing_row = existing_rows[i]
                    # Prepare the row
                    row = [None] * 6  # Initialize a 6-column row
                    # row[0] = frame_index  # Frame index in column 1
                    # row[1] = float(frame_index) * seconds_per_frame    # Timestamp in column 2
                    # row[2] = frame_index in keyframes  # Is keyframe in column 3
                    row[0:4] = existing_row[0:4]  # Preserve the existing caption in column 4
                    # self.logger.info(f"existing_row[3]: {existing_row[3]}")
                    if os.path.exists(frame_filename):                       
                        gpt4_caption = self.get_gpt_caption(frame_filename)  # GPT-4 Caption
                        row[4] = gpt4_caption
                        # gpt4_caption = ""
                        # vertex_caption = self.get_vertex_caption(gs_frame_filename)  # Vertex AI Caption
                        vertex_caption = ""

                        self.logger.info(f"Frame index: {frame_index}, GPT-4: {gpt4_caption}, Vertex: {vertex_caption}")
                    else:
                        self.logger.warning(f"Frame {frame_index} does not exist, skipping.")
                    writer.writerow(row)
                    outcsvfile.flush()

            # Mark image captioning as done in the database
            # update_status(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"], "done")

            # Save the generated captions to the database
            # update_module_output(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"],
                                # 'image_captioning', {"captions_file": outcsvpath})

            return True

        except Exception as e:
            self.logger.error(f"Error in image captioning: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False


    def load_keyframes(self, video_folder_path: str) -> List[int]:
        with open(os.path.join(video_folder_path, KEYFRAMES_CSV), newline='', encoding='utf-8') as incsvfile:
            reader = csv.reader(incsvfile)
            next(reader)  # skip header
            return [int(row[0]) for row in reader]

    PROMPT = (
    "Generate a concise and accurate description of this video frame. "
    "Prioritize accessibility for blind and visually impaired individuals. "
    "Consider the essential details visible in the frame."
    )

    def get_vertex_caption(self, filename: str) -> str:
        self.logger.info(f"Starting Vertex AI caption generation for file: {filename}")
        try:
            model = self.service_agents.get_vertex_model("gemini-1.5-flash-002")
            image_file = Part.from_uri(filename, "image/jpeg")
            response = model.generate_content([image_file, self.PROMPT])
            return response.text.strip() if response and response.text else ""
        except Exception as e:
            self.logger.error(f"Error with Vertex AI: {e}")
            return ""

    def get_gpt_caption(self, filename: str) -> str:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            self.logger.error("OPENAI_API_KEY not set in the environment.")
            return ""

        try:
            # Read and encode the image file
            with open(filename, "rb") as file:
                image_base64 = base64.b64encode(file.read()).decode('utf-8')

            # Create the payload
            payload = {
                "model": "gpt-4o",
                "messages": [
                    {"role": "system", "content": (
                        "Generate a concise and accurate description of this video frame. "
                        "Prioritize accessibility for blind and visually impaired individuals."
                    )},
                    {"role": "user", "content": [
                        {
                            "type": "text",
                            "text": self.PROMPT
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{image_base64}"
                            }
                        }
                    ]
                        
                    }
                ]
            }

            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            }

            # Send the POST request
            response = requests.post(
                "https://api.openai.com/v1/chat/completions", headers=headers, json=payload
            )

            # Handle the response
            if response.status_code == 200:
                return response.json()["choices"][0]["message"]["content"].strip()
            else:
                self.logger.error(f"GPT API returned error {response.status_code}: {response.text}")
                return ""

        except Exception as e:
            self.logger.error(f"Error generating caption: {e}")
            return ""



    def filter_keyframes_from_caption(self) -> None:
        video_folder_path = return_video_folder_name(self.video_runner_obj)
        keyframes = self.load_keyframes(video_folder_path)

        csv_file_path = os.path.join(video_folder_path, CAPTIONS_CSV)
        updated_rows = []

        with open(csv_file_path, 'r', newline='', encoding='utf-8') as csv_file:
            csv_reader = csv.reader(csv_file)
            header = next(csv_reader)
            updated_rows.append(header)

            for row in csv_reader:
                frame_index = int(row[0])
                is_keyframe = frame_index in keyframes
                row[2] = is_keyframe
                if '<unk>' not in row[3]:
                    updated_rows.append(row)

        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerows(updated_rowsow)

    def combine_image_caption(self) -> bool:
        video_folder_path = return_video_folder_name(self.video_runner_obj)
        captcsvpath = os.path.join(video_folder_path, CAPTIONS_CSV)

        if not os.path.exists(captcsvpath):
            self.logger.error(f"Captions file not found: {captcsvpath}")
            return False

        try:
            with open(captcsvpath, 'r', newline='', encoding='utf-8') as captcsvfile:
                data = csv.DictReader(captcsvfile)
                video_frames_path = return_video_frames_folder(self.video_runner_obj)
                image_caption_pairs = [
                    {
                        "frame_index": row[KEY_FRAME_HEADERS[FRAME_INDEX_SELECTOR]],
                        "frame_url": f'{video_frames_path}/frame_{row[KEY_FRAME_HEADERS[FRAME_INDEX_SELECTOR]]}.jpg',
                        "caption": row[KEY_FRAME_HEADERS[KEYFRAME_CAPTION_SELECTOR]]
                    } for row in data
                ]

            image_caption_csv_file = os.path.join(video_folder_path, CAPTION_IMAGE_PAIR)
            with open(image_caption_csv_file, 'w', encoding='utf8', newline='') as output_file:
                fieldnames = ["frame_index", "frame_url", "caption"]
                csvDictWriter = csv.DictWriter(output_file, fieldnames=fieldnames)
                csvDictWriter.writeheader()
                csvDictWriter.writerows(image_caption_pairs)

            self.logger.info(f"Completed Writing Image Caption Pair to CSV")
            return True

        except Exception as e:
            self.logger.error(f"Error in combine_image_caption: {str(e)}")
            return False