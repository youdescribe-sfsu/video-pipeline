import google.generativeai as genai
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

from PIL import Image
import cv2
import io

class GptCollageCaptioning:
    def __init__(self, video_runner_obj: Dict[str, Any]):
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        self.service_agents = ServiceAgents()  # Initialize service agents
    
    @timeit
    def run_gpt_collage_captioning(self) -> bool:
        video_folder_path = return_video_folder_name(self.video_runner_obj)
        video_frames_path = os.path.join(video_folder_path, "collages")

        self.logger.info(f"Video frames path: {video_frames_path}")
        self.logger.info(f"Video folder path: {video_folder_path}")

        try:
            num_frames = len(os.listdir(video_frames_path))
            json_path = os.path.join(video_folder_path, "summarized_scenes.json")
            incsvpath = os.path.join(video_folder_path, "scenesegmentedfile.csv")
            outcsvpath = os.path.join(video_folder_path, "gptcollagecaption.csv")
            
            self.logger.info(f"num_frames: {num_frames}")
            self.logger.info(f"Processing captions in: {outcsvpath}")

            # Read JSON data
            with open(json_path, 'r', encoding='utf-8') as json_file:
                existing_rows = json.load(json_file)


            # # Read existing rows
            # with open(incsvpath, 'r', encoding='utf-8') as incsvfile:
            #     reader = csv.reader(incsvfile)
            #     header = next(reader)
            #     existing_rows = list(reader)
                
            # self.logger.info(f"existing_rows: {existing_rows}")
                
            # Write updated rows
            with open(outcsvpath, 'w', newline='', encoding='utf-8') as outcsvfile:
                writer = csv.writer(outcsvfile)
                writer.writerow([ "start_time",	"end_time",	"description"
, "GPT4_Caption", "Gemini_Caption"])  # Add new columns
                
                for i in range(0, num_frames):
                    # Ensure the frame index does not exceed the number of rows in the JSON
                    if i < len(existing_rows):
                        existing_row = existing_rows[i]
                    else:
                        self.logger.warning(f"No data for frame {i} in summarized_scenes.json, skipping.")
                        continue

                    # Dynamically retrieve file names in the directory
                    frame_filenames = sorted(
                        [os.path.join(video_frames_path, file) for file in os.listdir(video_frames_path) if file.endswith('.png')]
                    )

                    # Ensure the frame index does not exceed the number of files
                    if i < len(frame_filenames):
                        frame_filename = frame_filenames[i]
                    else:
                        self.logger.warning(f"Frame {i} does not exist in {video_frames_path}, skipping.")
                        continue
        
                    # Prepare the row
                    row = [None] * 5  # Initialize a 6-column row
                    row[0] = existing_row["start_time"]
                    row[1] = existing_row["end_time"]
                    row[2] = existing_row["text"]

                    if os.path.exists(frame_filename):                       
                        gpt4_caption = self.get_gpt_caption(frame_filename)  # GPT-4 Caption
                        genai_caption = self.get_gemini_caption(frame_filename) # Gemini Caption
                        row[3] = gpt4_caption
                        row[4] = genai_caption

                        self.logger.info(f"Frame index: {i}, GPT-4: {gpt4_caption}, Gemini: {genai_caption}")
                    else:
                        self.logger.warning(f"Frame {i} does not exist, skipping.")
                    writer.writerow(row)
                    outcsvfile.flush()

            return True

        except Exception as e:
            self.logger.error(f"Error in image captioning: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False


    system_prompt = f"""Generate a unique, distinct, and concise description for a video segment. The description should be tailored to be accessible for blind and visually impaired individuals following these guidelines. 

    Guidelines - a Quality Description Must Be: 
    Accurate: There must be no errors in word selection, pronunciation, diction, or enunciation. Prioritized: Content essential to the intended learning and enjoyment outcomes is of primary importance. 
    Consistent: Both the description content and the voicing should match the style, tone, and pace of the program. 
    Appropriate: Consider the intended audience, be objective, and seek simplicity and succinctness.
    Clarity: If the image is unclear, describe it using a broader category rather than guessing specific details. Be objective.
    Equal: Equal access requires that the meaning and intention of the program be conveyed.
    Length: Must be between 30 and 40 words.

    The 8 frames are provided as a single image where frames are read from left to right. Please make description representative of the whole video clip and not frame by frame. 
    """
    PROMPT = (
    "Generate a concise and accurate description of this video frame. "
    "Prioritize accessibility for blind and visually impaired individuals. "
    "Consider the essential details visible in the frame."
    )

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
                    # {"role": "system", "content": (
                    #     "Generate a concise and accurate description of this video frame. "
                    #     "Prioritize accessibility for blind and visually impaired individuals."
                    # )},
                    {"role": "user", "content": [
                        {
                            "type": "text",
                            "text": self.system_prompt
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
            self.logger.error(f"Error generating GPT caption: {e}")
            return ""

    def get_gemini_caption(self, filename: str) -> str:
        gemini_api_key = os.getenv("GEMINI_API_KEY")
        if not gemini_api_key:
            self.logger.error("GEMINI_API_KEY not set in the environment.")
            return ""
        try:
            model = genai.GenerativeModel("gemini-1.5-pro")
            with open(filename, "rb") as file:
                image_base64 = base64.b64encode(file.read()).decode('utf-8')
            response = model.generate_content([{'mime_type':'image/jpeg', 'data': image_base64}, self.system_prompt])

            return response.text
        except Exception as e:
            self.logger.error(f"Error generating Gemini caption: {e}")
            return ""

        
