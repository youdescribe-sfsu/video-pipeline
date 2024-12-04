import os
import json
import base64
import requests
import pandas as pd
from dotenv import load_dotenv
from transformers import AutoTokenizer
import random, tiktoken
import numpy as np
import sys, csv
from openai import OpenAI

def set_seed(seed):
    random.seed(seed)
    np.random.seed(seed)

set_seed(42)

system_prompt = f"""Generate 10 unique, distinct, and concise descriptions for a video segment and text from a human describer. These descriptions should be tailored to be accessible for blind and visually impaired individuals following these guidelines. 

Guidelines - a Quality Description Must Be: 
Accurate: There must be no errors in word selection, pronunciation, diction, or enunciation. Prioritized: Content essential to the intended learning and enjoyment outcomes is of primary importance. 
Consistent: Both the description content and the voicing should match the style, tone, and pace of the program. 
Appropriate: Consider the intended audience, be objective, and seek simplicity and succinctness. 
Equal: Equal access requires that the meaning and intention of the program be conveyed.

The 8 frames are provided as a single image where frames are read from left to right. The original text description follows. Please make each description representative of the whole video clip and not frame by frame. Do not give titles to each descrpiton. Seperate each description with two newline characters. Do not print the number of the description. 
"""
# input_csv = "/home/918573232/vd_aug_gpt/vd_aug/spanish_caption_examples.csv"
input_csv = "/home/922053012/yd-video-caption-evaluation/regenerate10_gpt_captions.csv"
image_dir = '/data1/juve/datasets/youdescribe/videos/8-framed_images'
output_csv = "/home/918573232/vd_aug_gpt/vd_aug/YD_2.0_transcribed_audio_clips_english_regenerated_v5_v2_and_v6_4o_captions_attempt_10.csv"
# output_csv = "/home/918573232/vd_aug_gpt/vd_aug/YD_2.0_transcribed_audio_clips_spanish_4o_captions.csv"

df = pd.read_csv(input_csv)
gpt2_tokenizer = AutoTokenizer.from_pretrained("gpt2")
gpt4o_tokenizer = tiktoken.encoding_for_model("gpt-4o")

# Add 'gpt-4o_captions' column if it does not exist
if 'gpt-4o_captions' not in df.columns:
    df['gpt-4o_captions'] = None
    print("Created empty 'gpt-4o_captions' column")

load_dotenv() 
api_key = os.getenv('OPENAI_API_KEY')

# CREATE A CLIENT
client = OpenAI(
  organization=os.getenv("ORGANIZATION_ID"),
  project=os.getenv('PROJECT_ID'),
)

def encode_image(image_path):
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')

def update_row(video_id, new_captions):
    matching_rows = df[df['clip_id'] == video_id]
    for index in matching_rows.index:
        df.at[index, 'gpt-4o_captions'] = new_captions
    print(f"Updated {len(matching_rows)} rows for video ID {video_id}.")

output_tokens = []
request_times = []
count = 0
unknown_videos = []

for index, row in df.iterrows():
    curr_vid_id = row['clip_id']
    # gt_captions = row["oa_transcript_sentence"]
    gt_captions = row["audio_clip_transcript"]
    gpt_captions_value = row['gpt-4o_captions']
    print(f"Processing video ID {curr_vid_id}")
    print(image_dir + "/" + curr_vid_id + ".png")
    if not os.path.exists(image_dir + "/" + curr_vid_id + ".png"):
      unknown_videos.append(image_dir + "/" + curr_vid_id + ".png")
      continue

    image_path = os.path.join(image_dir, f"{curr_vid_id}.png")
    
    
    needs_captions = (
        pd.isna(gpt_captions_value) or 
        (isinstance(gpt_captions_value, list) and not any(gpt_captions_value)) or 
        gpt_captions_value == "['API request failed']" or 
        gpt_captions_value == ''
    )
    print(f"gpt_captions_value: {gpt_captions_value}")
    print(f"needs_caption: {needs_captions}")
    if needs_captions:
        print(f"Generating captions for video ID {curr_vid_id}")
            
        base64_image = encode_image(image_path)
        print(f"gt_captions: {gt_captions}")

        # sys.exit()
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }

        payload = {
            "model": "gpt-4o",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": system_prompt + gt_captions[0]
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{base64_image}"
                            }
                        }
                    ]
                }
            ],
            "max_tokens": 1024
        }

        try:
            response = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
            new_captions = response.json()['choices'][0]['message']['content']#.split("\n\n")
            update_row(curr_vid_id, new_captions)
        except Exception as e:
            print(f"Error processing video ID {curr_vid_id}: {str(e)}")
            update_row(curr_vid_id, ["API request failed"])

    # Periodically save the DataFrame to CSV
    if index % 100 == 0:
        print("Saving progress to CSV...")
        df.to_csv(output_csv, index=False)

df.to_csv(output_csv, index=False)
print("Finished processing all rows.")

# /home/918573232/vd_aug_gpt/vd_aug/YD_2.0_transcribed_audio_clips_spanish_4o_captions.csv

with open("./unknown_video_ids_regenerated_v5_v2_and_v6_4o_captions_attempt_10.csv", mode='w', newline='') as file:
# with open("/home/918573232/vd_aug_gpt/vd_aug/YD_2.0_transcribed_audio_clips_spanish_4o_captions_weird_videos.csv", mode='w', newline='') as file:
  writer = csv.writer(file)
  writer.writerow(["video_id"])
  
  for vid_id in unknown_videos:
    writer.writerow([vid_id])
