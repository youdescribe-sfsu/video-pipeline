import csv
import os
import requests
import json
import traceback
from typing import Dict, Any, List, Optional
from transformers import BlipProcessor, BlipForConditionalGeneration
import torch
from PIL import Image
from web_server_module.web_server_database import get_status_for_youtube_id, update_status, update_module_output
from ..utils_module.timeit_decorator import timeit
from ..utils_module.utils import (
    CAPTIONS_CSV, FRAME_INDEX_SELECTOR, IS_KEYFRAME_SELECTOR,
    KEY_FRAME_HEADERS, KEYFRAME_CAPTION_SELECTOR, KEYFRAMES_CSV,
    TIMESTAMP_SELECTOR, return_video_folder_name,
    return_video_frames_folder, CAPTION_IMAGE_PAIR
)


class ImageCaptioning:
    def __init__(self, video_runner_obj: Dict[str, Any]):
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.processor = BlipProcessor.from_pretrained("Salesforce/blip-image-captioning-large")
        self.model = BlipForConditionalGeneration.from_pretrained("Salesforce/blip-image-captioning-large").to(self.device)
        self.caption_history = []

    @timeit
    def run_image_captioning(self) -> bool:
        video_frames_path = return_video_frames_folder(self.video_runner_obj)
        video_folder_path = return_video_folder_name(self.video_runner_obj)

        # Check if image captioning is already done using the database
        if get_status_for_youtube_id(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"]) == "done":
            self.logger.info("Image captioning already done")
            return True

        try:
            # Retrieve common video values from the database
            step = self.load_common_video_value('step')
            num_frames = self.load_common_video_value('num_frames')
            frames_per_second = self.load_common_video_value('frames_per_second')

            video_fps = step * frames_per_second
            seconds_per_frame = 1.0 / video_fps

            keyframes = self.load_keyframes(video_folder_path)

            outcsvpath = os.path.join(video_folder_path, CAPTIONS_CSV)

            with open(outcsvpath, 'w', newline='', encoding='utf-8') as outcsvfile:
                writer = csv.writer(outcsvfile)
                writer.writerow([KEY_FRAME_HEADERS[FRAME_INDEX_SELECTOR], KEY_FRAME_HEADERS[TIMESTAMP_SELECTOR],
                                 KEY_FRAME_HEADERS[IS_KEYFRAME_SELECTOR], KEY_FRAME_HEADERS[KEYFRAME_CAPTION_SELECTOR]])

                for frame_index in range(0, num_frames, step):
                    frame_filename = f'{video_frames_path}/frame_{frame_index}.jpg'
                    captions = self.generate_captions(frame_filename)

                    if captions:
                        row = [frame_index, float(frame_index) * seconds_per_frame, frame_index in keyframes, json.dumps(captions)]
                        writer.writerow(row)

                    self.logger.info(f"Frame index: {frame_index}, Captions: {captions}")
                    outcsvfile.flush()

            # Mark image captioning as done in the database
            update_status(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"], "done")

            # Save the generated captions to the database
            update_module_output(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"], 'image_captioning', {"captions": captions})

            return True

        except Exception as e:
            self.logger.error(f"Error in image captioning: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False

    def load_common_video_value(self, key: str) -> Optional[Any]:
        """
        Load common video values like step, num_frames, frames_per_second from the database.
        """
        try:
            value = get_status_for_youtube_id(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"], 'video_common_values')[key]
            return value
        except KeyError:
            self.logger.error(f"Error: Could not retrieve {key} from video common values")
            return None

    def load_keyframes(self, video_folder_path: str) -> List[int]:
        """
        Loads keyframes from the keyframes CSV file.
        """
        with open(os.path.join(video_folder_path, KEYFRAMES_CSV), newline='', encoding='utf-8') as incsvfile:
            reader = csv.reader(incsvfile)
            next(reader)  # skip header
            return [int(row[0]) for row in reader]

    def generate_captions(self, image_path: str) -> List[str]:
        """
        Generate captions for a given image using the BLIP model.
        """
        image = Image.open(image_path).convert('RGB')
        inputs = self.processor(image, return_tensors="pt").to(self.device)

        # Generate multiple captions using different prompts
        captions = []

        captions.append(self.generate_single_caption(inputs))  # Standard caption
        captions.append(self.generate_single_caption(inputs, prompt="Describe the scene in detail:"))
        captions.append(self.generate_single_caption(inputs, prompt="What actions are happening in this image?"))
        captions.append(self.generate_single_caption(inputs, prompt="List the main objects in this image:"))

        # Remove duplicates
        captions = list(dict.fromkeys(captions))

        # Maintain a history of the last 5 captions
        self.caption_history.append(captions[0])
        if len(self.caption_history) > 5:
            self.caption_history.pop(0)

        return captions

    def generate_single_caption(self, inputs: Dict[str, torch.Tensor], prompt: Optional[str] = None) -> str:
        """
        Generates a single caption with an optional prompt.
        """
        if prompt:
            inputs['prompt'] = prompt

        # Use caption history for context if available
        if self.caption_history:
            context = " ".join(self.caption_history[-3:])
            inputs['prompt'] = f"Context: {context}. {prompt or ''}"

        outputs = self.model.generate(**inputs, max_new_tokens=50)
        return self.processor.decode(outputs[0], skip_special_tokens=True)

    def filter_keyframes_from_caption(self) -> None:
        """
        Filters keyframes from the caption CSV file.
        """
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
                captions = json.loads(row[3])
                if any(caption for caption in captions if '<unk>' not in caption):
                    updated_rows.append(row)

        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerows(updated_rows)

    def combine_image_caption(self) -> bool:
        """
        Combines image paths with their respective captions and saves them to a CSV file.
        """
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
                        "captions": json.loads(row[KEY_FRAME_HEADERS[KEYFRAME_CAPTION_SELECTOR]])
                    } for row in data
                ]

            image_caption_csv_file = os.path.join(video_folder_path, CAPTION_IMAGE_PAIR)
            with open(image_caption_csv_file, 'w', encoding='utf8', newline='') as output_file:
                fieldnames = ["frame_index", "frame_url", "caption1", "caption2", "caption3", "caption4"]
                csvDictWriter = csv.DictWriter(output_file, fieldnames=fieldnames)
                csvDictWriter.writeheader()
                for pair in image_caption_pairs:
                    row = {
                        "frame_index": pair["frame_index"],
                        "frame_url": pair["frame_url"],
                    }
                    for i, caption in enumerate(pair["captions"], start=1):
                        row[f"caption{i}"] = caption
                    csvDictWriter.writerow(row)

            self.logger.info(f"Completed Writing Image Caption Pair to CSV")

        except Exception as e:
            self.logger.error(f"Error reading captions file: {str(e)}")
            return False