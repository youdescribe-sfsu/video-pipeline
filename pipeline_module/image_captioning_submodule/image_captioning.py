import csv
import os
import requests
import json
import traceback
from typing import Dict, Any, List, Tuple, Optional
from transformers import BlipProcessor, BlipForConditionalGeneration
import torch
from PIL import Image
from ..utils_module.timeit_decorator import timeit
from ..utils_module.utils import (
    CAPTIONS_CSV, FRAME_INDEX_SELECTOR, IS_KEYFRAME_SELECTOR,
    KEY_FRAME_HEADERS, KEYFRAME_CAPTION_SELECTOR, KEYFRAMES_CSV,
    TIMESTAMP_SELECTOR, read_value_from_file, return_video_folder_name,
    return_video_frames_folder, CAPTION_IMAGE_PAIR, save_value_to_file
)


class ImageCaptioning:
    def __init__(self, video_runner_obj: Dict[str, Any]):
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.processor = BlipProcessor.from_pretrained("Salesforce/blip-image-captioning-large")
        self.model = BlipForConditionalGeneration.from_pretrained("Salesforce/blip-image-captioning-large").to(
            self.device)
        self.caption_history = []

    @timeit
    def run_image_captioning(self) -> bool:
        video_frames_path = return_video_frames_folder(self.video_runner_obj)
        video_folder_path = return_video_folder_name(self.video_runner_obj)

        if read_value_from_file(video_runner_obj=self.video_runner_obj, key="['ImageCaptioning']['started']") == 'done':
            self.logger.info("Image captioning already done")
            return True

        try:
            step = read_value_from_file(video_runner_obj=self.video_runner_obj, key="['video_common_values']['step']")
            num_frames = read_value_from_file(video_runner_obj=self.video_runner_obj,
                                              key="['video_common_values']['num_frames']")
            frames_per_second = read_value_from_file(video_runner_obj=self.video_runner_obj,
                                                     key="['video_common_values']['frames_per_second']")

            video_fps = step * frames_per_second
            seconds_per_frame = 1.0 / video_fps

            keyframes = self.load_keyframes(video_folder_path)

            outcsvpath = video_folder_path + '/' + CAPTIONS_CSV
            mode = 'w'

            with open(outcsvpath, mode, newline='', encoding='utf-8') as outcsvfile:
                writer = csv.writer(outcsvfile)
                writer.writerow([KEY_FRAME_HEADERS[FRAME_INDEX_SELECTOR], KEY_FRAME_HEADERS[TIMESTAMP_SELECTOR],
                                 KEY_FRAME_HEADERS[IS_KEYFRAME_SELECTOR], KEY_FRAME_HEADERS[KEYFRAME_CAPTION_SELECTOR]])

                for frame_index in range(0, num_frames, step):
                    frame_filename = f'{video_frames_path}/frame_{frame_index}.jpg'
                    captions = self.generate_captions(frame_filename)

                    if captions:
                        row = [frame_index, float(frame_index) * seconds_per_frame, frame_index in keyframes,
                               json.dumps(captions)]
                        writer.writerow(row)

                    self.logger.info(f"Frame index: {frame_index}, Captions: {captions}")
                    outcsvfile.flush()
                    save_value_to_file(video_runner_obj=self.video_runner_obj,
                                       key="['ImageCaptioning']['run_image_captioning']['last_processed_frame']",
                                       value=str(frame_index))

            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['ImageCaptioning']['started']",
                               value='done')
            return True

        except Exception as e:
            self.logger.error(f"Error in image captioning: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False

    def load_keyframes(self, video_folder_path: str) -> List[int]:
        with open(video_folder_path + '/' + KEYFRAMES_CSV, newline='', encoding='utf-8') as incsvfile:
            reader = csv.reader(incsvfile)
            next(reader)  # skip header
            return [int(row[0]) for row in reader]

    def generate_captions(self, image_path: str) -> List[str]:
        image = Image.open(image_path).convert('RGB')
        inputs = self.processor(image, return_tensors="pt").to(self.device)

        # Generate multiple captions with different strategies
        captions = []

        # Standard caption
        captions.append(self.generate_single_caption(inputs))

        # Caption with different prompt
        captions.append(self.generate_single_caption(inputs, prompt="Describe the scene in detail:"))

        # Caption focusing on actions
        captions.append(self.generate_single_caption(inputs, prompt="What actions are happening in this image?"))

        # Caption focusing on objects
        captions.append(self.generate_single_caption(inputs, prompt="List the main objects in this image:"))

        # Remove any duplicate captions
        captions = list(dict.fromkeys(captions))

        self.caption_history.append(captions[0])  # Add the main caption to history
        if len(self.caption_history) > 5:
            self.caption_history.pop(0)  # Keep only the last 5 captions

        return captions

    def generate_single_caption(self, inputs: Dict[str, torch.Tensor], prompt: Optional[str] = None) -> str:
        if prompt:
            inputs['prompt'] = prompt

        # Use caption history for context
        if self.caption_history:
            context = " ".join(self.caption_history[-3:])  # Use last 3 captions as context
            inputs['prompt'] = f"Context: {context}. {prompt or ''}"

        outputs = self.model.generate(**inputs, max_new_tokens=50)
        return self.processor.decode(outputs[0], skip_special_tokens=True)

    def filter_keyframes_from_caption(self) -> None:
        if read_value_from_file(video_runner_obj=self.video_runner_obj,
                                key="['ImageCaptioning']['filter_keyframes_from_caption']") == 1:
            self.logger.info("Filtering keyframes from caption already done, skipping step.")
            return

        video_folder_path = return_video_folder_name(self.video_runner_obj)
        keyframes = self.load_keyframes(video_folder_path)

        csv_file_path = video_folder_path + '/' + CAPTIONS_CSV
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

        save_value_to_file(video_runner_obj=self.video_runner_obj,
                           key="['ImageCaptioning']['filter_keyframes_from_caption']", value=str(1))

    def combine_image_caption(self) -> None:
        if read_value_from_file(video_runner_obj=self.video_runner_obj,
                                key="['ImageCaptioning']['combine_image_caption']") == 1:
            self.logger.info("Image Captioning already done")
            return

        captcsvpath = return_video_folder_name(self.video_runner_obj) + '/' + CAPTIONS_CSV

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

        image_caption_csv_file = return_video_folder_name(self.video_runner_obj) + '/' + CAPTION_IMAGE_PAIR
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

        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['ImageCaptioning']['combine_image_caption']",
                           value=str(1))
        self.logger.info(f"Completed Writing Image Caption Pair to CSV")


if __name__ == "__main__":
    # For testing purposes
    video_runner_obj = {
        "video_id": "test_video",
        "logger": print  # Use print as a simple logger for testing
    }
    image_captioning = ImageCaptioning(video_runner_obj)
    success = image_captioning.run_image_captioning()
    print(f"Image captioning {'succeeded' if success else 'failed'}")

    if success:
        image_captioning.filter_keyframes_from_caption()
        image_captioning.combine_image_caption()