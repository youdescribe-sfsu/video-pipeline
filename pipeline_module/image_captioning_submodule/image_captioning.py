import csv
import os
import requests
import json
import traceback
from typing import Dict, Any, List, Tuple

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

    def get_caption(self, filename: str) -> str:
        page = f'http://localhost:{os.getenv("GPU_LOCAL_PORT") or "8085"}/upload'
        token = 'VVcVcuNLTwBAaxsb2FRYTYsTnfgLdxKmdDDxMQLvh7rac959eb96BCmmCrAY7Hc3'

        with open(filename, 'rb') as file_buffer:
            multipart_form_data = {
                'token': ('', str(token)),
                'image': (os.path.basename(filename), file_buffer),
                'min_length': 25,
                'max_length': 50
            }

            self.logger.info(f"Running image captioning for {filename}")

            try:
                response = requests.post(page, files=multipart_form_data, timeout=10)
                response.raise_for_status()
                json_obj = response.json()
                return json_obj['caption'].strip()
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Error in image captioning request: {str(e)}")
                raise

    @timeit
    def run_image_captioning(self) -> None:
        video_frames_path = return_video_frames_folder(self.video_runner_obj)
        video_folder_path = return_video_folder_name(self.video_runner_obj)

        if read_value_from_file(video_runner_obj=self.video_runner_obj, key="['ImageCaptioning']['started']") == 'done':
            self.logger.info("Image captioning already done")
            return

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
                caption = self.get_caption(frame_filename)

                if caption:
                    row = [frame_index, float(frame_index) * seconds_per_frame, frame_index in keyframes, caption]
                    writer.writerow(row)

                self.logger.info(f"Frame index: {frame_index}, Caption: {caption}")
                outcsvfile.flush()
                save_value_to_file(video_runner_obj=self.video_runner_obj,
                                   key="['ImageCaptioning']['run_image_captioning']['last_processed_frame']",
                                   value=frame_index)

        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['ImageCaptioning']['started']", value='done')

    def load_keyframes(self, video_folder_path: str) -> List[int]:
        with open(video_folder_path + '/' + KEYFRAMES_CSV, newline='', encoding='utf-8') as incsvfile:
            reader = csv.reader(incsvfile)
            next(reader)  # skip header
            return [int(row[0]) for row in reader]

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
                if '<unk>' not in row[3]:
                    updated_rows.append(row)

        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerows(updated_rows)

        save_value_to_file(video_runner_obj=self.video_runner_obj,
                           key="['ImageCaptioning']['filter_keyframes_from_caption']", value=1)

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
                    "caption": row[KEY_FRAME_HEADERS[KEYFRAME_CAPTION_SELECTOR]]
                } for row in data
            ]

        image_caption_csv_file = return_video_folder_name(self.video_runner_obj) + '/' + CAPTION_IMAGE_PAIR
        with open(image_caption_csv_file, 'w', encoding='utf8', newline='') as output_file:
            csvDictWriter = csv.DictWriter(output_file, fieldnames=image_caption_pairs[0].keys())
            csvDictWriter.writeheader()
            csvDictWriter.writerows(image_caption_pairs)

        save_value_to_file(video_runner_obj=self.video_runner_obj, key="['ImageCaptioning']['combine_image_caption']",
                           value=1)
        self.logger.info(f"Completed Writing Image Caption Pair to CSV")