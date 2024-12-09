import pandas as pd
import av
import os
import logging
from PIL import Image, ImageOps
import numpy as np
import tqdm
from typing import Dict, Any, List
from web_server_module.web_server_database import update_status, update_module_output, get_module_output
from ..utils_module.timeit_decorator import timeit
from ..utils_module.utils import (
    return_video_folder_name
)

class GenerateCollage:
    def __init__(self, video_runner_obj: Dict[str, Any]):
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger") or logging.getLogger(__name__)        
        video_id = self.video_runner_obj["video_id"]
        self.logger.info(f"Collage generation started for video ID: {video_id}")

    def extract_n_frames_from_interval(
        self, video_folder_path: str, video_id: str, ext: str, n: int, start_time: float, end_time: float
    ) -> List[np.ndarray]:
        """
        Extracts n evenly spaced frames from a specified interval in a video.

        :param video_folder_path: Path to video clips stored on disk.
        :param video_id: Video ID for identifying the video file.
        :param ext: The video file extension (e.g., '.mp4').
        :param n: Number of evenly spaced frames to extract.
        :param start_time: Start time of the interval in seconds.
        :param end_time: End time of the interval in seconds.
        :return: List of extracted frames as numpy arrays.
        """
        self.logger.info(f"Collage generation started for video ID: {video_id}")
        try:
            container = av.open(os.path.join(video_folder_path, video_id + ext))
            container.seek(int(start_time * av.time_base))

            # Calculate total frames in the specified interval
            duration_frames = int((end_time - start_time) * container.streams.video[0].average_rate)
            indices = set(np.linspace(0, duration_frames, num=n, endpoint=True).astype(int))

            frames = []
            for i, frame in enumerate(container.decode(video=0)):
                if len(frames) >= n:  # Stop if we've collected enough frames
                    break
                if i in indices:
                    frames.append(frame.to_ndarray(format="rgb24"))
            return frames

        except Exception as e:
            self.logger.error(f"Error extracting frames: {e}")
            return []

    def is_frame_dark(self, frame: np.ndarray, threshold: float = 0.5) -> bool:
        """
        Determines if a frame is predominantly dark.

        :param frame: The frame as a numpy array.
        :param threshold: Brightness threshold to determine darkness.
        :return: Boolean indicating if the frame is dark.
        """
        grayscale = np.dot(frame[..., :3], [0.2989, 0.587, 0.114])
        avg_brightness = np.mean(grayscale) / 255.0
        return avg_brightness < threshold

    def save_n_framed_images_from_scenes(
        self, scene_csv_path: str, output_dir: str, video_folder_path: str, n: int, collage_width: int = 1200, collage_height: int = 600
    ):
        """
        Reads scene segments from a CSV file and generates n-framed collages for each scene.

        :param scene_csv_path: Path to the scene-segmented CSV file.
        :param output_dir: Directory to store the generated collages.
        :param video_folder_path: Directory containing video clips.
        :param n: Number of evenly spaced frames to include in each collage.
        :param collage_width: Width of the collage image.
        :param collage_height: Height of the collage image.
        """
        try:
            if not os.path.exists(scene_csv_path):
                raise FileNotFoundError(f"Scene CSV file not found: {scene_csv_path}")
            scenes = pd.read_csv(scene_csv_path)
            self.logger.info(f"Found {len(scenes)} scenes in the CSV file.")

            os.makedirs(output_dir, exist_ok=True)
            self.logger.info(f"Output directory created/verified: {output_dir}")

            cols, rows = 4, 2
            border_size = 10
            frame_width = (collage_width // cols) - (2 * border_size)
            frame_height = (collage_height // rows) - (2 * border_size)
            frame_size = (frame_width, frame_height)
            fps = 25

            for idx, row in tqdm.tqdm(scenes.iterrows(), desc="Processing Scenes", total=len(scenes), unit="scene"):
                self.logger.info(f"Number of scenes: {len(scenes)}")  # Log the number of scenes
                self.logger.info(f"Processing scene {idx}: {row}")
                start_time, end_time, description = row["start_time"], row["end_time"], row["description"]
                scene_name = f"frames_{int(start_time * fps):03d}_{int(end_time * fps):03d}_collage.png"
                video_id = self.video_runner_obj["video_id"]
                ext = ".mp4"

                frames = self.extract_n_frames_from_interval(video_folder_path, video_id, ext, n, start_time, end_time)
                self.logger.debug(f"Extracted {len(frames)} frames for scene {scene_name}.")
                
                if len(frames) < n:
                    self.logger.warning(f"Skipping scene {idx}: insufficient frames ({len(frames)}).")
                    continue

                collage_image = Image.new("RGB", (collage_width, collage_height))
                for index, frame in enumerate(frames):
                    self.logger.info(f"Processing frame {index + 1}/{len(frames)}")
                    self.logger.info(f"Frame type: {type(frame)}")
                    self.logger.info(f"Frame shape (if numpy array): {frame.shape if isinstance(frame, np.ndarray) else 'N/A'}")
                    self.logger.info(f"Collage dimensions: width={collage_width}, height={collage_height}")

                    frame = Image.fromarray(frame)
                    frame = frame.resize(frame_size, Image.Resampling.LANCZOS)

                    border_color = "white" if self.is_frame_dark(np.array(frame)) else "black"
                    bordered_frame = ImageOps.expand(frame, border=border_size, fill=border_color)

                    row, col = divmod(index, cols)
                    x = col * (frame_size[0] + 2 * border_size)
                    y = row * (frame_size[1] + 2 * border_size)

                    collage_image.paste(bordered_frame, (x, y))

                output_file = os.path.join(output_dir, scene_name)
                collage_image.save(output_file)
                self.logger.info(f"Saved collage for scene {idx}: {output_file}")

        except Exception as e:
            self.logger.error(f"Error generating collages: {e}")
    
    @timeit
    def run_generate_collage(self) -> bool:
        """
        Executes the collage generation process and returns True if successful, False otherwise.
        """
        video_folder_path = return_video_folder_name(self.video_runner_obj)
        scene_csv_path = os.path.join(video_folder_path, "scenesegmentedfile.csv")
        output_dir = os.path.join(video_folder_path, "collages")

        self.logger.info(f"Reading scene CSV: {scene_csv_path}")
        
        # Check if the scene CSV file exists
        if not os.path.exists(scene_csv_path):
            self.logger.error(f"Scene CSV file not found: {scene_csv_path}")
            return False

        try:
            n = 8
            # Attempt to generate collages
            self.save_n_framed_images_from_scenes(scene_csv_path, output_dir, video_folder_path, n)
            self.logger.info("Collage generation completed successfully.")
            return True
        except Exception as e:
            self.logger.error(f"Error generating collages: {e}")
            return False
