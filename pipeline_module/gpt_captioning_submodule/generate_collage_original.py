import pandas as pd
import av, os
from PIL import Image, ImageOps
import math
import numpy as np
import tqdm
from datasets import load_from_disk, DatasetDict

def extract_n_frames(path_to_clips: str, video_id: str, ext: str, n: int) -> list:
    '''
    Extracts n frames from a video clip & returns the extracted frames.
    
    :path_to_clips: path to video clips stored on disk.
    :video_id: video_id to know which clip from disk to process.
    :ext: the current video's file extension.
    :n: Number of evenly spaced-out frames per image.
    '''
    container = av.open(os.path.join(path_to_clips, video_id + ext))
    container.seek(0)
    
    # counting number of frames
    frame_count = 0
    for frame in container.decode(video=0):
        frame_count += 1
    
    # in case something wrong with the frame_count
    if frame_count < n:
        print(f"Skipping: videoID: {video_id}")
        pass
    
    # creating a set of indices that correspond to n equally spaced out frames in the interval "frame_count"
    indices = set(np.linspace(0, frame_count, num=n, endpoint=False).astype(np.int64))
    
    # rewinding video to beginning
    container.seek(0)

    # creating a list of frames for each indice in the video
    frames = []
    for i, frame in enumerate(container.decode(video=0)):
        if i in indices:
            frames.append(frame.to_ndarray(format="rgb24"))

    return frames


def is_frame_dark(frame: np.ndarray, threshold: float = 0.5) -> bool:
    '''
    Determines if a frame is predominantly dark.
    
    :frame: The frame as a numpy array.
    :threshold: The brightness threshold to determine if a frame is dark.
    :return: Boolean indicating if the frame is dark.
    '''
    # convert frame to grayscale
    grayscale = np.dot(frame[..., :3], [0.2989, 0.587, 0.114])
    # find average brightness
    avg_brightness = np.mean(grayscale) / 255.0
    return avg_brightness < threshold


def save_n_framed_image(dataset: DatasetDict, output_dir: str, path_to_clips: str, n: int, collage_width: int = 1200, collage_height: int = 600) -> None:
    '''
    Saves n-framed images to `output_dir`.
    
    :input_dataset_path: path to dataset that contains "video_id+start_end_time" field to
                         be able to look for the video clip on disk.
    :outout_dir: path to directory where generated images will be stored.
    :n: Number of evenly spaced-out frames per image.
    :return: None
    '''
    # creating a directory inside of output_dir to store n-framed images
    output_dir = os.path.join(output_dir, f"{n}-framed_images")
    os.makedirs(output_dir, exist_ok=True)

    # parameters for n-framed collage generation
    cols = 4
    rows = 2
    border_size = 10
    frame_width = (collage_width // cols) - (2 * border_size)
    frame_height = (collage_height // rows) - (2 * border_size)
    frame_size = (frame_width, frame_height)

    # loop thro each datapoint
    for vid_id in tqdm.tqdm(dataset["videoID"], desc="Processing Data", total=len(dataset), unit="rows"):
        ext = "mp4"
        
        # extract n frames from current video clip in dataset
        frames = extract_n_frames(path_to_clips, vid_id, ext, n)

        # create a blank image for the collage
        collage_image = Image.new('RGB', (collage_width, collage_height))

        # paste each frame into the empty collage image
        for index, frame in enumerate(frames):
            frame = Image.fromarray(frame)
            frame = frame.resize(frame_size, Image.Resampling.LANCZOS)

            border_color = 'white' if is_frame_dark(np.array(frame)) else 'black'

            # create a new image with border and paste the frame onto it
            bordered_frame = ImageOps.expand(frame, border=border_size, fill=border_color)

            # calculate current frame's position in collage
            row = index // cols
            col = index % cols
            x = col * (frame_size[0] + 2 * border_size)
            y = row * (frame_size[1] + 2 * border_size)

            # paste the curent frame to build collage image
            collage_image.paste(bordered_frame, (x, y))

        # save the collage image
        tmp_output_dir = os.path.join(output_dir, f"{vid_id}png")

        # print(tmp_output_dir)
        collage_image.save(tmp_output_dir)

input_dataset_csv = "/data1/juve/datasets/youdescribe/hf_datasets/YD_2.0_v5_v2_full_balanced_duration_10/arrow"
output_dir = "/data1/juve/datasets/youdescribe/videos"
path_to_clips = "/data1/juve/datasets/youdescribe/videos/clips/duration_10_balanced"
n = 8

# read in dataset
dataset = load_from_disk(input_dataset_csv)
dataset.set_format("torch")

for split in dataset:
    print(split)
    save_n_framed_image(dataset[split], output_dir, path_to_clips, n)
