# Runs all parts of the video processing pipeline except downloading the video
#! /usr/bin/env python
from dotenv import load_dotenv
import os
import argparse
from timeit_decorator import timeit
from full_pipeline.full_pipeline_module import FullPipeline

load_dotenv()

@timeit
def main_video_pipeline(video_id,pagePort,video_start_time,video_end_time):
    full_pipeline = FullPipeline(video_id,pagePort,video_start_time,video_end_time)
    full_pipeline.downloadAndGetAudioTranscription()
    print("=== DONE! ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--yolo",default=8081, help="Yolo Port", type=int)
    parser.add_argument("--videoid", help="Video Id",default="YLslsZuEaNE", type=str)
    parser.add_argument("--start_time",default=None, help="Start Time", type=str)
    parser.add_argument("--end_time",default=None, help="End Time", type=str)
    args = parser.parse_args()
    video_id = args.videoid
    pagePort = args.yolo
    video_start_time =  args.start_time
    video_end_time = args.end_time
    if(video_start_time != None and video_end_time != None):
        os.environ['START_TIME'] = video_start_time
        os.environ['END_TIME'] = video_end_time
    main_video_pipeline(video_id,pagePort,video_start_time,video_end_time)
