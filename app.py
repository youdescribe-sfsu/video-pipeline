import argparse
from pipeline_module.pipeline_runner import run_pipeline
from pipeline_module.generate_YDX_caption_submodule.generate_ydx_caption import GenerateYDXCaption
from dotenv import load_dotenv
load_dotenv()



if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--video_id", help="Video Id", type=str)
    parser.add_argument(
        "--upload_to_server", help="Upload To YDX Server", action="store_true"
    )
    parser.add_argument("--start_time", default=None, help="Start Time", type=str)
    parser.add_argument("--end_time", default=None, help="End Time", type=str)
    args = parser.parse_args()
    video_id = args.video_id
    video_start_time = args.start_time
    video_end_time = args.end_time
    upload_to_server = args.upload_to_server
    run_pipeline(video_id, video_start_time, video_end_time, upload_to_server)