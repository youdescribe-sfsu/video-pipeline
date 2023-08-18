import logging

from web_server_utils import load_pipeline_progress_from_file
from ..generate_YDX_caption_submodule.generate_ydx_caption import GenerateYDXCaption

def run_generate_ydx_caption(video_id, aiUserId):
    video_runner_obj={
        "video_id": video_id,
        "logger": logging.getLogger(f"PipelineLogger")
    }
    generate_YDX_caption = GenerateYDXCaption(video_runner_obj=video_runner_obj)
    save_data = load_pipeline_progress_from_file()
    if save_data is None:
        print("run_generate_ydx_caption :: No data found")
        logging.info("run_generate_ydx_caption :: No data found")
        return
    if video_id not in save_data.keys():
        print("run_generate_ydx_caption :: Video ID not found")
        logging.info("run_generate_ydx_caption :: Video ID not found")
        return
    if aiUserId not in save_data[video_id].keys():
        print("run_generate_ydx_caption :: AI User ID not found")
        logging.info("run_generate_ydx_caption :: AI User ID not found")
        return
    for obj in save_data[video_id][aiUserId]:
        generate_YDX_caption.generateYDXCaption(
            ydx_server=obj.get("ydx_server", None),
            ydx_app_host=obj.get("ydx_app_host", None),
            userId=obj.get("USER_ID", None),
            aiUserId=obj.get("AI_USER_ID", None),
        )
    return