

import os
import threading
import json
from fastapi import FastAPI
from dotenv import load_dotenv
from pipeline_module.pipeline_runner import run_pipeline
from pipeline_module.generate_YDX_caption_submodule.generate_ydx_caption import GenerateYDXCaption
from web_server_module.web_server_utils import load_pipeline_progress_from_file, save_pipeline_progress_to_file
from web_server_module.custom_logger import web_server_logger as logger
from contextlib import asynccontextmanager

load_dotenv()

# @asynccontextmanager
# async def lifespan(app: FastAPI):
    # Load the ML model
    


# app = FastAPI(lifespan=lifespan)


app = FastAPI()

class PostHandler:
    @staticmethod
    def run_pipeline_background(user_id):
        # Extract the user-specific arguments for the callback

        run_pipeline(**kwargs)
        # Add any code you want to execute after run_pipeline has finished
        logger.info("run_pipeline_background finished.")
        print("run_pipeline_background finished.")
        # Call the callback function
        PostHandler.on_pipeline_completed(user_id)

    @staticmethod
    def on_pipeline_completed(user_id):
        logger.info("Pipeline completed.")
        print("User ID: {}".format(user_id))
        logger.info("User ID: {}".format(user_id))
        # Code to trigger the function outside the PostHandler class
        logger.info("Triggering function outside the PostHandler class.")
        print("Triggering function outside the PostHandler class.")

@app.post("/generate_ai_caption")
async def generate_ai_caption(data_json: dict):
    try:

        logger.info("data_json :: {}".format(str(data_json)))
        logger.info("youtube_id :: {}".format(str(data_json.get("youtube_id", None))))

        if data_json.get("youtube_id") is None:
            logger.info("You need to provide a youtube_id")
            return "You need to provide a youtube_id"

        user_id = data_json.get("user_id")
        ydx_server = data_json.get("ydx_server", None)
        ydx_app_host = data_json.get("ydx_app_host", None)
        AI_USER_ID = data_json.get("AI_USER_ID", None)

        logger.info(
            "User ID: {} called for youtube video :: {}".format(
                user_id, data_json["youtube_id"]
            )
        )

        save_data = load_pipeline_progress_from_file()
        youtube_id = data_json["youtube_id"]

        if youtube_id in save_data.keys():
            if "data" in save_data[youtube_id]:
                if AI_USER_ID in save_data[youtube_id]["data"]:
                    # Entry already in progress, check if AI_USER_ID exists
                    if not any(entry['AI_USER_ID'] == AI_USER_ID for entry in save_data[youtube_id]["data"][AI_USER_ID]):
                        save_data[youtube_id]["data"][AI_USER_ID].append({
                            "user_id": data_json["user_id"],
                            "AI_USER_ID": AI_USER_ID,
                            "ydx_server": data_json["ydx_server"],
                            "ydx_app_host": data_json["ydx_app_host"],
                        })
                        save_pipeline_progress_to_file(save_data)
                        logger.info("Pipeline thread finished")
                        print("You posted: {}".format(json.dumps(data_json)))
                    else:
                        logger.info("In else of web_server.py :: line 105")
                        print("In else of web_server.py :: line 105")
                else:
                    # Initialize the data dictionary for AI_USER_ID
                    save_data[youtube_id]["data"][AI_USER_ID] = [{
                        "user_id": data_json["user_id"],
                        "AI_USER_ID": AI_USER_ID,
                        "ydx_server": data_json["ydx_server"],
                        "ydx_app_host": data_json["ydx_app_host"],
                    }]
                    save_pipeline_progress_to_file(save_data)
            else:
                # Create a new entry for youtube_id
                save_data[youtube_id]["data"] = {
                    AI_USER_ID: [{
                        "user_id": data_json["user_id"],
                        "AI_USER_ID": AI_USER_ID,
                        "ydx_server": data_json["ydx_server"],
                        "ydx_app_host": data_json["ydx_app_host"],
                    }]
                }
                save_pipeline_progress_to_file(save_data)
        else:
            # Create a new entry for youtube_id
            save_data[youtube_id] = {
                "data": {
                    AI_USER_ID: [{
                        "user_id": data_json["user_id"],
                        "AI_USER_ID": AI_USER_ID,
                        "ydx_server": data_json["ydx_server"],
                        "ydx_app_host": data_json["ydx_app_host"],
                        "status": "in_progress"
                    }]
                },
                "status": "in_progress"
            }
            save_pipeline_progress_to_file(save_data)
            logger.info("Starting pipeline thread")

            pipeline_thread = threading.Thread(
                target=PostHandler.run_pipeline_background,
                args=(user_id,),  # Pass the user_id as an argument
                kwargs={
                    "video_id": data_json["youtube_id"],
                    "video_start_time": data_json.get("video_start_time", None),
                    "video_end_time": data_json.get("video_end_time", None),
                    "upload_to_server": data_json.get("upload_to_server", True),
                    "multi_thread": data_json.get("multi_thread", False),
                    "tasks": data_json.get("tasks", None),
                    "ydx_server": ydx_server,
                    "ydx_app_host": ydx_app_host,
                    "user_id": user_id,
                    "AI_USER_ID": AI_USER_ID,
                },
            )
            logger.info("Starting pipeline thread")
            pipeline_thread.start()

        return "You posted: {}".format(str(data_json))
    except Exception as e:
        logger.info(f"Error Running Pipeline : {e}")
        print(f"Error Running Pipeline : {e}")
        return f"Error Running Pipeline : {e}"

if __name__ == "__main__":
    save_data = load_pipeline_progress_from_file()

    for video_id in save_data.keys():
        video_data = save_data[video_id]

        # Check if the status for the current video ID is "done"
        if video_data["status"] == "done":
            print(f"Processing video ID: {video_id}")
            logger.info(f"Processing video ID: {video_id}")

            # Iterate through the AI users' data for the current video ID
            for ai_user_id, objects in video_data["data"].items():
                video_runner_obj = {
                    "video_id": video_id,
                    "logger": logger,
                }
                generate_YDX_caption = GenerateYDXCaption(video_runner_obj=video_runner_obj)
                for obj in objects:
                    # Check if the status for the current object is "in_progress"
                    if obj["status"] == "in_progress":
                        # Your code to process this object goes here
                        logger.info(f"Processing object for AI user {ai_user_id}")
                        generate_YDX_caption.generateYDXCaption(
                            ydx_server=obj.get("ydx_server", None),
                            ydx_app_host=obj.get("ydx_app_host", None),
                            userId=obj.get("user_id", None),
                            AI_USER_ID=obj.get("AI_USER_ID", None),
                            logger=logger,
                        )

                        # Mark the object as "done"
                        obj["status"] = "done"

            # Optionally, update the JSON data if needed

        else:
            posthandler = PostHandler()
            ## select first AI user id
            AI_USER_ID = list(video_data["data"].keys())[0]

            pipeline_thread = threading.Thread(
                target=posthandler.run_pipeline_background,
                args=(AI_USER_ID,),  # Pass the AI_USER_ID as an argument
                kwargs={
                    "video_id": video_id,
                    "video_start_time": None,
                    "video_end_time": None,
                    "upload_to_server": True,
                    "multi_thread": False,
                    "tasks": None,
                    "ydx_server": video_data['data'][AI_USER_ID][0]['ydx_server'],
                    "ydx_app_host": video_data['data'][AI_USER_ID][0]['ydx_app_host'],
                    "user_id": video_data['data'][AI_USER_ID][0]['user_id'],
                    "AI_USER_ID": AI_USER_ID,
                },
            )
            logger.info("Starting pipeline thread")
            pipeline_thread.start()

    # import uvicorn

    # # Use uvicorn to run the FastAPI app
    # uvicorn.run(app, host="0.0.0.0", port=8000)
