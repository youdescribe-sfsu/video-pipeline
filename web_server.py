import datetime
import logging
import os
import threading
import web
import json
from pipeline_module.pipeline_runner import run_pipeline
from web_server_utils import (
    load_pipeline_progress_from_file,
    save_pipeline_progress_to_file,
)

urls = ("/generate_ai_caption", "PostHandler")

app = web.application(urls, globals())


def setup_logger():
    # Get the current date
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    log_file = f"pipeline_api_{current_date}.log"
    log_mode = "a" if os.path.exists(log_file) else "w"
    logger = logging.getLogger(f"PipelineLogger")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler = logging.FileHandler(log_file, mode=log_mode)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


## Port 8086
class PostHandler:
    @staticmethod
    def run_pipeline_background(callback, **kwargs):
        # Extract the user-specific arguments for the callback
        logger = setup_logger()
        user_id = kwargs.pop("user_id")

        run_pipeline(**kwargs)
        # Add any code you want to execute after run_pipeline has finished
        logger.info("run_pipeline_background finished.")
        print("run_pipeline_background finished.")
        # Call the callback function
        callback(user_id=user_id)

    @staticmethod
    def on_pipeline_completed(user_id):
        logger = setup_logger()
        logger.info("Pipeline completed.")
        print("User ID: {}".format(user_id))
        logger.info("User ID: {}".format(user_id))
        # Code to trigger the function outside the PostHandler class
        logger.info("Triggering function outside the PostHandler class.")
        print("Triggering function outside the PostHandler class.")

    def POST(self):
        data = web.data()
        data_json = json.loads(data)
        logger = setup_logger()

        if data_json.get("youtube_id") is None:
            logger.info("You need to provide a youtube_id")
            return "You need to provide a youtube_id"

        user_id = data_json.get("user_id")
        # user_email = data_json.get('user_email')
        # user_name = data_json.get('user_name')
        ydx_server = data_json.get("ydx_server", None)
        ydx_app_host = data_json.get("ydx_app_host", None)
        AI_USER_ID = data_json.get("AI_USER_ID", None)

        print("data :: {}".format(str(data_json)))

        logger.info(
            "User ID: {} called for youtube video :: {}".format(
                user_id, data_json["youtube_id"]
            )
        )

        save_data = load_pipeline_progress_from_file()

        if data_json["youtube_id"] in save_data.keys() and AI_USER_ID in save_data[data_json['youtube_id']].keys():
            ## Already in progress
            save_data[data_json["youtube_id"]][AI_USER_ID].append(
                {
                    "USER_ID": user_id,
                    "AI_USER_ID": AI_USER_ID,
                    "ydx_server": ydx_server,
                    "ydx_app_host": ydx_app_host,
                }
            )
            save_pipeline_progress_to_file(progress_data=save_data)
            logger.info("Pipeline thread finished")
            return "You posted: {}".format(str(data_json))

        # Create a separate thread to run the pipeline in the background
        logger.info("Starting pipeline thread")

        save_data[data_json["youtube_id"]][AI_USER_ID] = [
            {
                "USER_ID": user_id,
                "AI_USER_ID": AI_USER_ID,
                "ydx_server": ydx_server,
                "ydx_app_host": ydx_app_host,
            }
        ]

        pipeline_thread = threading.Thread(
            target=self.run_pipeline_background,
            args=(self.on_pipeline_completed,),  # Pass the callback function as a tuple
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
                "aiUserId": AI_USER_ID,
            },
        )
        logger.info("Starting pipeline thread")
        pipeline_thread.start()

        # Wait for the pipeline_thread to finish using join()
        # pipeline_thread.join()
        logger.info("Pipeline thread finished")
        return "You posted: {}".format(str(data_json))


if __name__ == "__main__":
    app.run()
