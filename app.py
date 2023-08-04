import datetime
import logging
import os
import threading
import web
import json
from pipeline_runner import run_pipeline

urls = (
    '/generate_ai_caption', 'PostHandler'
)

app = web.application(urls, globals())

def setup_logger():
    # Get the current date
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    log_file = f"pipeline_api_{current_date}.log"
    log_mode = 'a' if os.path.exists(log_file) else 'w'
    logger = logging.getLogger(f"PipelineLogger")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler = logging.FileHandler(log_file, mode=log_mode)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger

class PostHandler:
    
    @staticmethod
    def run_pipeline_background(callback, **kwargs):
        # Extract the user-specific arguments for the callback
        logger = setup_logger()
        user_id = kwargs.pop('user_id')
        user_email = kwargs.pop('user_email')
        user_name = kwargs.pop('user_name')
        
        run_pipeline(**kwargs)
        # Add any code you want to execute after run_pipeline has finished
        logger.info("run_pipeline_background finished.")
        print("run_pipeline_background finished.")
        # Call the callback function
        callback(user_id=user_id, user_email=user_email, user_name=user_name)

    @staticmethod
    def on_pipeline_completed(user_id, user_email, user_name):
        logger = setup_logger()
        logger.info("Pipeline completed.")
        print("User ID: {}".format(user_id))
        print("User email: {}".format(user_email))
        print("User name: {}".format(user_name))
        logger.info("User ID: {}".format(user_id))
        logger.info("User email: {}".format(user_email))
        logger.info("User name: {}".format(user_name))
        # Code to trigger the function outside the PostHandler class
        logger.info("Triggering function outside the PostHandler class.")
        print("Triggering function outside the PostHandler class.")

    def POST(self):
        data = web.data()
        data_json = json.loads(data)
        logger = setup_logger()

        if data_json.get('youtube_id') is None:
            logger.info("You need to provide a youtube_id")
            return "You need to provide a youtube_id"

        user_id = data_json.get('user_id')
        user_email = data_json.get('user_email')
        user_name = data_json.get('user_name')
        
        logger.info("User ID: {} called for youtube video :: {}".format(user_id,data_json['youtube_id']))

        # Create a separate thread to run the pipeline in the background
        logger.info("Starting pipeline thread")
        pipeline_thread = threading.Thread(
            target=self.run_pipeline_background,
            args=(self.on_pipeline_completed,),  # Pass the callback function as a tuple
            kwargs={
                'video_id': data_json['youtube_id'],
                'video_start_time': data_json.get('video_start_time', None),
                'video_end_time': data_json.get('video_end_time', None),
                'upload_to_server': data_json.get('upload_to_server', True),
                'tasks': data_json.get('tasks', None),
                'user_id': user_id,
                'user_email': user_email,
                'user_name': user_name
            }
        )
        logger.info("Starting pipeline thread")
        pipeline_thread.start()

        # Wait for the pipeline_thread to finish using join()
        # pipeline_thread.join()
        logger.info("Pipeline thread finished")
        return "You posted: {}".format(str(data_json))

if __name__ == "__main__":
    app.run()
