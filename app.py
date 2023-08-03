import threading
import web
import json
from pipeline_runner import run_pipeline

urls = (
    '/generate_ai_caption', 'PostHandler'
)

app = web.application(urls, globals())

class PostHandler:
    @staticmethod
    def run_pipeline_background(callback, **kwargs):
        # Extract the user-specific arguments for the callback
        user_id = kwargs.pop('user_id')
        user_email = kwargs.pop('user_email')
        user_name = kwargs.pop('user_name')
        
        run_pipeline(**kwargs)
        # Add any code you want to execute after run_pipeline has finished
        print("run_pipeline_background finished.")
        # Call the callback function
        callback(user_id=user_id, user_email=user_email, user_name=user_name)

    @staticmethod
    def on_pipeline_completed(user_id, user_email, user_name):
        print("User ID: {}".format(user_id))
        print("User email: {}".format(user_email))
        print("User name: {}".format(user_name))
        # Code to trigger the function outside the PostHandler class
        print("Triggering function outside the PostHandler class.")

    def POST(self):
        data = web.data()
        data_json = json.loads(data)

        if data_json.get('youtube_id') is None:
            return "You need to provide a youtube_id"

        user_id = data_json.get('user_id')
        user_email = data_json.get('user_email')
        user_name = data_json.get('user_name')

        # Create a separate thread to run the pipeline in the background
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

        pipeline_thread.start()

        # Wait for the pipeline_thread to finish using join()
        # pipeline_thread.join()

        return "You posted: {}".format(str(data_json))

if __name__ == "__main__":
    app.run()
