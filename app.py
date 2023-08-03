import threading
import web
import json
from pipeline_runner import run_pipeline

urls = (
    '/generate_ai_caption', 'PostHandler'
)

app = web.application(urls, globals())

class PostHandler:
    def run_pipeline_background(callback, **kwargs):
        run_pipeline(**kwargs)
        # Add any code you want to execute after run_pipeline has finished
        print("run_pipeline_background finished.")
        # Call the callback function
        callback(**kwargs)
        
    def on_pipeline_completed(**kwargs):
            print("User ID: {}".format(kwargs['user_id']))
            print("User email: {}".format(kwargs['user_email']))
            print("User name: {}".format(kwargs['user_name']))
            # Code to trigger the function outside the PostHandler class
            print("Triggering function outside the PostHandler class.")
            return

    def POST(self):
        data = web.data()
        data_json = json.loads(data)

        if data_json.get('video_id') is None:
            return "You need to provide a video_id"

        user_id = data_json.get('user_id')
        user_email = data_json.get('user_email')
        user_name = data_json.get('user_name')

        # Define a callback function to be executed after the background thread finishes

        # Create a separate thread to run the pipeline in the background
        pipeline_thread = threading.Thread(
            target=self.run_pipeline_background,
            kwargs={
                'callback': self.on_pipeline_completed,  # Pass the callback function
                'video_id': data_json['video_id'],
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
        pipeline_thread.join()

        return "You posted: {}".format(str(data_json))

if __name__ == "__main__":
    app.run() 