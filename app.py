import web
import json
from pipeline_runner import run_pipeline

urls = (
    '/post', 'PostHandler'
)

app = web.application(urls, globals())

class PostHandler:
    def POST(self):
        data = web.data()
        # data_dict = dict(data)  # Convert the Storage object to a dictionary
        print(data)
        data_json = json.loads(data)
        print("data_json", data_json)
        if(data_json.get('video_id', None) is None):
            return "You need to provide a video_id"
        # run_pipeline(
        #     video_id=data_json['video_id'],
        #     video_start_time=data_json.get('video_start_time', None),
        #     video_end_time=data_json.get('video_end_time', None),
        #     upload_to_server=data_json.get('upload_to_server', True),
        #     tasks=data_json.get('tasks', None)
        # )
        
        return "You posted: {}".format(str(data_json))

if __name__ == "__main__":
    app.run()