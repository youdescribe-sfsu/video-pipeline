from fastapi import FastAPI, BackgroundTasks
import asyncio
import queue
import threading
from contextlib import asynccontextmanager
from web_server_module.web_server_database import create_database, get_pending_jobs_with_youtube_ids, get_status_for_youtube_id,process_incoming_data
from web_server_module.web_server_types import WebServerRequest
from web_server_module.custom_logger import web_server_logger

task_queue = queue.Queue()

@asynccontextmanager
async def lifespan(app: FastAPI):

    create_database()
    pending_jobs_with_youtube_ids = get_pending_jobs_with_youtube_ids()
    ## Add the pending jobs to the queue
    for youtube_id, ai_user_id in pending_jobs_with_youtube_ids:
        task_queue.put((youtube_id, ai_user_id))

app = FastAPI(lifespan=lifespan)



# app = FastAPI()



# def process_video(video_id, ai_user_id):
#     # Check if the status for the current video ID is "done"
    
#     status = get_status_for_youtube_id(video_id, ai_user_id)
    
#     if status == "pending":
#         print(f"Processing video ID: {video_id}")
#         web_server_logger.info(f"Processing video ID: {video_id}")

#         # Iterate through the AI users' data for the current video ID
#         for ai_user_id, objects in video_data["data"].items():
#             for obj in objects:
#                 # Check if the status for the current object is "in_progress"
#                 if obj["status"] == "in_progress":
#                     logger.info(f"Processing object for AI user {ai_user_id}")

#                     # Enqueue the task to process this object
#                     task_queue.put((obj, logger))

#                     # Mark the object as "done"
#                     obj["status"] = "done"
#     else:
#         # Select the first AI user ID
#         AI_USER_ID = list(video_data["data"].keys())[0]

#         # Enqueue the task to run the pipeline
#         task_queue.put((AI_USER_ID, video_id, video_data, logger))

# def process_objects():
#     while True:
#         item = task_queue.get()
#         if isinstance(item, tuple):
#             # Task to process an object
#             obj, logger = item
#             generate_YDX_caption = GenerateYDXCaption(video_runner_obj={"video_id": None, "logger": logger})
#             generate_YDX_caption.generateYDXCaption(
#                 ydx_server=obj.get("ydx_server", None),
#                 ydx_app_host=obj.get("ydx_app_host", None),
#                 userId=obj.get("user_id", None),
#                 AI_USER_ID=obj.get("AI_USER_ID", None),
#                 logger=logger,
#             )
#         elif isinstance(item, str):
#             # Task to run the pipeline
#             AI_USER_ID, video_id, video_data, logger = item
#             posthandler = PostHandler()
#             pipeline_thread = threading.Thread(
#                 target=posthandler.run_pipeline_background,
#                 args=(AI_USER_ID,),
#                 kwargs={
#                     "video_id": video_id,
#                     "video_start_time": None,
#                     "video_end_time": None,
#                     "upload_to_server": True,
#                     "multi_thread": False,
#                     "tasks": None,
#                     "ydx_server": video_data['data'][AI_USER_ID][0]['ydx_server'],
#                     "ydx_app_host": video_data['data'][AI_USER_ID][0]['ydx_app_host'],
#                     "user_id": video_data['data'][AI_USER_ID][0]['user_id'],
#                     "AI_USER_ID": AI_USER_ID,
#                 },
#             )
#             logger.info("Starting pipeline thread")
#             pipeline_thread.start()
#         task_queue.task_done()

# # Create a thread to process objects from the queue
# object_processing_thread = threading.Thread(target=process_objects)
# object_processing_thread.start()





# async def background_function():
#     while not request_queue.empty():
#         request = request_queue.get()
#         # Process the request here (replace with your processing logic)
#         print(f"Processing request: {request}")
#         await asyncio.sleep(1)  # Simulate processing time

# @app.post("/add_request/")
# async def add_request(request_data: str, background_tasks: BackgroundTasks):
#     request_queue.put(request_data)
#     background_tasks.add_task(background_function)
#     return {"message": "Request added to the queue"}



@app.post("/generate_ai_caption",response_model=str)
async def generate_ai_caption(post_data: WebServerRequest):
    try:
        data_json = post_data.model_dump()
        
        user_id = data_json.get("user_id")
        ydx_server = data_json.get("ydx_server", None)
        ydx_app_host = data_json.get("ydx_app_host", None)
        ai_user_id = data_json.get("AI_USER_ID", None)
        youtube_id = data_json.get("youtube_id", None)
        
        
        web_server_logger.info("data_json :: {}".format(str(data_json)))
        web_server_logger.info("youtube_id :: {}".format(str(youtube_id)))
        
        web_server_logger.info(
            "User ID: {} called for youtube video :: {}".format(
                user_id, youtube_id
            )
        )
        process_incoming_data(user_id, ydx_server, ydx_app_host, ai_user_id, youtube_id)
        
        
    except Exception as e:
        web_server_logger.error("Exception :: {}".format(str(e)))
    