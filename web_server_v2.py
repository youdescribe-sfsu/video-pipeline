from fastapi import FastAPI
from dotenv import load_dotenv
load_dotenv()
import queue
from contextlib import asynccontextmanager

import uvicorn
from pipeline_module.generate_YDX_caption_submodule.generate_ydx_caption import GenerateYDXCaption
from pipeline_module.pipeline_runner import run_pipeline
from web_server_module.web_server_database import create_database, get_data_for_youtube_id_ai_user_id, get_data_for_youtube_id_and_user_id, get_pending_jobs_with_youtube_ids, update_ai_user_data, update_status,process_incoming_data,StatusEnum
from web_server_module.web_server_types import WebServerRequest
from web_server_module.custom_logger import web_server_logger
import asyncio
from fastapi.encoders import jsonable_encoder
task_queue = queue.Queue()
import signal
import json
running_tasks = []
async def cleanup_tasks():
    for task in running_tasks:
        task.cancel()
    await asyncio.gather(*running_tasks, return_exceptions=True)


# Function to update the queue with new requests every 30 minutes
async def update_queue_periodically():
    while True:
        # Fetch new requests and add them to the queue
        pending_jobs_with_youtube_ids = get_pending_jobs_with_youtube_ids()
        for data in pending_jobs_with_youtube_ids:
            web_server_logger.info("Adding youtube_id: {}, ai_user_id: {} to queue".format(data['youtube_id'], data['ai_user_id']))
            task_queue.put((data['youtube_id'], data['ai_user_id']))
        
        # Wait for 30 minutes before the next update
        await asyncio.sleep(1800)  # 30 minutes = 1800 seconds
        
        
# Function to process requests from the queue
async def process_queue():
    web_server_logger.info("Starting to process queue")
    while True:
        # Check if the queue has any items to process
        if not task_queue.empty():
            youtube_id, ai_user_id = task_queue.get()
            ydx_server, ydx_app_host = get_data_for_youtube_id_ai_user_id(youtube_id, ai_user_id)
            web_server_logger.info("Processing request for youtube_id: {}, ai_user_id: {}".format(youtube_id, ai_user_id))
            run_pipeline(
                video_id=youtube_id,
                video_end_time=None,
                video_start_time=None,
                upload_to_server=False,
                multi_thread=False,
                tasks=None,
                ydx_server=ydx_server,
                ydx_app_host=ydx_app_host,
                userId=None,
                AI_USER_ID=ai_user_id,
            )
            # Process the request
            update_status(youtube_id, ai_user_id, StatusEnum.done.value)
            
            video_runner_obj={
                "video_id": youtube_id,
                "logger": web_server_logger
            }            
            generate_YDX_caption = GenerateYDXCaption(video_runner_obj=video_runner_obj)
            
            
            user_data = get_data_for_youtube_id_and_user_id(youtube_id, ai_user_id)

            for data in user_data:
                generate_YDX_caption.generateYDXCaption(
                    ydx_server=data.get("ydx_server", None),
                    ydx_app_host=data.get("ydx_app_host", None),
                    userId=data.get("user_id", None),
                    AI_USER_ID=data.get("ai_user_id", None),
                    logger=web_server_logger,
                )
                update_ai_user_data(
                    youtube_id=youtube_id,
                    ai_user_id=ai_user_id,
                    status=StatusEnum.done.value,
                )
                        
        else:
            web_server_logger.info("Queue is empty")
            # If queue is empty, wait before checking again
            await asyncio.sleep(900)  # Check every 15 minutes

@asynccontextmanager
async def lifespan(app: FastAPI):
    
    create_database()
    asyncio.create_task(update_queue_periodically())
    asyncio.create_task(process_queue())
    signal.signal(signal.SIGINT, lambda s, f: asyncio.create_task(cleanup_tasks()))
    try:
        yield
    finally:
        await cleanup_tasks()
    # for task in asyncio.Task.all_tasks():
    #     web_server_logger.info("Cancelling task :: {}".format(str(task)))
    #     task.cancel()

app = FastAPI(lifespan=lifespan)

@app.post("/generate_ai_caption")
async def generate_ai_caption(post_data: WebServerRequest):
    try:
        data_json = json.loads(post_data.model_dump_json())
        print("data_json :: {}".format((data_json)))

        user_id = data_json['user_id']
        ydx_server = data_json['ydx_server']
        ydx_app_host = data_json['ydx_app_host']
        ai_user_id = data_json['AI_USER_ID']
        youtube_id = data_json['youtube_id']
        
        
        web_server_logger.info("data_json :: {}".format(str(data_json)))
        web_server_logger.info("youtube_id :: {}".format(str(youtube_id)))
        
        web_server_logger.info(
            "User ID: {} called for youtube video :: {}".format(
                user_id, youtube_id
            )
        )
        print(
            "User ID: {} called for youtube video :: {}".format(
                user_id, youtube_id
            )
        )
        process_incoming_data(user_id, ydx_server, ydx_app_host, ai_user_id, youtube_id)
        task_queue.put((youtube_id, ai_user_id))
        
        return "You posted: {}".format(str(data_json))
        
        
    except Exception as e:
        print(e)
        print("Exception :: {}".format(str(e)))
        web_server_logger.error("Exception :: {}".format(str(e)))
        return "error"
    

if __name__ == "__main__":
    uvicorn.run("web_server_v2:app", host="0.0.0.0", port=8086,reload=True)
    
    ## To Kill
    # ps aux | grep web_server_v2:app
    # kill -s KILL -- PID