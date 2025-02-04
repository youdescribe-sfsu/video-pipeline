# queue_config.py
###############################################################################
# Central configuration file for Redis queues
# This ensures we have a single source of truth for queue definitions
###############################################################################

from redis import Redis
from rq import Queue
import logging

logger = logging.getLogger(__name__)

# Redis connection setup
redis_conn = Redis(host="localhost", port=6379)

# Define our queues once, to be imported by other modules
global_task_queue = Queue('video_tasks', connection=redis_conn)
caption_queue = Queue('caption_tasks', connection=redis_conn)

def get_queue_for_task(task_type: str) -> Queue:
    """Helper function to get the appropriate queue for a task type"""
    queue = caption_queue if task_type == "image_captioning" else global_task_queue
    logger.info(f"Routing task {task_type} to queue {queue.name}")
    return queue