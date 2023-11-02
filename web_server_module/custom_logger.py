import datetime
import logging
from logging.handlers import BaseRotatingHandler

import time

from logging.handlers import TimedRotatingFileHandler

def create_daily_rotating_log(path):
    """
    Creates a daily rotating log
    """
    logger = logging.getLogger("Daily Rotating Log")
    logger.setLevel(logging.INFO)
    
    # Add a timed rotating handler
    handler = TimedRotatingFileHandler(path, when="MIDNIGHT", interval=1, backupCount=5)
    return handler


#----------------------------------------------------------------------


log_filename = 'pipeline_logs/pipeline_{}.log'.format(datetime.datetime.now().strftime("%Y-%m-%d"))
web_server_logger = logging.getLogger('my_logger')
web_server_logger.setLevel(logging.INFO)
log_handler = create_daily_rotating_log(log_filename)
web_server_logger.addHandler(log_handler)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler.setFormatter(formatter)