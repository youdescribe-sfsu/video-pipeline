import datetime
import logging
from logging.handlers import BaseRotatingHandler

class CustomDateRotatingFileHandler(BaseRotatingHandler):
    def __init__(self, filename, mode='a', maxBytes=0, backupCount=0, encoding=None, delay=0):
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        filename = f"{filename}_{current_date}.log"
        super().__init__(filename, mode, maxBytes, backupCount, encoding, delay)

# Configure the logging
log_filename = 'pipeline_logs/pipeline_api'
log_handler = CustomDateRotatingFileHandler(log_filename, mode='a', maxBytes=1024*1024, backupCount=3)

# Create a logger
web_server_logger = logging.getLogger('my_logger')
web_server_logger.setLevel(logging.INFO)


# Add the handler to the logger
web_server_logger.addHandler(log_handler)

# Create a formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler.setFormatter(formatter)

