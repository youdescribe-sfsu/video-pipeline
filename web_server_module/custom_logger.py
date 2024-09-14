import datetime
import logging
from logging.handlers import TimedRotatingFileHandler
import os


def create_daily_rotating_log(path):
    """
    Creates a daily rotating log
    """
    logger = logging.getLogger("Daily Rotating Log")
    logger.setLevel(logging.INFO)

    # Add a timed rotating handler
    handler = TimedRotatingFileHandler(path, when="MIDNIGHT", interval=1, backupCount=5)
    return handler


def setup_logger():
    """
    Sets up and returns a configured logger
    """
    # Define the path to the log file relative to the parent directory
    log_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'pipeline_logs')

    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    log_filename = os.path.join(log_directory, f'pipeline_{datetime.datetime.now().strftime("%Y-%m-%d")}.log')

    logger = logging.getLogger('web_server_logger')
    logger.setLevel(logging.INFO)

    log_handler = create_daily_rotating_log(log_filename)
    logger.addHandler(log_handler)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    log_handler.setFormatter(formatter)

    return logger