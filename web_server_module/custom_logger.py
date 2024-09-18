import logging
import sys


def setup_logger():
    """
    Sets up and returns a configured logger with console output only
    """
    logger = logging.getLogger('web_server_logger')
    logger.setLevel(logging.INFO)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)

    # Formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(console_handler)

    return logger