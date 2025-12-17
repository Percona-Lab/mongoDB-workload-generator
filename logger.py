# logger.py
import logging
import sys

def configure_logging(log_file=None, level=logging.INFO):
    """
    Configures logging to stream to stdout and optionally to a file.

    Args:
        log_file (str, optional): Path to the log file. Defaults to None.
        level (int, optional): The logging level to set. Defaults to logging.INFO.
    """
    handlers = [logging.StreamHandler(sys.stdout)]
    
    if log_file:
        handlers.append(logging.FileHandler(log_file, mode="a"))

    # Use the 'level' parameter to configure the root logger
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
        force=True 
    )

    # ---- HIDE PYMONGO DEBUG MESSAGES ----
    # After setting our app's level, raise the level for the noisy
    # pymongo library to WARNING, so we only see its serious errors.
    logging.getLogger("pymongo").setLevel(logging.WARNING)
    # ----------------------------------------------------    