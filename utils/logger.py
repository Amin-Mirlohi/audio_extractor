import logging
import os

from config import LOG_DIR


def setup_loggers():
    """
    Configures two loggers:
      1) app_logger: general application logging
      2) failures_logger: logs the filenames that fail to process
    Returns (app_logger, failures_logger) so they can be used in the rest of the app.
    """

    # Ensure the log directory exists
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    # Create the general application logger
    app_logger = logging.getLogger("app_logger")
    app_logger.setLevel(logging.INFO)

    # Create the file handler for the main application log
    app_log_file = os.path.join(LOG_DIR, "app.log")
    fh_app = logging.FileHandler(app_log_file)
    fh_app.setLevel(logging.INFO)

    # Create console handler for convenience (optional)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # Define a standard log format
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    fh_app.setFormatter(formatter)
    ch.setFormatter(formatter)

    # Attach handlers to the application logger
    app_logger.addHandler(fh_app)
    app_logger.addHandler(ch)

    # Create a separate logger for failures
    failures_logger = logging.getLogger("failures_logger")
    failures_logger.setLevel(logging.ERROR)

    # Create the file handler for failures
    failures_log_file = os.path.join(LOG_DIR, "failed_files.log")
    fh_failures = logging.FileHandler(failures_log_file)
    fh_failures.setLevel(logging.ERROR)
    fh_failures.setFormatter(formatter)

    # Attach handler to failures logger
    failures_logger.addHandler(fh_failures)

    # Return both loggers
    return app_logger, failures_logger
