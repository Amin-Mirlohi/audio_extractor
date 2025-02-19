import os


def ensure_dir(path):
    """Creates the directory if it does not exist."""
    if not os.path.exists(path):
        os.makedirs(path)
