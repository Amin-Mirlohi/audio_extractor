import os
import subprocess

import ffmpeg


def extract_audio(input_file_path, output_file_path, logger):
    """
    Extract the audio stream from input_file_path and save to output_file_path.
    If you prefer using the subprocess approach, uncomment the subprocess example and comment out ffmpeg-python lines.
    """

    logger.info(f"Extracting audio from {input_file_path}...")

    # Example using ffmpeg-python:
    try:
        (
            ffmpeg.input(input_file_path)
            .output(
                output_file_path, vn=None, acodec="copy"
            )  # "vn" to disable video, "acodec=copy" to avoid re-encoding
            .overwrite_output()
            .run()
        )
        logger.info(f"Audio extracted successfully: {output_file_path}")
    except ffmpeg.Error as e:
        logger.error(f"ffmpeg-python error: {e.stderr.decode()}")
        raise

    # Alternatively, using subprocess:
    # command = [
    #     "ffmpeg", "-i", input_file_path,
    #     "-vn", "-acodec", "copy",
    #     "-y",  # overwrite
    #     output_file_path
    # ]
    # try:
    #     subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #     logger.info(f"Audio extracted successfully: {output_file_path}")
    # except subprocess.CalledProcessError as e:
    #     logger.error(f"Failed to run ffmpeg: {e.stderr}")
    #     raise

    # You can pick which approach (subprocess or ffmpeg-python) you prefer.

    # Finally, return the path for further use if needed
    return output_file_path
