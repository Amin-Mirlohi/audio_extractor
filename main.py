import os
import shutil
from datetime import datetime  # <-- ADDED

import boto3
from botocore.exceptions import ClientError

# Local imports
from config import BUCKET_INPUT, BUCKET_OUTPUT, LOCAL_TEMP_DIR, REGION_NAME
from utils.logger import setup_loggers
from video_processor.audio_extractor import extract_audio
from video_processor.downloader import download_video_from_s3
from video_processor.uploader import upload_audio_to_s3


def main():
    # Set up loggers
    app_logger, failures_logger = setup_loggers()

    # Ensure local temp directory exists
    if not os.path.exists(LOCAL_TEMP_DIR):
        os.makedirs(LOCAL_TEMP_DIR)

    # Create an S3 client
    s3_client = boto3.client("s3", region_name=REGION_NAME)

    # List objects (videos) from input bucket
    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET_INPUT)
        if "Contents" not in response:
            app_logger.warning(f"No objects found in bucket {BUCKET_INPUT}. Exiting...")
            return
    except ClientError as e:
        app_logger.error(f"Failed to list objects in {BUCKET_INPUT}: {e}")
        return

    # Iterate over all objects in the bucket
    for item in response["Contents"]:
        object_key = item["Key"]

        # You might want to skip if object_key is not .mp4:
        if not object_key.lower().endswith(".mp4"):
            app_logger.info(f"Skipping non-MP4 file: {object_key}")
            continue

        app_logger.info(f"Processing video file: {object_key}")

        # Write a timestamp entry for when processing starts
        with open("time.txt", "a") as time_file:
            time_file.write(f"{datetime.now().isoformat()} - START: {object_key}\n")

        # 1) Download the file
        local_video_path = download_video_from_s3(BUCKET_INPUT, object_key, app_logger)
        if not local_video_path:
            # download_video_from_s3 already logged the error
            failures_logger.error(f"DOWNLOAD_FAILED: {object_key}")
            continue

        # 2) Extract audio
        base_name = os.path.splitext(os.path.basename(object_key))[0]
        local_audio_filename = base_name + ".m4a"
        local_audio_path = os.path.join(LOCAL_TEMP_DIR, local_audio_filename)

        try:
            extract_audio(local_video_path, local_audio_path, app_logger)
        except Exception as e:
            app_logger.exception(f"Audio extraction failed for {object_key}")
            failures_logger.error(f"EXTRACTION_FAILED: {object_key}")
            continue

        # 3) Upload audio to output S3 bucket
        audio_object_key = local_audio_filename
        uploaded = upload_audio_to_s3(
            local_audio_path, BUCKET_OUTPUT, audio_object_key, app_logger
        )
        if not uploaded:
            failures_logger.error(f"UPLOAD_FAILED: {object_key}")

        # 4) Clean up local files to free space
        if os.path.exists(local_video_path):
            os.remove(local_video_path)
        if os.path.exists(local_audio_path):
            os.remove(local_audio_path)

        # Write a timestamp entry for when processing ends
        with open("time.txt", "a") as time_file:
            time_file.write(f"{datetime.now().isoformat()} - END: {object_key}\n")

    app_logger.info("Processing complete.")


if __name__ == "__main__":
    main()
