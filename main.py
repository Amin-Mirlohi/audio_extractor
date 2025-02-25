
import os
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

    # ----------------------------------------------------------------------------
    # 1. Gather all already-processed audio keys from the BUCKET_OUTPUT.
    #    We'll store them in a set so we can skip re-processing.
    # ----------------------------------------------------------------------------
    processed_audio_keys = set()
    try:
        paginator_out = s3_client.get_paginator("list_objects_v2")
        page_iterator_out = paginator_out.paginate(Bucket=BUCKET_OUTPUT)
        for page_out in page_iterator_out:
            # If the bucket is empty or page is empty, skip
            if "Contents" not in page_out:
                continue
            for obj_out in page_out["Contents"]:
                # Add the existing audio file name (in lower case) to the set
                processed_audio_keys.add(obj_out["Key"].lower())

        app_logger.info(
            f"Found {len(processed_audio_keys)} files already in {BUCKET_OUTPUT}."
        )
    except ClientError as e:
        app_logger.error(f"Error listing objects in output bucket: {e}")
    # ----------------------------------------------------------------------------

    # Use a paginator to list objects in pages from the input bucket
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=BUCKET_INPUT)

    page_count = 0
    for page in page_iterator:
        page_count += 1
        app_logger.info(f"Processing page #{page_count}")

        # If there are no objects (or this page is empty), skip
        if "Contents" not in page:
            app_logger.info(f"No objects found in page #{page_count}. Skipping...")
            continue

        # Iterate over all objects in the current page
        for item in page["Contents"]:
            object_key = item["Key"]

            # Skip if the file is not an MP4
            if not object_key.lower().endswith(".mp4"):
                app_logger.info(f"Skipping non-MP4 file: {object_key}")
                continue

            base_name = os.path.splitext(os.path.basename(object_key))[0]
            local_audio_filename = base_name + ".m4a"
            audio_object_key = local_audio_filename  # The key we'll use in BUCKET_OUTPUT

            # ----------------------------------------------------------------------------
            # 2. Check if this audio file has already been processed (exists in BUCKET_OUTPUT)
            # ----------------------------------------------------------------------------
            if audio_object_key.lower() in processed_audio_keys:
                app_logger.info(
                    f"Audio for {object_key} (would be '{audio_object_key}') "
                    f"already exists in {BUCKET_OUTPUT}. Skipping..."
                )
                continue

            app_logger.info(f"Processing video file: {object_key}")

            # 1) Download the file
            local_video_path = download_video_from_s3(BUCKET_INPUT, object_key, app_logger)
            if not local_video_path:
                # download_video_from_s3 already logged the error
                failures_logger.error(f"DOWNLOAD_FAILED: {object_key}")
                continue

            # 2) Extract audio
            local_audio_path = os.path.join(LOCAL_TEMP_DIR, local_audio_filename)
            try:
                extract_audio(local_video_path, local_audio_path, app_logger)
            except Exception as e:
                app_logger.exception(f"Audio extraction failed for {object_key}")
                failures_logger.error(f"EXTRACTION_FAILED: {object_key}")
                # Clean up the downloaded video before continuing
                if os.path.exists(local_video_path):
                    os.remove(local_video_path)
                continue

            # 3) Upload audio to output S3 bucket
            uploaded = upload_audio_to_s3(
                local_audio_path, BUCKET_OUTPUT, audio_object_key, app_logger
            )
            if not uploaded:
                failures_logger.error(f"UPLOAD_FAILED: {object_key}")
            else:
                # If uploaded successfully, add it to processed_audio_keys so we
                # won't process it again if the script runs multiple times.
                processed_audio_keys.add(audio_object_key.lower())

            # 4) Clean up local files to free space
            if os.path.exists(local_video_path):
                os.remove(local_video_path)
            if os.path.exists(local_audio_path):
                os.remove(local_audio_path)

    app_logger.info("Processing complete.")


if __name__ == "__main__":
    main()