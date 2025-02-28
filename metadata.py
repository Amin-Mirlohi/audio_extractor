import os
import json
import boto3
from botocore.exceptions import ClientError
from pymediainfo import MediaInfo

# Existing input bucket name
INPUT_BUCKET_NAME = "damodaran-youtube-videos"  # Keep as-is
REGION_NAME = "us-east-1"

# This is just a local temp folder for downloading videos before processing
LOCAL_TEMP_DIR = "/tmp/video_metadata"

# New output bucket for storing metadata
METADATA_BUCKET_NAME = "damodaran-vidoes-metadata"

def download_video_from_s3(bucket_name, object_key, local_dir):
    """
    Download a video file from S3 to a local directory.
    Return the local file path if successful, None if failed.
    """
    s3_client = boto3.client("s3", region_name=REGION_NAME)
    local_filename = os.path.basename(object_key)
    local_path = os.path.join(local_dir, local_filename)

    try:
        print(f"Downloading {object_key} from bucket {bucket_name}...")
        s3_client.download_file(bucket_name, object_key, local_path)
        print(f"Successfully downloaded {object_key} to {local_path}")
        return local_path
    except ClientError as e:
        print(f"Failed to download {object_key}: {e}")
        return None

def extract_metadata(local_video_path):
    """
    Extract metadata from a video file using pymediainfo.
    Returns a dictionary containing metadata.
    """
    media_info = MediaInfo.parse(local_video_path)
    return media_info.to_data()

def main():
    # Ensure our local temp directory exists
    if not os.path.exists(LOCAL_TEMP_DIR):
        os.makedirs(LOCAL_TEMP_DIR)

    s3_client = boto3.client("s3", region_name=REGION_NAME)

    # Paginate through all objects in the input bucket
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=INPUT_BUCKET_NAME)

    page_count = 0
    for page in page_iterator:
        page_count += 1
        print(f"Processing page #{page_count}")

        if "Contents" not in page:
            print(f"No objects found in page #{page_count}. Skipping...")
            continue

        for item in page["Contents"]:
            object_key = item["Key"]
            # Only process MP4 files
            if not object_key.lower().endswith(".mp4"):
                print(f"Skipping non-MP4 file: {object_key}")
                continue

            print(f"Processing video file: {object_key}")

            # 1) Download the file locally
            local_video_path = download_video_from_s3(
                INPUT_BUCKET_NAME, object_key, LOCAL_TEMP_DIR
            )
            if not local_video_path:
                print(f"DOWNLOAD_FAILED: {object_key}")
                continue

            try:
                # 2) Extract metadata
                video_metadata = extract_metadata(local_video_path)

                # 3) Upload metadata JSON to the new “Metadata” bucket
                file_base_name = os.path.splitext(os.path.basename(object_key))[0]
                metadata_key = f"{file_base_name}_metadata.json"

                serialized_metadata = json.dumps(video_metadata, indent=2, ensure_ascii=False)

                s3_client.put_object(
                    Bucket=METADATA_BUCKET_NAME,
                    Key=metadata_key,
                    Body=serialized_metadata,
                    ContentType='application/json'
                )

                print(f"Metadata uploaded to s3://{METADATA_BUCKET_NAME}/{metadata_key}")

            except Exception as e:
                print(f"ERROR extracting or uploading metadata for {object_key}: {e}")
            finally:
                # Clean up local video file to save space
                if os.path.exists(local_video_path):
                    os.remove(local_video_path)

if __name__ == "__main__":
    main()