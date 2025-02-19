import os

import boto3
from botocore.exceptions import ClientError

from config import LOCAL_TEMP_DIR, REGION_NAME


def download_video_from_s3(bucket_name, object_key, logger):
    """
    Download a video file from S3 to LOCAL_TEMP_DIR.
    Return the local file path if successful, None if failed.
    """
    s3_client = boto3.client("s3", region_name=REGION_NAME)
    local_path = os.path.join(LOCAL_TEMP_DIR, os.path.basename(object_key))

    try:
        logger.info(f"Downloading {object_key} from bucket {bucket_name}...")
        s3_client.download_file(bucket_name, object_key, local_path)
        logger.info(f"Successfully downloaded {object_key} to {local_path}")
        return local_path
    except ClientError as e:
        logger.error(f"Failed to download {object_key}: {e}")
        return None
