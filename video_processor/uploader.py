import boto3
from botocore.exceptions import ClientError

from config import REGION_NAME


def upload_audio_to_s3(local_file_path, bucket_name, object_key, logger):
    """
    Upload the local extracted audio file to S3.
    object_key should be the desired key (filename) in the destination bucket.
    """
    s3_client = boto3.client("s3", region_name=REGION_NAME)

    try:
        logger.info(
            f"Uploading {local_file_path} to bucket {bucket_name} (key: {object_key})..."
        )
        s3_client.upload_file(local_file_path, bucket_name, object_key)
        logger.info(
            f"Successfully uploaded {local_file_path} to s3://{bucket_name}/{object_key}"
        )
        return True
    except ClientError as e:
        logger.error(f"Failed to upload {local_file_path}: {e}")
        return False
