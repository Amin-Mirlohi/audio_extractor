import boto3


def list_files_in_bucket(bucket_name):
    """
    Lists and prints the names of all objects in the specified S3 bucket.

    :param bucket_name: Name of the S3 bucket whose objects you want to list.
    """
    s3 = boto3.client("s3")

    # Use a paginator to handle all pages of objects in the bucket
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket_name)

    for page in page_iterator:
        # 'Contents' contains information about the objects in the bucket on this page
        for obj in page.get("Contents", []):
            print(obj["Key"])


list_files_in_bucket("video-audio-test")
