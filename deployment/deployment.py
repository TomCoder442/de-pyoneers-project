import boto3
from botocore.client import ClientError
import json

try:
    s3_client = boto3.client('s3')
    EXTRACT_BUCKET = "pyoneers-extracted-data-test-2022-12-15"
    LOAD_BUCKET = "pyoneers-processed-data-test-2022-12-15"
    s3_client.create_bucket(Bucket='pyoneers-extracted-data-test-2022-12-15')
    s3_client.create_bucket(Bucket='pyoneers-processed-data-test-2022-12-15')
except ClientError as e:
    print(e)

print(f'Buckets {EXTRACT_BUCKET} and {LOAD_BUCKET} ready for use')