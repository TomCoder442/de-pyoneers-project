from docs_script import cw_policy_json, s3_read_policy_json
import zipfile
import logging
import boto3
from botocore.exceptions import ClientError
import time
import json

timestamp = round(time.time())
code_bucket = f'bucketname_{timestamp}'
ingestion_bucket =f'raw_data_bucket_{timestamp}'
processed_data_bucket = f'processed_data_bucket_{timestamp}'
function_name = f'de_pyoneers_lambda_{timestamp}'
aws_region = 'us-east-1'
sts_client = boto3.client('sts')
caller_identity = sts_client.get_caller_identity()
aws_account = caller_identity['Account']
aws_user_id = caller_identity['UserId']
session = boto3.session.Session().region_name
print(session)


def create_bucket(bucket_name, region='us-east-1'):
    s3 = boto3.client('s3')
    try: s3.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def zipper(lambda_function_path, zip_file_name, bucket=code_bucket):
    with zipfile.ZipFile(f'../{zip_file_name}', 'w') as zip:
        zip.write(lambda_function_path)
    s3 = boto3.client('s3')

    s3.upload_file(f'../{zip_file_name}', bucket, f'{function_name}/function.zip')

def setting_iam_policies():
    # Create an IAM client
    iam = boto3.client('iam')
    # S3 read policy
    # Create the s3 read policy and extract the arn
    s3_read_policy_creation = iam.create_policy(
        PolicyName=f's3-policy-${function_name}',
        PolicyDocument=json.dumps(s3_read_policy_json)
    )
    s3_read_policy = s3_read_policy_creation['Policy']['Arn']

    # Cloudwatch policy
    # Create the s3 read policy and extract the arn
    cloudwatch_policy_creation = iam.create_policy(
        PolicyName=f'cloudwatch-policy-${function_name}',
        PolicyDocument=json.dumps(cw_policy_json)
    )
    cloudwatch_policy = cloudwatch_policy_creation['Policy']['Arn']

    return s3_read_policy, cloudwatch_policy

# NEED TO ADD THE EXECUTION ROLE 
