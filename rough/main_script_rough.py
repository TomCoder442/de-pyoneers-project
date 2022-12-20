import boto3
# from docs_script import cloud_watch_log_policy
import time
from datetime import datetime
import json

timestamp = round(time.time())
code_bucket = f'bucketname_{timestamp}'
incoming_data_bucket =f'raw_data_bucket_{timestamp}'
processed_data_bucket = f'processed_data_bucket_{timestamp}'
function_name = f'de_pyoneers_lambda_{timestamp}'
# Getting the account identity 
sts_client = boto3.client('sts')
identity = sts_client.get_caller_identity()
aws_account_id = identity['Account']
# can delete line below after testing
print(aws_account_id)
aws_region = 'us-east-1'

# Create an S3 client
s3 = boto3.client('s3')

# Create the bucket
response = s3.create_bucket(Bucket='my-bucket')

#  **CREATE THE LAMBDA FILE THEN ZIP IT HERE**

# Upload a the zip file to the bucket
# s3.upload_file('/path/to/local/file', 'my-bucket', ‘file.txt’)

# Create a CloudWatch Logs client
logs = boto3.client('logs')

# Define the log group names
all_log_groups_name = f'arn:aws:logs:${aws_region}:${aws_account_id}:*'
cw_log_group_name = f'arn:aws:logs:${aws_region}:${aws_account_id}:log-group:/aws/lambda/${aws_account_id}:*'

# Create the log groups
all_logs_creation_response = logs.create_log_group(logGroupName=all_log_groups_name)
cloud_watch_logs_creation_response = logs.create_log_group(logGroupName=cw_log_group_name)

# Create an IAM client
iam = boto3.client('iam')

# S3 read policy
# Load the s3 read policy document from a file
with open('s3_read_policy.json', 'r') as f:
    policy_document = json.load(f)

# Create the s3 read policy and extract the arn
s3_read_policy_creation = iam.create_policy(
    PolicyName=f's3-policy-${function_name}',
    PolicyDocument=json.dumps(policy_document)
)
s3_read_policy = s3_read_policy_creation['Policy']['Arn']

# Cloudwatch policy
# Load the cloudwatch policy document from a file
with open('cloudwatch_policy.json', 'r') as f:
    policy_document = json.load(f)
# Create the s3 read policy and extract the arn
cloudwatch_policy_creation = iam.create_policy(
    PolicyName=f'cloudwatch-policy-${function_name}',
    PolicyDocument=json.dumps(policy_document)
)

cloudwatch_policy = cloudwatch_policy_creation['Policy']['Arn']
