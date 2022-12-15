from script.docs_script import cw_policy_json, s3_read_policy_json
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
def setting_iam_policies2():
    iam = boto3.client('iam')

    CW_REGION = f"arn:aws:logs:us-east-1:{aws_user_id}:*"
    CW_RES = f"arn:aws:logs:us-east-1:{aws_user_id}:log-group:/aws/lambda/{FUNCTION_NAME}:*"

# Read the CloudWatch log policy template
    with open("../templates/cloudwatch_log_policy_template.json") as f:
        policy_template = json.load(f)

# update the policy template with the ARNs
    policy_template["Statement"][0]["Resource"] = CW_REGION
    policy_template["Statement"][1]["Resource"] = CW_RES



    CODE_BUCKET_NAME = "code-bucket"
    DATA_BUCKET_NAME = "data-bucket"

    # Read the contents of the s3_read_policy_template.json file
    with open("../templates/s3_read_policy_template.json", "r") as f:
        S3_READ_POLICY_TEMPLATE = json.load(f)

    # Update the .Statement[0].Resource elements in the policy template to refer to the specified S3 buckets
    S3_READ_POLICY_TEMPLATE[".Statement"][0]["Resource"][0] = f"arn:aws:s3:::{DATA_BUCKET_NAME}/*"
    S3_READ_POLICY_TEMPLATE[".Statement"][0]["Resource"][1] = f"arn:aws:s3:::{CODE_BUCKET_NAME}/*"
    # update buckets variables
    S3_READ_POLICY_TEMPLATE[".Statement"][0]["Resource"][2] = f"arn:aws:s3:::{BUCKETSSSSSS}/*"




    # The Amazon Resource Name (ARN) of the AWS Lambda function 
    lambda_function_arn = 'arn:aws:lambda:<REGION>:<ACCOUNT_ID>:function:<FUNCTION_NAME>'



# Create a new target for the rule, which is the AWS Lambda function that you
# want to be invoked when the rule is triggered
eventbridge_client.put_targets(
    Rule=rule_name,
    Targets=[
        {
            'Id': 'my-eventbridge-target',
            'Arn': lambda_function_arn
        }
    ]
)



    eventbridge = boto3.client('eventbridge')
    lambda_client = boto3.client('lambda')

    rule_name = 'OnFiveMinutes'

# The schedule expression that determines how often the rule is triggered. In
# this case, the rule will be triggered every five minutes
    schedule_expression = 'rate(5 minutes)'
    eventbridge.put_rule(
    Name=rule_name,
    ScheduleExpression=schedule_expression,
    State='ENABLED'
    )

    lambda_client.add_permission(
        FunctionName='FUNCTION_NAME',
        StatementId='EVENTBRIDGE_INVOKE_1',
        Action='lambda:InvokeFunction',
        Principal='events.amazonaws.com',
        SourceArn='arn:aws:events:REGION:ACCOUNT_ID:event-bus/EVENT_BUS_NAME'
    )

    # Add the specified Lambda function as a target for the rule
    eventbridge.put_targets(
        Rule=rule_name,
        Targets=[
            {
                'Id': 'EVENTBRIDGE_TARGET_1',
                'Arn': lambda_function_arn
            }
        ]
    )

 
    response = iam.attach_role_policy(
        PolicyArn=S3_READ_POLICY_TEMPLATE,
        RoleName="lambda-execution-role-{}".format(FUNCTION_NAME)
    )
