# from script.docs_script import cw_policy_json, s3_read_policy_json
import zipfile
import logging
import boto3
from botocore.exceptions import ClientError
import time
import json

timestamp = round(time.time())
code_bucket = f'bucketname-{timestamp}'
ingestion_bucket =f'raw-data-bucket-{timestamp}'
processed_data_bucket = f'processed-data-bucket-{timestamp}'
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
    with zipfile.ZipFile(f'./{zip_file_name}', 'w') as zip:
        zip.write(lambda_function_path)
    s3 = boto3.client('s3')

    s3.upload_file(f'./{zip_file_name}', bucket, f'{function_name}/function.zip')

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
    CW_RES = f"arn:aws:logs:us-east-1:{aws_user_id}:log-group:/aws/lambda/{function_name}:*"

    with open("templates/cloudwatch_log_policy_template.json") as f:
        cw_policy_template = json.load(f)

    cw_policy_template["Statement"][0]["Resource"] = CW_REGION
    cw_policy_template["Statement"][1]["Resource"] = CW_RES

    with open("templates/s3_read_policy_template.json", "r") as f:
        S3_READ_POLICY_TEMPLATE = json.load(f)

    S3_READ_POLICY_TEMPLATE["Statement"][0]["Resource"][0] = f"arn:aws:s3:::{code_bucket}/*"
    S3_READ_POLICY_TEMPLATE["Statement"][0]["Resource"][1] = f"arn:aws:s3:::{ingestion_bucket}/*"
    S3_READ_POLICY_TEMPLATE["Statement"][0]["Resource"][2] = f"arn:aws:s3:::{processed_data_bucket}/*"

    s3_response = iam.create_policy(
        PolicyName="s3_read_policy",
        PolicyDocument=json.dumps(S3_READ_POLICY_TEMPLATE)
    )

    s3_policy_arn = s3_response["Policy"]["Arn"]

    cw_response = iam.create_policy(
        PolicyName="cloudwatch_log_policy",
        PolicyDocument=json.dumps(cw_policy_template)
    )

    cw_policy_arn = cw_response["Policy"]["Arn"]

    with open('templates/trust_policy.json') as f:
        trust_policy = json.load(f)

    trust_policy_string = json.dumps(trust_policy)

    response = iam.create_role(
        RoleName=f"lambda-execution-role-{function_name}",
        AssumeRolePolicyDocument=trust_policy_string
    )

    EXECUTION_ROLE = response['Role']['Arn']

    response2 = iam.attach_role_policy(
        PolicyArn=s3_policy_arn,
        RoleName="lambda-execution-role-{}".format(function_name)
    )

    iam.attach_role_policy(
        PolicyArn=cw_policy_arn,
        RoleName="lambda-execution-role-{}".format(function_name)
    )

    return {"response": response, "response2": response2}


    # lambda_function_arn = f'arn:aws:lambda:{aws_region}:{aws_account}:function:{function_name}'

    # lambda_client = boto3.client('lambda')





    # eventbridge = boto3.client('events')


    # rule_name = 'OnFiveMinutes'

    # # The schedule expression that determines how often the rule is triggered. In
    # # this case, the rule will be triggered every five minutes
    # schedule_expression = 'rate(5 minutes)'
    
    # eventbridge.put_rule(
    #     Name=rule_name,
    #     ScheduleExpression=schedule_expression,
    #     State='ENABLED'
    # )

    # lambda_client.add_permission(
    #     FunctionName=f'{function_name}',
    #     StatementId='EVENTBRIDGE_INVOKE_1',
    #     Action='lambda:InvokeFunction',
    #     Principal='events.amazonaws.com',
    #     SourceArn=f'arn:aws:events:{aws_region}:{aws_account}:event-bus/my-event-bus'
    # )

    # # Add the specified Lambda function as a target for the rule
    # eventbridge.put_targets(
    #     Rule=rule_name,
    #     Targets=[
    #         {
    #             'Id': 'EVENTBRIDGE_TARGET_1',
    #             'Arn': lambda_function_arn
    #         }
    #     ]
    # )

 
    # response = iam.attach_role_policy(
    #     PolicyArn=S3_READ_POLICY_TEMPLATE,
    #     RoleName="lambda-execution-role-{}".format(function_name)
    # )

def create_lambda_function():
    lambda_client = boto3.client('lambda')


    iam_client = boto3.client('iam')

    with open('templates/trust_policy.json') as f:
        trust_policy = json.load(f)

    trust_policy_string = json.dumps(trust_policy)

    response = iam_client.create_role(
        RoleName="lambda-execution-role-de_pyoneers_lambda_1671112825",
        AssumeRolePolicyDocument=trust_policy_string
    )

    EXECUTION_ROLE = response['Role']['Arn']

    # Role=EXECUTION_ROLE,

    response = lambda_client.create_function(
        FunctionName=function_name,
        Runtime='python3.9',
        Role= EXECUTION_ROLE,
        Handler='main.handler',
        Code={
            # 'ZipFile': open(deployment_package, 'rb').read(),
            'S3Bucket': "bucketname-1671112825",
            'S3Key': "de_pyoneers_lambda_1671112825/function.zip"
        }
    )

    with open('confirmation.json', 'w') as f:
        json.dump(response, f)

# create_lambda_function()



# create_bucket(code_bucket)
# create_bucket(ingestion_bucket)
# create_bucket(processed_data_bucket)
# zipper('./script/test/test_lambda_function.py', 'function.zip', 'bucketname-1671112825')
# setting_iam_policies2()