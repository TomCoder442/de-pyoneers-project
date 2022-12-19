# from script.docs_script import cw_policy_json, s3_log_policy_json
import zipfile
import logging
import boto3
from botocore.exceptions import ClientError
import time
import json

timestamp = round(time.time())
timestamp = '000002'
code_bucket = f'code-bucket-{timestamp}'
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



def setting_iam_policies2():
    iam = boto3.client('iam')

    # Customising the CLOUDWATCH POLICY from the json template and saving into a variable, then adding it to IAM
    # Creating the customised arns to paste into the cloudwatch policy template
    cw_region = f"arn:aws:logs:{aws_region}:{aws_account}:*"
    cw_resources = f"arn:aws:logs:{aws_region}:{aws_account}:log-group:/aws/lambda/{function_name}:*"
    # Loading the template policy into a variable
    with open("templates/cloudwatch_log_policy_template.json") as f:
        cw_policy_template = json.load(f)
    # Customising the template policy inside of the variable
    cw_policy_template["Statement"][0]["Resource"] = cw_region
    cw_policy_template["Statement"][1]["Resource"] = cw_resources
    # Fully customised cw_policy_template now exists in above variable

    # Creating the policy on IAM with the value of the customised template variable
    cw_creation_response = iam.create_policy(
        PolicyName=f"cloudwatch_log_policy_{timestamp}",
        PolicyDocument=json.dumps(cw_policy_template)
    )
    # Saving the ARN of the policy from IAM
    cw_policy_arn = cw_creation_response["Policy"]["Arn"]

    # Customising the S3_read POLICY from the json template and saving into a variable, then adding it to IAM
    with open("templates/s3_read_policy_template.json", "r") as f:
        s3_read_policy_template = json.load(f)

    # Adding each of the buckets to the permissions policy template document and saving to a variable
    s3_read_policy_template["Statement"][0]["Resource"][0] = f"arn:aws:s3:::{code_bucket}/*"
    s3_read_policy_template["Statement"][0]["Resource"][1] = f"arn:aws:s3:::{ingestion_bucket}/*"
    s3_read_policy_template["Statement"][0]["Resource"][2] = f"arn:aws:s3:::{processed_data_bucket}/*"
    
    # Creating the policy on IAM with the value of the customised template variable
    s3_creation_response = iam.create_policy(
        PolicyName=f"s3_read_policy_{timestamp}",
        PolicyDocument=json.dumps(s3_read_policy_template)
    )

    # Saving the ARN of the policy from IAM
    s3_policy_arn = s3_creation_response["Policy"]["Arn"]

    # Adding the trust policy (no need to customise) for the execution role to a variable and converting back to a json string
    with open('templates/trust_policy.json') as f:
        trust_policy = json.load(f)
    trust_policy_string = json.dumps(trust_policy)
    
    # Saving the execution role in IAM with trust policy attached
    saving_execution_role_to_iam_response = iam.create_role(
        RoleName=f"lambda-execution-role-{function_name}",
        AssumeRolePolicyDocument=trust_policy_string
    )

    # Saving the ARN of the execution role on IAM
    execution_role = saving_execution_role_to_iam_response['Role']['Arn']
    
    attaching_s3_policy_to_er_response = iam.attach_role_policy(
        PolicyArn=s3_policy_arn,
        RoleName=f'lambda-execution-role-{function_name}'
    )

    attaching_cw_policy_to_er_response = iam.attach_role_policy(
        PolicyArn=cw_policy_arn,
        RoleName=f'lambda-execution-role-{function_name}'
    )


    response = {"CW_creation_response": cw_creation_response, "S3_creation_response": cw_creation_response, "Saving_execution_role_to_iam_response": saving_execution_role_to_iam_response, "Attaching_cw_policy_to_er_response": attaching_cw_policy_to_er_response, "Attaching_s3_policy_to_er_response": attaching_s3_policy_to_er_response, "execution_role": execution_role}

    return response

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
    #     PolicyArn=S3_log_POLICY_TEMPLATE,
    #     RoleName="lambda-execution-role-{}".format(function_name)
    # )

def create_lambda_function(iam_role, bucket, lambda_function):
    lambda_client = boto3.client('lambda')

    response = lambda_client.create_function(
        FunctionName=lambda_function,
        Runtime='python3.9',
        Role= iam_role,
        Handler='main.handler',
        Code={
            # 'ZipFile': open(deployment_package, 'rb')_log(),
            'S3Bucket': bucket,
            'S3Key': f"{lambda_function}/function.zip"
        }
    )

    with open('confirmation.json', 'w') as f:
        json.dump(response, f)

    return response

# create_lambda_function()



# create_bucket(code_bucket)
# create_bucket(ingestion_bucket)
# create_bucket(processed_data_bucket)
# zipper('script/test/test_lambda_function.py', 'function.zip')
# response = setting_iam_policies2()
# time.sleep(5)
# create_lambda_function(response['execution_role'], code_bucket, function_name)
# create_lambda_function('arn:aws:iam::846141597640:role/lambda-execution-role-de_pyoneers_lambda_000002', code_bucket, function_name)


def eventbridge_trigger(lambda_function_arn = f'arn:aws:lambda:{aws_region}:{aws_account}:function:{function_name}'):

    eventbridge = boto3.client('events')

    # lambda_function_arn = f'arn:aws:lambda:{aws_region}:{aws_account}:function:{function_name}'

    # lambda_client = boto3.client('lambda')

    rule_name = 'OnFiveMinutes'

    # The schedule expression that determines how often the rule is triggered. In
    # this case, the rule will be triggered every five minutes
    schedule_expression = 'rate(5 minutes)'
    
    eventbridge.put_rule(
        Name=rule_name,
        ScheduleExpression=schedule_expression,
        State='ENABLED'
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

# eventbridge_trigger()

# def cloudwatch_logs_creation():
#     cw_region = f'arn:aws:logs:{aws_region}:{aws_account}:*'
#     cw_resources = f'arn:aws:logs:{aws_region}:{aws_account}:log-group:/aws/lambda/{function_name}:*'
#     # Create a logs client
#     logs_client = boto3.client('logs')
#     # Set the name of the log group you want to create
#     log_group_name = f'/aws/lambda/{function_name}'
#     # Customising the polocy doc
#     create_cw_logs_group_response = logs_client.create_log_group(logGroupName=log_group_name)
#     with open('templates/cloudwatch_log_policy_template.json', 'r') as f:
#             cloudwatch_log_policy_template = json.load(f)
#     cloudwatch_log_policy_template['Statement'][0]['Resource'] = cw_region
#     cloudwatch_log_policy_template['Statement'][1]['Resource'] = cw_resources
#     policy_document = json.dumps(cloudwatch_log_policy_template)
#     # Call the put_resource_policy method
#     put_cw_resource_policy_response = logs_client.put_resource_policy(
#         policyName='lambda-cw_logs',
#         policyDocument=policy_document,
#         logGroupName=log_group_name
#     )
#     return {'create_cw_logs_group_response': create_cw_logs_group_response, 'put_cw_resource_policy_response': put_cw_resource_policy_response}

# cloudwatch_logs_creation()
 


