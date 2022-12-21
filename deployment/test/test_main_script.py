from unittest.mock import Mock, patch
import pytest
from deployment.deployment import Lambda_script
# create_bucket, zipper, create_lambda_function, timestamp, eventbridge_trigger, setting_eventbridge_permissions, creating_cw_policy, creating_s3_policy, creating_the_execution_role, attaching_policies_to_er
from moto import mock_s3, mock_iam, mock_lambda, mock_events
import time
import boto3
from unittest.mock import patch
import json
import unittest

script = Lambda_script('deployment/test/test_lambda_function.py', 'function.zip')

@mock_s3
def test_create_bucket():
    # This tests the creation of buckets inside of s3 on aws
    m3 = boto3.client('s3')
    timestamp = round(time.time())
    bucket_name = f'bucketname-{timestamp}'
    script.create_bucket(bucket_name)
    response =m3.list_buckets()
    assert bucket_name in [buckets['Name'] for buckets in response['Buckets']]

# This tests the function which zips the lambda python code and uploads to the code bucket on aws
@mock_s3
@patch.multiple(Lambda_script, function_name='test', timestamp='test')
def test_zipper():
    m3 = boto3.client('s3')
    m3.create_bucket(Bucket='test')

    # with patch('script.main_script.Lambda_script.lambda_function_path', new='script/test/test_lambda_function.py'):
    function_name = 'test'
    m3.create_bucket(Bucket='my_bucket')
    script.zipper('test')
    assert f'{function_name}/function.zip' in m3.list_objects(Bucket='test')['Contents'][0]['Key']

@mock_iam
@patch.object(Lambda_script, 'timestamp', 'test')
def test_creating_cw_policy():
    iam = boto3.client('iam')
    # with patch('script.main_script.Lambda_script.timestamp', new='test'):
    #     Saving the response object of the test function
    response = script.creating_cw_policy()

    # Asserting that the AWS api was succesfully pinged when requesting the policy creation
    assert response["CW_creation_response"]['ResponseMetadata']['HTTPStatusCode'] == 200

    print(iam.get_policy(PolicyArn=response['CW_policy_arn'])['Policy']['PolicyName'])
    # Asserting that the policy has been succesfully added to IAM
    assert iam.get_policy(PolicyArn=response['CW_policy_arn'])['Policy']['PolicyName'] == 'cloudwatch_log_policy_test'

@patch.object(Lambda_script, 'timestamp', 'test')
@mock_iam
def test_creating_s3_policy():
    iam = boto3.client('iam')

    # Saving the response object of the test function
    response = script.creating_s3_policy()
    print(response)

    # Asserting that the AWS api was succesfully pinged when requesting the policy creation
    assert response["S3_creation_response"]['ResponseMetadata']['HTTPStatusCode'] == 200

    print(iam.get_policy(PolicyArn=response['S3_policy_arn'])['Policy']['PolicyName'])
    # Asserting that the policy has been succesfully added to IAM
    assert iam.get_policy(PolicyArn=response['S3_policy_arn'])['Policy']['PolicyName'] == 's3_read_policy_test'

@mock_iam
@patch.multiple(Lambda_script, function_name='test', timestamp='test')
def test_creating_the_execution_role():
    iam = boto3.client('iam')

    # Saving the response object of the test function
    response = script.creating_the_execution_role()

    # Asserting that the AWS api was succesfully pinged when requesting the policy creation
    assert response["Saving_execution_role_to_iam_response"]['ResponseMetadata']['HTTPStatusCode'] == 200

    # Asserting that the role has been succesfully added and exists in IAM
    roles_list = iam.list_roles()
    print(roles_list)
    check_role_exists = [role['RoleName'] for role in roles_list['Roles'] if role['RoleName'] == 'lambda-execution-role-test']
    assert check_role_exists[0] == 'lambda-execution-role-test'

@mock_iam
@patch.multiple(Lambda_script, function_name='test', timestamp='test', aws_account='123456789012')
def test_attaching_policies_to_er():
    iam = boto3.client('iam')

    script.creating_the_execution_role()
    script.creating_cw_policy()
    script.creating_s3_policy()
    response = script.attaching_policies_to_er()
    timestamp = 'test'

    policies_attached_to_erole = iam.list_attached_role_policies(RoleName=f"lambda-execution-role-{'test'}")['AttachedPolicies']
    print(policies_attached_to_erole[0]['PolicyArn'], policies_attached_to_erole[1]['PolicyArn'])
    assert response["Attaching_cw_policy_to_er_response"]['ResponseMetadata']['HTTPStatusCode'] == 200
    assert response["Attaching_s3_policy_to_er_response"]['ResponseMetadata']['HTTPStatusCode'] == 200
    assert policies_attached_to_erole[0]['PolicyArn'] == f'arn:aws:iam::123456789012:policy/cloudwatch_log_policy_{timestamp}'
    assert policies_attached_to_erole[1]['PolicyArn'] == f'arn:aws:iam::123456789012:policy/s3_read_policy_{timestamp}'

@mock_lambda
@mock_s3
@mock_iam
@patch.multiple(Lambda_script, aws_account='123456789012', function_name='test-function', timestamp='test')
def test_create_lambda_function():
    # This test is testing the function which unzips the deployment lambda script into the code bucket and subsequently create the lambda function. 
    s3_client = boto3.client('s3')
    lambda_client = boto3.client('lambda')
    iam= boto3.client('iam')


    # THIS CODE IS SETTING UP THE MOCK PREREQUISITE AWS RESOURCES REQUIRED TO TEST THE LAMBDA CREATION FUNCTION
    # This code is creating the mock buckets on moto
    bucket_name = 'test_bucket'
    s3_client.create_bucket(Bucket=bucket_name)
    
    res = s3_client.list_buckets()
    assert bucket_name in [buckets['Name'] for buckets in res['Buckets']]

    s3_client.upload_file('function.zip', 'test_bucket', 'my-function/function.zip')

    with open('templates/trust_policy.json') as f:
        trust_policy = json.load(f)
    trust_policy_string = json.dumps(trust_policy)
    iam.create_role(
            RoleName=f"lambda-execution-role-{'test-function'}",
            AssumeRolePolicyDocument=trust_policy_string
        )

# *******
    # bucket_name = 'test_bucket'
    # s3_client.create_bucket(Bucket=bucket_name)
    # res = s3_client.list_buckets()
    # assert bucket_name in [buckets['Name'] for buckets in res['Buckets']]
    # s3_client.upload_file('function.zip', 'test_bucket', 'my-function/function.zip')
    # script.creating_cw_policy()
    # script.creating_s3_policy()
    # script.creating_the_execution_role()
    # script.attaching_policies_to_er

    # This code is creating the mock execution which the created lambda function will be attached to. 
    # with open('templates/trust_policy.json') as f:
    #     trust_policy = json.load(f)
    # trust_policy_string = json.dumps(trust_policy)
    # response = iam_client.create_role(
    #     RoleName="test-function-role",
    #     AssumeRolePolicyDocument=trust_policy_string
    # )
    # EXECUTION_ROLE = response['Role']['Arn']
    # print(EXECUTION_ROLE)
# *********

    # This is calling the function we are testing which will create the lambda.
    response = script.create_lambda_function(bucket_name, 'my-function')
    function_list = lambda_client.list_functions()['Functions']
    function_names = [func['FunctionName'] for func in function_list]

    # This asserts that the Lambda function now exists in AWS lambda.
    assert response['FunctionName'] in function_names

@mock_s3
@mock_iam
@mock_lambda
@mock_events
@patch.multiple(Lambda_script, aws_account='123456789012', function_name='test-function', timestamp='test')
def test_setting_event_bridge_permissions():
    # This test is testing the function which will add permissions to the lambda, which grant eventbridge permission to invoke the lambda.   
    lambda_client = boto3.client('lambda')
    s3_client = boto3.client('s3')
    iam_client = boto3.client('iam')
    # SETTING UP THE PREREQUISITE AWS RESOURCES
    # with patch('script.main_script.Lambda_script.function_name', new='test'):
    bucket_name = 'test_bucket'
    s3_client.create_bucket(Bucket=bucket_name)
    
    res = s3_client.list_buckets()
    assert bucket_name in [buckets['Name'] for buckets in res['Buckets']]

    s3_client.upload_file('function.zip', 'test_bucket', 'my-function/function.zip')

    with open('templates/trust_policy.json') as f:
        trust_policy = json.load(f)
    trust_policy_string = json.dumps(trust_policy)

    response = iam_client.create_role(
        RoleName=f"lambda-execution-role-{'test-function'}",
        AssumeRolePolicyDocument=trust_policy_string
    )

    response = script.create_lambda_function(bucket_name, 'test-function')
    print(response['FunctionArn'])

    # Calling the function we are testing
    script.setting_eventbridge_permissions()
    
    # Setting up the testing variables
    policy_response = lambda_client.get_policy(FunctionName='test-function')
    policy = json.loads(policy_response['Policy'])

    # Assertions
    # Checks the add_permissions request pinged the api succesfully
    assert policy_response['ResponseMetadata']['HTTPStatusCode'] == 200
    # Checks that the permission to allow eventbridge to invoke lambda was added succesfully
    assert policy['Statement'][0]['Action'] == 'lambda:InvokeFunction'


@mock_s3
@mock_iam
@mock_lambda
@mock_events
@patch.multiple(Lambda_script, aws_account='123456789012', function_name='test-function', timestamp='test')
def test_eventbridge_trigger_creation():
    # Now the permissions have been succesfully added, this test is testing the function which will add a rule to Eventbridge which will schedule the lambda to be invoked every 5 minutes from an eventbridge schedule. 
    eventbridge = boto3.client('events')
    lambda_client = boto3.client('lambda')
    s3_client = boto3.client('s3')
    iam= boto3.client('iam')

    #  SETTING UP THE PREREQUISITE AWS RESOURCES
    # with patch('script.main_script.Lambda_script.function_name', new='test'):

    bucket_name = 'test_bucket'
    s3_client.create_bucket(Bucket=bucket_name)
    
    res = s3_client.list_buckets()
    assert bucket_name in [buckets['Name'] for buckets in res['Buckets']]

    s3_client.upload_file('function.zip', 'test_bucket', 'my-function/function.zip')

    with open('templates/trust_policy.json') as f:
        trust_policy = json.load(f)
    trust_policy_string = json.dumps(trust_policy)
    iam.create_role(
            RoleName=f"lambda-execution-role-{'test-function'}",
            AssumeRolePolicyDocument=trust_policy_string
        )

    response = script.create_lambda_function(bucket_name, 'test-function')
    print(response['FunctionArn'])

    function_list = lambda_client.list_functions()['Functions']
    function_names = [func['FunctionName'] for func in function_list]

    script.setting_eventbridge_permissions()

    # Calling the eventbridge trigger function with the test function ARN
    script.eventbridge_trigger(response['FunctionArn'])

    # Setting up testing variables
    eventbridge_rules = eventbridge.list_rules()    
    targets = eventbridge.list_targets_by_rule(Rule='OnFiveMinutes')['Targets']
    print(targets)

    # ASSERTIONS
    # This is checking the function the rule was targetted to is correct. 
    assert response['FunctionName'] in function_names
    # This is checking that the eventbridge rule was indeed created in aws.
    assert 'OnFiveMinutes' in eventbridge_rules['Rules'][0]['Name']
    # This is checking that the rule was added with the correct target. 
    assert any(target['Arn'] == response['FunctionArn'] for target in targets)

@mock_s3
@mock_iam
@mock_lambda
@mock_events
@patch.multiple(Lambda_script, function_name='test', timestamp='test_master', aws_account='123456789012')
def test_master():
    script.master()
    m3 = boto3.client('s3')
    response =m3.list_buckets()
    
    
    