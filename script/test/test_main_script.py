from unittest.mock import Mock, patch
import pytest
from script.main_script import create_bucket, zipper, setting_iam_policies2, create_lambda_function, timestamp
from moto import mock_s3, mock_iam, mock_lambda
import time
import boto3
from unittest.mock import patch
import json



@mock_s3
def test_create_bucket():
    m3 = boto3.client('s3')
    timestamp = round(time.time())
    bucket_name = f'bucketname-{timestamp}'
    create_bucket(bucket_name)
    response =m3.list_buckets()
    assert bucket_name in [buckets['Name'] for buckets in response['Buckets']]


@mock_s3
def test_zipper():
    m3 = boto3.client('s3')
    with patch('script.main_script.function_name', new='test'):
        function_name = 'test'
        m3.create_bucket(Bucket='my_bucket')
        zipper('script/test/test_lambda_function.py', 'function.zip', 'my_bucket')
        print(m3.list_objects(Bucket='my_bucket'))
        assert f'{function_name}/function.zip' in m3.list_objects(Bucket='my_bucket')['Contents'][0]['Key']


@mock_iam
def test_setting_iam_policies3():
    iam = boto3.client('iam')
    with patch('script.main_script.function_name', new='test'):
        response = setting_iam_policies2()

        # TEST AREA 
        # response5 = iam.list_policies()
        # print([p["PolicyName"] for p in response5["Policies"] if p['PolicyName'] == 's3_read_policy'])
        # print(response)

        response8 = iam.get_role(RoleName=f"lambda-execution-role-{'test'}")
        # role = response8['Role']
        # print(role)
        policies_attached_to_erole = iam.list_attached_role_policies(RoleName=f"lambda-execution-role-{'test'}")['AttachedPolicies']
        print(policies_attached_to_erole[0]['PolicyArn'], policies_attached_to_erole[1]['PolicyArn'])
        assert response["Saving_execution_role_to_iam_response"]['ResponseMetadata']['HTTPStatusCode'] == 200
        assert response["Saving_execution_role_to_iam_response"]['Role']['Arn'] == f'arn:aws:iam::123456789012:role/lambda-execution-role-test'
        assert response["CW_creation_response"]['ResponseMetadata']['HTTPStatusCode'] == 200
        assert response["S3_creation_response"]['ResponseMetadata']['HTTPStatusCode'] == 200
        assert response["Attaching_cw_policy_to_er_response"]['ResponseMetadata']['HTTPStatusCode'] == 200
        assert response["Attaching_s3_policy_to_er_response"]['ResponseMetadata']['HTTPStatusCode'] == 200
        assert policies_attached_to_erole[0]['PolicyArn'] == f'arn:aws:iam::123456789012:policy/cloudwatch_log_policy_{timestamp}'
        assert policies_attached_to_erole[1]['PolicyArn'] == f'arn:aws:iam::123456789012:policy/s3_read_policy_{timestamp}'






@mock_lambda
@mock_iam
@mock_s3
def test_create_lambda_function():
    iam_client = boto3.client('iam')
    s3_client = boto3.client('s3')
    lambda_client = boto3.client('lambda')

    bucket_name = 'test_bucket'
    s3_client.create_bucket(Bucket=bucket_name)
    
    res = s3_client.list_buckets()
    assert bucket_name in [buckets['Name'] for buckets in res['Buckets']]

    s3_client.upload_file('function.zip', 'test_bucket', 'my-function/function.zip')

    with open('templates/trust_policy.json') as f:
        trust_policy = json.load(f)

    trust_policy_string = json.dumps(trust_policy)

    response = iam_client.create_role(
        RoleName="test-function-role",
        AssumeRolePolicyDocument=trust_policy_string
    )
    EXECUTION_ROLE = response['Role']['Arn']

    response = create_lambda_function(EXECUTION_ROLE, bucket_name, 'my-function')


    function_list = lambda_client.list_functions()['Functions']
    function_names = [func['FunctionName'] for func in function_list]

    assert response['FunctionName'] in function_names

# def test_eventbridge_policy():
#     pass