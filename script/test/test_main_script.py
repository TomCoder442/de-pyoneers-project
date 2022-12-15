from unittest.mock import Mock, patch
import pytest
from script.main_script import create_bucket, zipper, function_name, setting_iam_policies2
from moto import mock_s3, mock_iam
import time
import boto3
from unittest.mock import patch


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
    m3.create_bucket(Bucket='my_bucket')
    zipper('script/test/test_lambda_function.py', 'function.zip', 'my_bucket')
    print(m3.list_objects(Bucket='my_bucket'))
    assert f'{function_name}/function.zip' in m3.list_objects(Bucket='my_bucket')['Contents'][0]['Key']

# @mock_iam
# def test_setting_iam_policies2():
#     m3 = boto3.client('iam')
#     setting_iam_policies2()
#     m3.list_attached_policies(UserName='username')
#     assert 1==2

@mock_iam
def test_setting_iam_policies3():
    with patch('script.main_script.function_name', new = 'test'):
        iam = boto3.client('iam')
        response = setting_iam_policies2()
        print(repsonse)
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200
        assert response['Role']['Arn'] == f'arn:aws:iam::123456789012:role/lambda-execution-role-test'
        assert iam.attach_role_policy(PolicyArn=s3_policy_arn, RoleName="lambda-execution-role-{}".format(function_name))
        assert iam.assert_called_with(PolicyArn=cw_policy_arn, RoleName="lambda-execution-role-{}".format(function_name))


from moto import mock_iam

@mock_iam
def test_setting_iam_policies4():
    iam = boto3.client('iam')
    response = setting_iam_policies2()

    # Get the attach_role_policy method from the mock iam object
    attach_role_policy_method = iam.attach_role_policy

    print(dir(attach_role_policy_method))

    # Get the policy ARNs that were passed to the attach_role_policy method
    s3_policy_arn = attach_role_policy_method.call_args_list[0][1]['PolicyArn']
    cw_policy_arn = attach_role_policy_method.call_args_list[1][1]['PolicyArn']

    # Assert that the attach_role_policy method was called with the correct policy ARNs
    assert attach_role_policy_method.assert_called_with(PolicyArn=s3_policy_arn, RoleName="lambda-execution-role-{}".format(function_name))
    assert attach_role_policy_method.assert_called_with(PolicyArn=cw_policy_arn, RoleName="lambda-execution-role-{}".format(function_name))
