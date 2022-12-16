from unittest.mock import Mock, patch
import pytest
from script.main_script import create_bucket, zipper, setting_iam_policies2
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
    with patch('script.main_script.function_name', new='test'):
        function_name = 'test'
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
        assert policies_attached_to_erole[0]['PolicyArn'] == 'arn:aws:iam::123456789012:policy/cloudwatch_log_policy'
        assert policies_attached_to_erole[1]['PolicyArn'] == 'arn:aws:iam::123456789012:policy/s3_read_policy'
        assert 1 == 2



# @mock_iam
# def test_setting_iam_policies4():
#     iam = boto3.client('iam')
    


#     # Get the attach_role_policy method from the mock iam object
#     attach_role_policy_method = iam.attach_role_policy

#     print(dir(attach_role_policy_method))

#     setting_iam_policies2()

#     # Get the policy ARNs that were passed to the attach_role_policy method
#     s3_policy_arn = attach_role_policy_method.call_args_list[0][1]['PolicyArn']
#     cw_policy_arn = attach_role_policy_method.call_args_list[1][1]['PolicyArn']

#     # Assert that the attach_role_policy method was called with the correct policy ARNs
#     assert attach_role_policy_method.assert_called_with(PolicyArn=s3_policy_arn, RoleName="lambda-execution-role-{}".format(function_name))
#     assert attach_role_policy_method.assert_called_with(PolicyArn=cw_policy_arn, RoleName="lambda-execution-role-{}".format(function_name))


# def test_create_lambda_function():
#     pass

# def test_eventbridge_policy():
#     pass