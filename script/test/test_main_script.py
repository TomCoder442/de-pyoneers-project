from unittest.mock import Mock, patch
import pytest
from main_script import create_bucket, zipper, function_name, setting_iam_policies
from moto import mock_s3
import time
import boto3


@mock_s3
def test_create_bucket():
    m3 = boto3.client('s3')
    timestamp = round(time.time())
    bucket_name = f'bucketname_{timestamp}'
    create_bucket(bucket_name)
    response =m3.list_buckets()
    assert bucket_name in [buckets['Name'] for buckets in response['Buckets']]


@mock_s3
def test_zipper():
    m3 = boto3.client('s3')
    m3.create_bucket(Bucket='my_bucket')
    zipper('./test/test_lambda_function.py', 'function.zip', 'my_bucket')
    print(m3.list_objects(Bucket='my_bucket'))
    assert f'{function_name}/function.zip' in m3.list_objects(Bucket='my_bucket')['Contents'][0]['Key']

@mock_s3
def test_setting_iam_policies():
    m3 = boto3.client('iam')
    setting_iam_policies()
    m3.list_attached_policies(UserName='username')
    assert 1==2

