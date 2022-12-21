from deployment.extractor import grab_last_created_updated_dates, load_to_s3
import pytest
import boto3
from moto import mock_s3
from pandas import util

@mock_s3
def test_load_to_s3():
    df= util.testing.makeDataFrame()
    src_tables = ['counterparty','currency','department','design','staff','sales_order','address','payment','purchase_order','payment_type','transaction']
    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket='mybucket')
    for tbl in src_tables:
        load_to_s3(df, tbl, 'mybucket')
    bucket_lst=[]
    for my_bucket_object in conn.Bucket('mybucket').objects.all():
        bucket_lst.append(my_bucket_object.key)
    assert len(bucket_lst) == len(src_tables)
    
def test_grab_last_created_date():
    src_tables = ['counterparty','currency','department','design','staff','sales_order','address','payment','purchase_order','payment_type','transaction']

    max_created_dates, max_updated_dates = grab_last_created_updated_dates()
    assert type(max_created_dates) is dict
    for tbl in src_tables:
        assert type(max_created_dates[tbl]) is str
        assert type(max_updated_dates[tbl]) is str

    assert type(max_created_dates) is dict
    assert type(max_updated_dates) is dict
    assert len(src_tables) == len(max_updated_dates)
    assert len(src_tables) == len(max_created_dates)
