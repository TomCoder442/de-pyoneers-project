import json
import pandas as pd
# import awswrangler as wr
# import boto3


# def lambda_handler(event, context):
#     s3 = boto3.resource('s3')
#     bucket = s3.Bucket("pyoneers-extracted-data-test-2022-12-20-22")
#     for obj in bucket.objects.all():
#         key = obj.key
#         print(key)
#         body = obj.get()['Body']
        
#         s3_url = f's3://pyoneers-processed-data-test-2022-12-20-22/{key}.parquet'

#         wr.s3.to_parquet(
#         df = pd.read_csv(body),
#         path=s3_url)
    
print("hello world")