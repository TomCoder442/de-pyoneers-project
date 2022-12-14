import json

from main_script import aws_region, aws_account_id, function_name, code_bucket, ingestion_bucket, processed_data_bucket

# # USE THESE VARIABLE VALUES FOR TESTING WHEN NOT CONNECTED TO AWS SANDBOX
# aws_region ='a'
# aws_account_id = 'b'
# function_name ='0'
# code_bucket = 'c'
# incoming_data_bucket = 'd'

cloud_watch_log_policy ={
                                "Version": "2012-10-17",
                                "Statement": [
                                    {
                                        "Effect": "Allow",
                                        "Action": "logs:CreateLogGroup",
                                        "Resource": f'arn:aws:logs:{aws_region}:{aws_account_id}:*'
                                    },
                                    {
                                        "Effect": "Allow",
                                        "Action": [
                                            "logs:CreateLogStream",
                                            "logs:PutLogEvents"
                                        ],
                                        "Resource": [
                                            f'arn:aws:logs:{aws_region}:{aws_account_id}:log-group:/aws/lambda/{function_name}:*'
                                        ]
                                    }
                                ]
                            }

s3_read_policy_statement = {
                                "Version": "2012-10-17",
                                "Statement": [
                                    {
                                        "Sid": "VisualEditor0",
                                        "Effect": "Allow",
                                        "Action": [
                                            "s3:GetObject"
                                        ],
                                        "Resource": [
                                            f'"arn:aws:s3:::{code_bucket}/*"',
                                            f'"arn:aws:s3:::{ingestion_bucket}/*"'
                                            f'"arn:aws:s3:::{processed_data_bucket}/*"'
                                        ]
                                    }
                                ]
                            }

# Encode the data as a JSON string
cw_policy_json = json.dumps(cloud_watch_log_policy, indent=4)

s3_read_policy_json = json.dumps(s3_read_policy_statement, indent=4)

print(s3_read_policy_json)
