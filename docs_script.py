import json
# from main_script import aws_region, aws_account_id, function_name, code_bucket, incoming_data_bucket

# USE THESE VARIABLE VALUES FOR TESTING WHEN NOT CONNECTED TO AWS SANDBOX
aws_region ='a'
aws_account_id = 'b'
function_name ='0'
code_bucket = 'c'
incoming_data_bucket = 'd'

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

s3_event_config = {"LambdaFunctionConfigurations": [
                        {
                        "LambdaFunctionArn": f'"arn:aws:lambda:{aws_region}:{aws_account_id}:function:{function_name}"',
                        "Events": ["s3:ObjectCreated:*"]
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
                                            f'"arn:aws:s3:::{incoming_data_bucket}/*"'
                                        ]
                                    }
                                ]
                            }

# Encode the data as a JSON string
cw_policy_json = json.dumps(cloud_watch_log_policy, indent=4)
print(cw_policy_json)
with open("cloudwatch_policy.json", "w") as f:
  # Use the json.dump() method to convert the Python object into a JSON object
  json.dump(cw_policy_json, f)
