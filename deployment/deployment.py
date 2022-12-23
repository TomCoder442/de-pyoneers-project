# from script.docs_script import cw_policy_json, s3_log_policy_json
import zipfile
import logging
import boto3
from botocore.exceptions import ClientError
import time
import json
import os

class Lambda_script:
    timestamp = round(time.time())
    code_bucket = 'code-bucket-2022-12-21-1617'
    ingestion_bucket ='ingestion-bucket-2022-12-21-1617'
    processed_data_bucket = 'processed-data-bucket-2022-12-21-1617'
    # function_name = f'de_pyoneers_lambda_{timestamp}'
    aws_region = 'us-east-1'
    sts_client = boto3.client('sts')
    caller_identity = sts_client.get_caller_identity()
    aws_account = caller_identity['Account']
    aws_user_id = caller_identity['UserId']
    session = boto3.session.Session().region_name

    def __init__(self, lambda_function_path, zip_file_name, function_name_template, schedule):
        self.zip_file_name = zip_file_name
        self.lambda_function_path = lambda_function_path
        self.folder_name = function_name_template
        self.function_name = f'{function_name_template}-2022-12-21-1617'
        self.schedule = schedule
        # self.timestamp = round(time.time())
        
                
    def create_bucket(self, bucket_name, region='us-east-1'):
        s3 = boto3.client('s3')
        try: s3.create_bucket(Bucket=bucket_name)
        except ClientError as e:
            logging.error(e)
            print(e)
            return False
        return True

    def zipper(self, bucket=code_bucket):
        with zipfile.ZipFile(f'./{self.zip_file_name}', 'w') as zip:
            zip.write(self.lambda_function_path)
        s3 = boto3.client('s3')
        s3.upload_file(f'./{self.zip_file_name}', bucket, f'{self.function_name}/function.zip')

    def creating_cw_policy(self):
        iam = boto3.client('iam')

        # Customising the CLOUDWATCH POLICY from the json template and saving into a variable, then adding it to IAM
        # Creating the customised arns to paste into the cloudwatch policy template
        cw_region = f"arn:aws:logs:{self.aws_region}:{self.aws_account}:*"
        cw_resources = f"arn:aws:logs:{self.aws_region}:{self.aws_account}:log-group:/aws/lambda/{self.function_name}:*"
        # Loading the template policy into a variable
        with open("templates/cloudwatch_log_policy_template.json") as f:
            cw_policy_template = json.load(f)
        # Customising the template policy inside of the variable
        cw_policy_template["Statement"][0]["Resource"] = cw_region
        cw_policy_template["Statement"][1]["Resource"] = cw_resources
        # Fully customised cw_policy_template now exists in above variable

        # Creating the policy on IAM with the value of the customised template variable
        cw_creation_response = {"Policy":{"Arn": 'Default, will return this value if resource already existed on aws'}}

        try:
            cw_creation_response = iam.create_policy(
                PolicyName=f"cloudwatch_log_policy_{self.folder_name}{self.timestamp}",
                PolicyDocument=json.dumps(cw_policy_template)
            )
        except ClientError as e:
            print(e)
        
        return {'CW_creation_response': cw_creation_response, 'CW_policy_arn': cw_creation_response["Policy"]["Arn"]}

    def creating_s3_policy(self):
        iam = boto3.client('iam')

        # Customising the S3_read POLICY from the json template and saving into a variable, then adding it to IAM
        with open("templates/s3_read_policy_template.json", "r") as f:
            s3_read_policy_template = json.load(f)

        # Adding each of the buckets to the permissions policy template document and saving to a variable
        s3_read_policy_template["Statement"][0]["Resource"][0] = f"arn:aws:s3:::{self.code_bucket}/*"
        s3_read_policy_template["Statement"][0]["Resource"][1] = f"arn:aws:s3:::{self.ingestion_bucket}/*"
        s3_read_policy_template["Statement"][0]["Resource"][2] = f"arn:aws:s3:::{self.processed_data_bucket}/*"
        s3_read_policy_template["Statement"][0]["Resource"][3] = f"arn:aws:s3:::{self.ingestion_bucket}"
        s3_read_policy_template["Statement"][0]["Resource"][4] = f"arn:aws:s3:::{self.processed_data_bucket}"
        s3_read_policy_template["Statement"][0]["Resource"][5] = f"arn:aws:s3:::max-date-bucket-2022-12-21-1617/*"
        s3_read_policy_template["Statement"][0]["Resource"][6] = f"arn:aws:s3:::max-date-bucket-2022-12-21-1617"

        s3_creation_response = {"Policy":{"Arn": 'Default, will return this value of policy already existed on aws'}}
        try:
        # Creating the policy on IAM with the value of the customised template variable
            s3_creation_response = iam.create_policy(
                PolicyName=f"s3_read_policy_{self.timestamp}",
                PolicyDocument=json.dumps(s3_read_policy_template)
            )
        except ClientError as e:
            print(e)  

        return {'S3_creation_response': s3_creation_response, 'S3_policy_arn': s3_creation_response["Policy"]["Arn"]}

    def creating_the_execution_role(self):

        iam = boto3.client('iam')

        # Adding the trust policy (no need to customise) for the execution role to a variable and converting back to a json string
        with open('templates/trust_policy.json') as f:
            trust_policy = json.load(f)
        trust_policy_string = json.dumps(trust_policy)
        
        # Saving the execution role in IAM with trust policy attached
        saving_execution_role_to_iam_response = 'Default, will return this value of policy already existed on aws'

        try:
            saving_execution_role_to_iam_response = iam.create_role(
            RoleName=f"lambda-execution-role-{self.function_name}",
            AssumeRolePolicyDocument=trust_policy_string)
            
        except ClientError as e:
            print(e)

        execution_role = f'arn:aws:iam::{self.aws_account}:role/lambda-execution-role-{self.function_name}'

        return {'Saving_execution_role_to_iam_response': saving_execution_role_to_iam_response, 'ExecutionRole': execution_role}
    def attaching_policies_to_er(self):
        iam = boto3.client('iam')

        attaching_cw_policy_to_er_response = 'Default, will return this value of policy already existed on aws'
        attaching_s3_policy_to_er_response = 'Default, will return this value of policy already existed on aws'

        
        s3_policy_arn = f'arn:aws:iam::{self.aws_account}:policy/s3_read_policy_{self.timestamp}'
        cw_policy_arn = f'arn:aws:iam::{self.aws_account}:policy/cloudwatch_log_policy_{self.folder_name}{self.timestamp}'
        # s3_bucket_policy_arn = f'arn:aws:iam::{self.aws_account}:policy/s3_list_buckets_policy{self.timestamp}'
            
        
        try:
            attaching_s3_policy_to_er_response = iam.attach_role_policy(
                PolicyArn=s3_policy_arn,
                RoleName=f'lambda-execution-role-{self.function_name}'
            )
        except ClientError as e:
            print(e)

        try:
            attaching_cw_policy_to_er_response = iam.attach_role_policy(
                PolicyArn=cw_policy_arn,
                RoleName=f'lambda-execution-role-{self.function_name}'
            )
        except ClientError as e:
            print(e)

        return {'Attaching_s3_policy_to_er_response': attaching_s3_policy_to_er_response, 'Attaching_cw_policy_to_er_response': attaching_cw_policy_to_er_response}

    def create_lambda_function(self, bucket, lambda_function):
        lambda_client = boto3.client('lambda')

        response = 'Default, will return this value of policy already existed on aws'

        try:
            response = lambda_client.create_function(
                FunctionName=lambda_function,
                Runtime='python3.9',
                Role= f"arn:aws:iam::{self.aws_account}:role/lambda-execution-role-{self.function_name}",
                Handler= f'src/{self.folder_name}.lambda_handler',
                Code={
                    # 'ZipFile': open(deployment_package, 'rb')_log(),
                    'S3Bucket': bucket,
                    'S3Key': f"{lambda_function}/function.zip"
                })
        except ClientError as e:
            print(e)

        with open('confirmation.json', 'w') as f:
            json.dump(response, f)

        return response

    def layers(self):
        lambda_client = boto3.client('lambda')
        lambda_client.update_function_configuration(
        FunctionName= self.function_name,
        Layers=['arn:aws:lambda:us-east-1:770693421928:layer:Klayers-p39-psycopg2-binary:1', 'arn:aws:lambda:us-east-1:770693421928:layer:Klayers-p39-SQLAlchemy:7', 'arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python39:2' ]
    )

    def eventbridge_trigger(self):        
        eventbridge = boto3.client('events')

        rule_name = f'On{self.schedule}Minutes{self.function_name}'

        # The schedule expression that determines how often the rule is triggered. In
        # this case, the rule will be triggered every five minutes
        schedule_expression = f'rate({self.schedule} minutes)'

        put_rule_response = 'Default, will return this value if policy already existed on aws'
        put_rule_response = eventbridge.put_rule(
            Name=rule_name,
            ScheduleExpression=schedule_expression,
            State='ENABLED',
        )

    # Add the specified Lambda function as a target for the rule
        lambda_function_arn = f'arn:aws:lambda:{self.aws_region}:{self.aws_account}:function:{self.function_name}'

        eventbridge.put_targets(
            Rule=rule_name,
            Targets=[
                {
                    'Id': 'EVENTBRIDGE_TARGET_1',
                    'Arn': lambda_function_arn
                }
            ]
        )

        return put_rule_response

    def setting_eventbridge_permissions(self):
        lambda_client = boto3.client('lambda')
        # Grant permission to EventBridge to invoke the function via a schedule
        putting_eventbridge_permission_response = 'Default, will return this value if resource already existed on aws'
        
        try:
            putting_eventbridge_permission_response = lambda_client.add_permission(
                FunctionName=self.function_name,
                StatementId=f'EventBridgeInvokePermission{self.function_name}',
                Action='lambda:InvokeFunction',
                Principal='events.amazonaws.com',
                SourceArn=f'arn:aws:events:{self.aws_region}:{self.aws_account}:rule/On{self.schedule}Minutes{self.function_name}',
                )
        except ClientError as e:
            print(e)
        
        lambda_client.update_function_configuration(
            FunctionName=f'{self.function_name}',
            Timeout=300  # Increase the timeout to 300 seconds
        )

        return putting_eventbridge_permission_response

    def master(self):
        print("Creating buckets")
        self.create_bucket(self.code_bucket)
        time.sleep(2)
        print(f'Created code-bucket-2022-12-21-1617')
        
        self.create_bucket(self.ingestion_bucket)
        time.sleep(2)
        print(f'Created ingestion-bucket-2022-12-21-1617')

        self.create_bucket(self.processed_data_bucket)
        time.sleep(2)
        print(f'Created {self.processed_data_bucket}')

        self.create_bucket(f'max-date-bucket-2022-12-21-1617')
        time.sleep(2)
        print(f' Created max-date-bucket-2022-12-21-1617')
        time.sleep(2)

        print("All Buckets created > Now zipping lambda function")
        self.zipper()
        time.sleep(2)
        print(f'Lambda function has been zipped and added to the code bucket > Now creating cloudwatch_log_policy_{self.folder_name}{self.timestamp}')
        self.creating_cw_policy()
        time.sleep(2)
        print(f'cloudwatch_log_policy_{self.folder_name}{self.timestamp}has been added to Iam > Now creating s3_read_policy_{self.timestamp}')
        self.creating_s3_policy()
        time.sleep(2)
        print(f's3_read_policy_{self.timestamp} has been added to Iam > Now creating lambda-execution-role-{self.function_name}')
        self.creating_the_execution_role()
        time.sleep(7)
        print(f'lambda-execution-role-{self.function_name} has been added to Iam > Now attaching policies to lambda-execution-role-{self.function_name}')
        self.attaching_policies_to_er()
        time.sleep(8)
        print(f'cloudwatch_log_policy_{self.folder_name}{self.timestamp} and s3_read_policy_{self.timestamp} have been attached to lambda-execution-role-{self.function_name} > Now creating the lambda_function: {self.function_name}')
        self.create_lambda_function(self.code_bucket, self.function_name)
        time.sleep(6)
        print(f"Lambda_function: {self.function_name} has now been created and exists in the {self.code_bucket} > Now creating Lambda layers")
        self.layers()
        time.sleep(5)
        # This area could do with some certification
        print(f'Lambda layers have been created > Now creating the schedule to invoke {self.function_name} on eventbridge')
        self.eventbridge_trigger()
        time.sleep(5)
        print('Eventbridge schedule now in place, Now adding permissions to lambda which will allow {self.function_name} to be invoked by Eventbridge')
        self.setting_eventbridge_permissions()
        time.sleep(6)
        print(f'Permissions for eventbridge to invoke {self.function_name} have now been added >  view cloudwatch logs for more info.')
        
    def master2(self):
        self.zipper()
        time.sleep(2)
        print(f'Lambda function has been zipped and added to the code bucket > Now creating cloudwatch_log_policy_{self.folder_name}{self.timestamp}')
        self.creating_cw_policy()
        time.sleep(2)
        print(f'cloudwatch_log_policy_{self.timestamp} has been added to Iam > Now creating s3_read_policy_{self.timestamp}')
        self.creating_s3_policy()
        time.sleep(2)
        print(f's3_read_policy_{self.timestamp} has been added to Iam > Now creating lambda-execution-role-{self.function_name}')
        self.creating_the_execution_role()
        time.sleep(6)
        print(f'lambda-execution-role-{self.function_name} has been added to Iam > Now attaching policies to lambda-execution-role-{self.function_name}')
        self.attaching_policies_to_er()
        time.sleep(6)
        print(f'cloudwatch_log_policy_{self.timestamp} and s3_read_policy_{self.timestamp} have been attached to lambda-execution-role-{self.function_name} > Now creating the lambda_function: {self.function_name}')
        self.create_lambda_function(self.code_bucket, self.function_name)
        time.sleep(6)
        print(f"Lambda_function: {self.function_name} has now been created and exists in the {self.code_bucket} > Now creating Lambda layers")
        self.layers()
        time.sleep(5)
        # This area could do with some certification
        print(f'Lambda layers have been created > Now creating the schedule to invoke {self.function_name} on eventbridge')
        self.eventbridge_trigger()
        time.sleep(5)
        print('Eventbridge schedule now in place, Now adding permissions to lambda which will allow {self.function_name} to be invoked by Eventbridge')
        self.setting_eventbridge_permissions()
        time.sleep(6)
        print(f'Permissions for eventbridge to invoke {self.function_name} have now been added >  view cloudwatch logs for more info.')

extract_lambda = Lambda_script('src/extract.py', 'extract.zip', "extract", '5')
extract_lambda.master()
transform_lambda = Lambda_script('src/transform.py', 'transform.zip', "transform", '10')
transform_lambda.master2()
load_lambda = Lambda_script('src/load.py', 'load.zip', "load", '15')
load_lambda.master2()





































