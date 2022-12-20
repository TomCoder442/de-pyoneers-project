import boto3
import json 


def cloudwatch_logs_creation():
    cw_region = f"arn:aws:logs:{aws_region}:{aws_account}:*"
    cw_resources = f"arn:aws:logs:{aws_region}:{aws_account}:log-group:/aws/lambda/{function_name}:*"

    # Create a logs client
    logs_client = boto3.client('logs')
    # Set the name of the log group you want to create
    log_group_name = f'/aws/lambda/{function}'

    # Customising the polocy doc
    create_cw_logs_group_response = logs_client.create_log_group(logGroupName=log_group_name)
    with open("templates/cloudwatch_read_policy_template.json", "r") as f:
            cloudwatch_read_policy_template = json.load(f)
    cloudwatch_read_policy_template["Statement"][0]["Resource"] = cw_region
    cloudwatch_read_policy_template["Statement"][1]["Resource"] = cw_resources
    policy_document = json.dumps(cloudwatch_read_policy_template)

    # Call the put_resource_policy method
    put_cw_resource_policy_response = logs_client.put_resource_policy(
        policyName='lambda-cw_logs',
        policyDocument=policy_document,
        logGroupName=log_group_name
    )

    return {'create_cw_logs_group_response': create_cw_logs_group_response, 'put_cw_resource_policy_response': put_cw_resource_policy_response}






EVENT BRIDGE 

event_pattern = {
    "source": ["aws.events"],
    "detail-type": ["Scheduled Event"],
    "detail": {
        "schedule": "rate(5 minutes)"
    }
}


import boto3

# Create an EventBridge client
eventbridge_client = boto3.client('eventbridge')

# Set the name of the rule and the ARN of the target Lambda function
rule_name = 'my-event-rule'
function_arn = f'arn:aws:lambda:{aws_region}:{aws_account}:function:{function_name}'

# Set the event pattern for the rule
event_pattern = {
    # SHOULD IT BE AWS.EVENTS OR AWS.LAMBDA ??

    "source": ["aws.events"], 
    "detail-type": ["Scheduled Event"],
    "detail": {
        "schedule": "rate(5 minutes)"
    }
}

# Create the rule and add the Lambda function as a target
response = eventbridge_client.put_rule(
    Name=rule_name,
    EventPattern=event_pattern,
    State='ENABLED',
    Targets=[
        {
            'Id': 'my-function-target',
            'Arn': function_arn,
            'Input': '{ "key": "value" }'
        }
    ]
)
