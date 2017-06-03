import json
import boto3
import sys
import botocore
from boto3.session import Session
from boto3.dynamodb.conditions import Attr, Key

print('Loading function')

BUCKETNAME = 'tianzhui-web/'

def dynamodb_put_item(filename, s3_object_url, each_record):
    dynamodb = boto3.resource(
        'dynamodb',
        region_name='us-east-1',
        )
    table = dynamodb.Table('tianzhui-img')
    response = table.put_item(
        Item={
            'filename': filename,  # pk = filename
            'url': s3_object_url,  # url
            's3_origin_put_record': each_record,
        },
    )
    return response

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    # read each metadata(record) produced by s3 put
    # now the list always has only one record, until bulk upload is enabled
    for each_record in event['Records']:
        print(each_record)
        objectkey = each_record['s3']['object']['key']
        if '/' in objectkey:
            # if objectkey incl. prefix, then strip prefix
            # e.g. 'key': 'web/Auto_Scaling_Lifecycle_Hooks_00.jpg'
            filename = objectkey.split('/')[1]
        else:
            # if objectkey doesn't incl. prefix, then use directly
            # e.g. 'key': 'HappyFace.jpg'
            filename = objectkey
        s3_object_url = 'https://s3.amazonaws.com/' + BUCKETNAME + objectkey
        print(s3_object_url)
        dynamodb_put_item(filename, s3_object_url, each_record)
    return event['Records']  # Echo back the first key value
    #raise Exception('Something went wrong')
