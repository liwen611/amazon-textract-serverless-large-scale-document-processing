import json
import boto3
import os
from helper import AwsHelper
import time


def startJob(bucketName, objectName, documentId, snsTopic, snsRole, detectText = True, detectForms = True, detectTables = True):

    print("Starting job with documentId: {}, bucketName: {}, objectName: {}".format(documentId, bucketName, objectName))

    response = None
    client = AwsHelper().getClient('textract')
    if(not detectForms and not detectTables):
        response = client.start_document_text_detection(
            ClientRequestToken  = documentId,
            DocumentLocation={
                'S3Object': {
                    'Bucket': bucketName,
                    'Name': objectName
                }
            },
            NotificationChannel= {
              "RoleArn": snsRole,
              "SNSTopicArn": snsTopic
           },
           JobTag = documentId)
    else:
        features  = []
        if(detectTables):
            features.append("TABLES")
        if(detectForms):
            features.append("FORMS")

        response = client.start_document_analysis(
            ClientRequestToken  = documentId,
            DocumentLocation={
                'S3Object': {
                    'Bucket': bucketName,
                    'Name': objectName
                }
            },
            FeatureTypes=features,
            NotificationChannel= {
                  "RoleArn": snsRole,
                  "SNSTopicArn": snsTopic
               },
            JobTag = documentId)

    return response["JobId"]


def lambda_handler(event, context = None):
    # TODO implement
    records = event['Records']
    record = records[0]
    bucketName = "texttract-incoming"
    objectName = record['dynamodb']['NewImage']['objectName']['S']
    documentId = record['dynamodb']['NewImage']['documentId']['S']

    snsTopic = "arn:aws:sns:us-west-2:415596832415:AmazonTextractTopic"
    snsRole = "arn:aws:iam::415596832415:role/liwens_texttract_role"
    job_id = startJob(bucketName, objectName, documentId, snsTopic, snsRole, detectText = True, detectForms = True, detectTables = True)
    return {
        'statusCode': 202,
        'body': job_id
    }

if __name__ == "__main__":
    boto3.setup_default_session(profile_name = "dev")
    
    with open("../testdocs/dynamo_trigger.json", "r") as file:
        event = json.load(file)
    
    lambda_handler(event, context = None)