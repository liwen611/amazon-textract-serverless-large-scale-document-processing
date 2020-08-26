import logging
import boto3
import json
import os
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def create_presigned_post(bucket_name, object_name,
                          fields=None, conditions=None, expiration=3600):
    """Generate a presigned URL S3 POST request to upload a file

    :param bucket_name: string
    :param object_name: string
    :param fields: Dictionary of prefilled form fields
    :param conditions: List of conditions to include in the policy
    :param expiration: Time in seconds for the presigned URL to remain valid
    :return: Dictionary with the following keys:
        url: URL to post to
        fields: Dictionary of form fields and values to submit with the POST
    :return: None if error.
    """

    # Generate a presigned S3 POST URL
    s3_client = boto3.client('s3')
    try:
        response = s3_client.generate_presigned_post(bucket_name,
                                                     object_name,
                                                     Fields=fields,
                                                     Conditions=conditions,
                                                     ExpiresIn=expiration)
    except ClientError as e:
        logger.error(e)
        return None

    # The response contains the presigned URL and required fields
    return response

def lambda_handler(event, context):
    logger.info(f"this is the orignal event: {event}")
    
    #bucket_name = event["queryStringParameters"]["bucket"]
    bucket_name = os.environ.get("texttract_bucket", "texttract-incoming")
    file_name = event["queryStringParameters"]["file"]
    # bucket_name = "texttract-incoming"
    # file_name = "random.pdf"
    
    response = create_presigned_post(bucket_name, file_name)
    if response is None:
        exit(1)
    
    # return response
    # url = s3_client.generate_presigned_url(
    # ClientMethod = "put_object", 
    # Params = {
    #     "Bucket": bucket_name, 
    #     "Key": object_name}, 
    # ExpiresIn = 3600,
    # HttpMethod='PUT'
    # )
    
    return {
        'statusCode': 200,
        'headers': { 'Content-Type': 'application/json' },
        'body': json.dumps({'response': response})
    }