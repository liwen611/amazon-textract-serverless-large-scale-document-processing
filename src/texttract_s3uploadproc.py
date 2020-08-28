import logging
import boto3
import json
import os
from botocore.exceptions import ClientError
import uuid

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

def create_presigned_url_expanded(client_method_name, method_parameters=None,
                                  expiration=3600, http_method=None):
    """Generate a presigned URL to invoke an S3.Client method

    Not all the client methods provided in the AWS Python SDK are supported.

    :param client_method_name: Name of the S3.Client method, e.g., 'list_buckets'
    :param method_parameters: Dictionary of parameters to send to the method
    :param expiration: Time in seconds for the presigned URL to remain valid
    :param http_method: HTTP method to use (GET, etc.)
    :return: Presigned URL as string. If error, returns None.
    """

    # Generate a presigned URL for the S3 client method
    s3_client = boto3.client('s3')
    try:
        response = s3_client.generate_presigned_url(ClientMethod=client_method_name,
                                                    Params=method_parameters,
                                                    ExpiresIn=expiration,
                                                    HttpMethod=http_method)
    except ClientError as e:
        logging.error(e)
        return None

    # The response contains the presigned URL
    return response



def lambda_handler(event, context):
    logger.info(f"this is the orignal event: {event}")
    
    bucketName = os.getenv("texttract_bucket", "texttract-incoming")
    fileName = event["queryStringParameters"]["file"]
    userEmail = event["queryStringParameters"]["email"]
    documentId = str(uuid.uuid1())
    
    Metadata = {
        "document_id" : documentId,
        "user_email": userEmail
    }    
    Parameters = {
        "Key": fileName,
        "Bucket": bucketName,
        "Metadata": Metadata
        }

    if os.getenv("url_method") == "post":
        response = create_presigned_post(bucketName, fileName)
    else:
        response = create_presigned_url_expanded(client_method_name = "put_object", method_parameters=Parameters,
                             http_method="PUT")
    
    return {
        'statusCode': 200,
        'headers': { 'Content-Type': 'application/json' },
        'body': json.dumps({'response': response})
        }

if __name__ == "__main__":
    with open("../events/api_request.json", "r") as file:
        event = json.load(file)

    print("function return: ", lambda_handler(event, context = None))



