import logging
import boto3
import json
from botocore.exceptions import ClientError

boto3.setup_default_session(profile_name = "dev")

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
        logging.error(e)
        return None

    # The response contains the presigned URL and required fields
    return response

bucket_name = "texttract-incoming"
object_name = "random3.png"

response = create_presigned_post(bucket_name, object_name)
if response is None:
    exit(1)

# Demonstrate how another Python program can use the presigned URL to upload a file
import requests 

object_name = "/Users/lhuang/Project/aws-texttract-research/input_data/images/paystub1.png"

with open(object_name, 'rb') as f:
    files = {'file': (object_name, f)}
    http_response = requests.post(response['url'], data=response['fields'], files=files)
# If successful, returns HTTP status code 204
logging.info(f'File upload HTTP status code: {http_response.status_code}')

# alternative is to use 
# s3_client = boto3.client('s3')

# url = s3_client.generate_presigned_url(
#     ClientMethod = "put_object", 
#     Params = {
#         "Bucket": bucket_name, 
#         "Key": object_name}, 
#     ExpiresIn = 3600,
#     HttpMethod='PUT'
#     )

# in terminal 
# curl --request PUT --upload-file file.ext http://your-pre-signed-url.com