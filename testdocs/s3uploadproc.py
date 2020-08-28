# The following demo how to use presigned url to upload s3 object 
# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html

import logging
import boto3
import json
from botocore.exceptions import ClientError

boto3.setup_default_session(profile_name = "dev")
s3_client = boto3.client('s3')

# the recommended method to upload file through an url is through a POST request
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

# simuating making the POST request with python code     
bucket_name = "texttract-incoming"
object_name = "url_test4/random.pdf"
document_id = "12345"
# fields = {
#     "document_id": document_id}
#conditions = [{"x-amz-meta-document_id": document_id}]

#url_response = create_presigned_post(bucket_name, object_name, conditions = conditions)
url_response = create_presigned_post(bucket_name, object_name)
if url_response is None:
    exit(1)

# Demonstrate how another Python program can use the presigned URL to upload a file
import requests 

file_name = "/Users/lhuang/Project/aws-texttract-research/input_data/images/paystub1.png"

#response['fields'].pop("document_id")
with open(file_name, 'rb') as f:
    files = {'file': (file_name, f)}
    http_response = requests.post(
        url_response['url'], 
        data=url_response['fields'], 
        headers = {"x-amz-meta-document_id": document_id}, # send the metadata through headers
        files=files)
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



# when there are extra parameters to be passed in, use the extended method 
object_name = "url_test6/random.pdf"
bucket_name = "texttract-incoming"
file_name = "/Users/lhuang/Project/aws-texttract-research/input_data/images/paystub1.png"
Metadata={
        'document_id': '12345'
    }
           
upload_file_parameters = {
    "Key": object_name,
    "Bucket": bucket_name,
    "Body": file_name,
    "Metadata": Metadata
    }

put_url = s3_client.generate_presigned_url(
    client_method_name = "put_object", 
    method_parameters=upload_file_parameters,
    http_method=None)

# use request to perform the put action 
import requests

file_name = "/Users/lhuang/Project/aws-texttract-research/input_data/images/paystub1.png"

with open(file_name, 'rb') as f:
    files = {'file': (file_name, f)}
    put_response = requests.put(put_url, data=files, headers = {'document_id': '12345'})
# If successful, returns HTTP status code 204


