import json
import os
import uuid
import urllib
import datastore
from helper import FileHelper

#TODO this it a patch, add the functionality to retrieve metadata in the helper module
import boto3

def processRequest(request):

    output = ""

    print("request: {}".format(request))

    bucketName = request["bucketName"]
    objectName = request["objectName"]
    documentsTable = request["documentsTable"]
    outputTable = request["outputTable"]
    documentId = request["documentId"]
    userEmail = request["userEmail"] 

    print("Input Object: {}/{} with document_id {} created by user_email{}".format(bucketName, objectName, documentId, userEmail))

    ext = FileHelper.getFileExtenstion(objectName.lower())
    print("Extension: {}".format(ext))

    if(ext in ["jpg", "jpeg", "png", "pdf", ""]):
        if not documentId: 
            documentId = str(uuid.uuid1())
        ds = datastore.DocumentStore(documentsTable, outputTable)
        ds.createDocument(documentId, bucketName, objectName)

        output = "Saved document {} for {}/{}".format(documentId, bucketName, objectName)

        print(output)

    return {
        'statusCode': 200,
        'body': json.dumps(output)
    }

def lambda_handler(event, context):

    print("event: {}".format(event))

    request = {}
    request["bucketName"] = event['Records'][0]['s3']['bucket']['name']
    request["objectName"] = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    request["documentsTable"] = os.getenv('DOCUMENTS_TABLE', 'texttract_documents_table')
    request["outputTable"] = os.getenv('OUTPUT_TABLE', 'texttract_output_table')

    # retrieve the document id and the user email 
    s3_resource = boto3.resource('s3')
    s3_object = s3_resource.Object(request["bucketName"],request["objectName"])

    document_id = s3_object.metadata.get("document_id")
    user_email = s3_object.metadata.get("user_email")

    request["documentId"] = document_id  
    request["userEmail"] = user_email

    return processRequest(request)

if __name__ == "__main__":
    with open("../events/s3_meta_trigger.json", "r") as file:
        event = json.load(file)
    
    lambda_handler(event, context = None)
