import json
import os
import uuid
import urllib
import datastore
from helper import FileHelper

def processRequest(request):

    output = ""

    print("request: {}".format(request))

    bucketName = request["bucketName"]
    objectName = request["objectName"]
    documentsTable = request["documentsTable"]
    outputTable = request["outputTable"]

    print("Input Object: {}/{}".format(bucketName, objectName))

    ext = FileHelper.getFileExtenstion(objectName.lower())
    print("Extension: {}".format(ext))

    if(ext in ["jpg", "jpeg", "png", "pdf", ""]):
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

    return processRequest(request)

if __name__ == "__main__":
    with open("../testdocs/s3_woext_trigger.json", "r") as file:
        event = json.load(file)
    
    lambda_handler(event, context = None)
