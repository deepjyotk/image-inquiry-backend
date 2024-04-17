import os
import json
import logging


import datetime
import boto3
from opensearchpy.connection.http_requests import RequestsHttpConnection
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth


logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    LABEL_DETECTION_MIN_CONFIDENCE = 99
    logger.info("Hello from LambdaHandler")
    logger.info(f"Event received: {event}")
    print(f"event received is: {event}")

    try:
        print("***----------------")
        print("yar I've finally changed lf1***----------------")
        print("yar I've finally changed yar lf1***----------------")
        # Extract details from event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        obj_key = event['Records'][0]['s3']['object']['key']
        logger.info(f"Extracted Bucket: {bucket_name}, Key: {obj_key}")

        # Call Rekognition
        rkgn_client = boto3.client('rekognition')
        rkgn_response = rkgn_client.detect_labels(
            Image={
                "S3Object": {
                    "Bucket": bucket_name,
                    "Name": obj_key
                }
            },
            MinConfidence=LABEL_DETECTION_MIN_CONFIDENCE
        )
        print(f"Rekognition response received with {(rkgn_response['Labels'])} labels")
        
        labels = [label_group["Name"] for label_group in rkgn_response["Labels"]]
        print(f"Labels is: {labels}")
        
        # Get custom labels from S3 metadata
        s3 = boto3.client("s3", region_name='us-east-1')
        s3_resp = s3.head_object(Bucket=bucket_name, Key=obj_key)
        custom_labels = s3_resp["Metadata"].get("customlabels", "")
        logger.info(f"Custom labels retrieved: {custom_labels}")

        # Prepare for OpenSearch index
        host = os.environ.get("OPENSEARCH_HOST_ENDPOINT")
        esUsername = os.environ['esUsername']
        esPassword = os.environ['esPassword']
        logger.debug(f"OpenSearch credentials retrieved: {esUsername}")
        
        
        if len(custom_labels) > 0:
            labels.extend(custom_labels) 

        auth = (esUsername, esPassword)
        INDEX_NAME = "photo-label"
        esEndPoint = os.environ["OPENSEARCH_HOST_ENDPOINT"]

        es = Elasticsearch(
            hosts=[{'host': esEndPoint, 'port': 443}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )
        es.info()
        logger.info(f"Connected to OpenSearch at {esEndPoint}")

        # Insert data into OpenSearch
        current_time = datetime.datetime.now().isoformat()
        logger.info(f"Current timestamp: {current_time}")
        record = {
            "bucket": bucket_name,
            "objectKey": obj_key,
            "createdTimestamp": current_time,
            "labels": ' '.join(labels).lower()
        }
        print(f"Record getting stored in os is: {record}")
        es_response = es.index(index=INDEX_NAME, body=record)
        logger.info(f"Document indexed in OpenSearch with response: {es_response}")

    except KeyError as e:
        logger.error(f"Key error: {str(e)} - Event may not have the expected structure.")
        return {'statusCode': 500, 'body': json.dumps('Key Error!')}
    except boto3.exceptions.Boto3Error as e:
        logger.error(f"AWS Boto3 error: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps('AWS Service Error!')}
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps('An unexpected error occurred.')}

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }