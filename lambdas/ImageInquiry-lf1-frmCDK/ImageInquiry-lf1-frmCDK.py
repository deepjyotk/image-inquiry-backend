import json
import boto3
import base64
import os
import datetime
from botocore.exceptions import ClientError
from elasticsearch import Elasticsearch, RequestsHttpConnection

LABEL_DETECTION_MIN_CONFIDENCE = 75

def parse_multipart_data(content_type, body_data):
    boundary = content_type.split("boundary=")[-1]
    body_data = base64.b64decode(body_data)
    parts = body_data.split(bytes('--' + boundary, 'utf-8'))
    
    parsed_parts = {}
    image_bytes = None
    
    for part in parts:
        if b'Content-Disposition: form-data;' in part:
            header_area, _, content = part.partition(b'\r\n\r\n')
            header_area = header_area.decode('utf-8')
            name_part = header_area.split('name="')[1].split('"')[0]
            
            if 'filename="' in header_area:
                image_bytes = content.rstrip(b'\r\n')
            else:
                parsed_parts[name_part] = content.decode('utf-8').rstrip('\r\n')
    
    return parsed_parts, image_bytes

def upload_to_s3(s3_client, bucket_name, object_name, image_bytes, metadata):
    try:
        response = s3_client.put_object(
            Bucket=bucket_name,
            Key=object_name,
            Body=image_bytes,
            Metadata=metadata
        )
        return response
    except ClientError as e:
        raise RuntimeError(f"Failed to upload to S3: {str(e)}")

def detect_labels(rkgn_client, bucket_name, object_name):
    try:
        response = rkgn_client.detect_labels(
            Image={"S3Object": {"Bucket": bucket_name, "Name": object_name}},
            MinConfidence=LABEL_DETECTION_MIN_CONFIDENCE
        )
        return [label["Name"] for label in response["Labels"]]
    except ClientError as e:
        raise RuntimeError(f"Failed to detect labels: {str(e)}")

def get_custom_labels(s3_client, bucket_name, object_name):
    try:
        response = s3_client.head_object(Bucket=bucket_name, Key=object_name)
        return response["Metadata"].get("customlabels", "")
    except ClientError as e:
        raise RuntimeError(f"Failed to retrieve custom labels: {str(e)}")

def index_to_opensearch(es_client, index_name, record):
    try:
        response = es_client.index(index=index_name, body=record)
        return response
    except Exception as e:
        raise RuntimeError(f"Failed to index to OpenSearch: {str(e)}")

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    rkgn_client = boto3.client('rekognition')
    
    try:
        content_type = event['headers']['Content-Type']
        body_data = event['body']
        
        parsed_parts, image_bytes = parse_multipart_data(content_type, body_data)
        
        bucket_name = 'imageinquiry-b2-frmcdk'
        object_name = parsed_parts.get('filename', 'filename')
        
        if image_bytes:
            upload_to_s3(
                s3_client, bucket_name, object_name, image_bytes,
                {'customlabels': parsed_parts.get('customlabels', '')}
            )
        
        labels = detect_labels(rkgn_client, bucket_name, object_name)
        
        custom_labels = get_custom_labels(s3_client, bucket_name, object_name)
        if custom_labels:
            labels.extend(custom_labels.split(','))
        
        es_client = Elasticsearch(
            hosts=[{'host': os.environ["OPENSEARCH_HOST_ENDPOINT"], 'port': 443}],
            http_auth=(os.environ['ESUSERNAME'], os.environ['ESPASSWORD']),
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )
        
        current_time = datetime.datetime.now().isoformat()
        record = {
            "bucket": bucket_name,
            "objectKey": object_name,
            "createdTimestamp": current_time,
            "labels": ' '.join(labels).lower()
        }
        
        index_to_opensearch(es_client, "photo-label", record)
        
        return {
            'statusCode': 200,
            'body': json.dumps('Hello from Lambda!')
        }

    except KeyError as e:
        return {'statusCode': 500, 'body': json.dumps(f'Key Error: {str(e)}')}
    except boto3.exceptions.Boto3Error as e:
        return {'statusCode': 500, 'body': json.dumps(f'AWS Service Error: {str(e)}')}
    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps(f'An unexpected error occurred: {str(e)}')}
