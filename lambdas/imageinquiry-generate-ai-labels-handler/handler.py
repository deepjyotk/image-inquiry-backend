import json
import boto3
import base64
import os
import datetime
import logging
import uuid
from botocore.exceptions import ClientError
from elasticsearch import Elasticsearch, RequestsHttpConnection

LABEL_DETECTION_MIN_CONFIDENCE = 75

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
            
            # logger.info(f"header_area is: {header_area}")
            # logger.info(f"_ is: {_}")
            # logger.info(f"name_part: {name_part}")
            # logger.info(f"content is: {header_area}")
            
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



def insert_item(user_id, s3_path, image_id, filename, caption):
    # Initialize a session using Amazon DynamoDB
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    
    # Select your DynamoDB table
    table = dynamodb.Table('imageinquiry-images')

    # Insert item into the table with an empty tags list
    response = table.put_item(
        Item={
            'user_id': user_id,
            'image_id': image_id,
            's3-path': s3_path,
            'filename': filename,
            'caption': caption,
            'tags': [],  # Initially empty,
            'image_status':'AI_LABELS_GENERATED'
        }
    )
    
    return response


def lambda_handler(event, context):
    logger.info('Event: %s', json.dumps(event))
    s3_client = boto3.client('s3')
    rkgn_client = boto3.client('rekognition')
    user_id = event.get('requestContext', {}).get('authorizer', {}).get('claims', {}).get('sub', {})
    logger.info(f"user_id is: {user_id}")
    
    try:
        content_type = event['headers']['Content-Type']
        logger.info('Content-Type: %s', content_type)
        body_data = event['body']
        logger.info('Body data received')
        
        parsed_parts, image_bytes = parse_multipart_data(content_type, body_data)
        logger.info('Parsed parts: %s', parsed_parts)
        filename = parsed_parts.get('filename')
        logger.info(f"filename is: {filename}")
        
        bucket_name = 'imageinquiry-images'
        
        image_id = str(uuid.uuid4())
        object_name = f"{user_id}/{image_id}"
        logger.info('Object name: %s', object_name)
        
        isS3UploadSuccessful = False
        if image_bytes:
            upload_to_s3(
                s3_client, bucket_name, object_name, image_bytes,
                {'customlabels': parsed_parts.get('customlabels', '')}
            )
            isS3UploadSuccessful = True
            logger.info('Image uploaded to S3')
        
        if not isS3UploadSuccessful:
            return {'statusCode': 500, 'body': json.dumps('An unexpected error occurred.')}
        
        labels = detect_labels(rkgn_client, bucket_name, object_name)
        
        
        response = insert_item(
            user_id=f"{user_id}",
            s3_path = f"{bucket_name}/{object_name}",
            image_id=f"{image_id}",
            filename=f"{filename}",
            caption=""
        )
        logger.info("Insert Item Response: %s", response)
        
        return {
            'statusCode': 200,
            'body': json.dumps({'labels': labels, 'image_id': image_id})
        }

    except KeyError as e:
        logger.error('Key Error: %s', str(e))
        return {'statusCode': 500, 'body': json.dumps(f'Key Error: {str(e)}')}
    except boto3.exceptions.Boto3Error as e:
        logger.error('AWS Service Error: %s', str(e))
        return {'statusCode': 500, 'body': json.dumps(f'AWS Service Error: {str(e)}')}
    except Exception as e:
        logger.error('An unexpected error occurred: %s', str(e))
        return {'statusCode': 500, 'body': json.dumps(f'An unexpected error occurred: {str(e)}')}