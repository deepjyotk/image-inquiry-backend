import json
import os
import logging
import requests
from datetime import datetime
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests.auth import HTTPBasicAuth
import boto3

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def insert_document(user_id, s3_path, final_labels):
    logger.info("Starting the document insertion process.")
    
    es_label = f"photo-label-{user_id}"
    logger.info(f"ES label: {es_label}")
    
    # Define the index endpoint
    index_url = f'https://{os.environ["OPENSEARCH_HOST_ENDPOINT"]}/{es_label}/_doc'
    logger.info(f"Index URL: {index_url}")
    
    current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"Current timestamp: {current_timestamp}")
    
    # Define the document payload
    document_payload = {
        "user_id": user_id,
        "s3-path": s3_path,
        "final-labels": final_labels,
        "timestamp": current_timestamp
    }
    logger.info(f"Document payload: {json.dumps(document_payload, indent=2)}")
    
    # Perform the document insertion
    insert_response = requests.post(index_url, headers={"Content-Type": "application/json"},
                                    data=json.dumps(document_payload), 
                                    auth=HTTPBasicAuth(os.environ["ESUSERNAME"], os.environ["ESPASSWORD"]))
    logger.info(f"Insert response status: {insert_response.status_code}")
    logger.info(f"Insert response content: {insert_response.text}")
    
    insert_response_data = insert_response.json()
    logger.info(f"Insert response data: {json.dumps(insert_response_data, indent=2)}")
    
    # Return the response
    return insert_response_data

def fetch_item_from_dynamoDB(user_id, image_id):
    logger.info(f"Fetching item from DynamoDB with user_id: {user_id}, image_id: {image_id}")
    
    # Initialize a session using Amazon DynamoDB
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    logger.info("Initialized DynamoDB resource")
    
    # Select your DynamoDB table
    table = dynamodb.Table('imageinquiry-images')
    logger.info(f"Selected DynamoDB table: {table.name}")

    # Query the table using both user_id and image_id
    response = table.get_item(
        Key={
            'user_id': user_id,
            'image_id': image_id
        }
    )
    logger.info(f"Response from DynamoDB: {json.dumps(response, indent=2)}")

    return response.get('Item')

def update_dynamodb(user_id, image_id, final_labels):
    logger.info(f"Updating DynamoDB with user_id: {user_id}, image_id: {image_id}")
    
    # Initialize a session using Amazon DynamoDB
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    logger.info("Initialized DynamoDB resource")
    
    # Select your DynamoDB table
    table = dynamodb.Table('imageinquiry-images')
    logger.info(f"Selected DynamoDB table: {table.name}")

    # Sort final labels
    sorted_tags = sorted(final_labels)
    logger.info(f"Sorted tags: {sorted_tags}")
    
    # Update the table
    response = table.update_item(
        Key={
            'user_id': user_id,
            'image_id': image_id
        },
        UpdateExpression="SET image_status = :status, tags = :tags",
        ExpressionAttributeValues={
            ':status': 'SAVED',
            ':tags': sorted_tags
        },
        ReturnValues="UPDATED_NEW"
    )
    logger.info(f"DynamoDB update response: {json.dumps(response, indent=2)}")

    return response

def lambda_handler(event, context):
    logger.info("Lambda function invoked")
    logger.info(f"Event received: {event}")
    
    try:
        if isinstance(event, str):
            event = json.loads(event)
            logger.info(f"Event after JSON parsing: {event}")
        
        # Ensure the event is a dictionary
        if not isinstance(event, dict):
            raise TypeError("Event is not a dictionary")
        
        request_context = event.get('requestContext', {})
        logger.info(f"Request context: {request_context}")
        
        authorizer = request_context.get('authorizer', {})
        logger.info(f"Authorizer: {authorizer}")
        
        claims = authorizer.get('claims', {})
        logger.info(f"Claims: {claims}")
        
        user_sub = claims.get('sub', {})
        logger.info(f"User sub: {user_sub}")
        
        body = event.get("body", {})
        logger.info(f"Body: {body}")
        
        if isinstance(body, str):
            body = json.loads(body)
            logger.info(f"Body after JSON parsing: {body}")
        
        image_id = body.get("image_id")
        logger.info(f"Image ID: {image_id}")
        
        final_labels = body.get("final_labels")
        logger.info(f"Final Labels: {final_labels}")
        
        item = fetch_item_from_dynamoDB(user_sub, image_id)
        logger.info(f"Item fetched from DynamoDB: {json.dumps(item, indent=2)}")
        
        upload_date = datetime.now().isoformat()
        logger.info(f'Upload Date: {upload_date}')
        
        # Insert the document in OpenSearch
        insert_response = insert_document(user_sub, item['s3-path'], final_labels)
        logger.info(f'Insert response from OpenSearch: {json.dumps(insert_response, indent=2)}')
        
        # Update DynamoDB with new status and sorted tags
        update_response = update_dynamodb(user_sub, image_id, final_labels)
        logger.info(f'Update response from DynamoDB: {json.dumps(update_response, indent=2)}')
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Record inserted and DynamoDB updated successfully'})
        }
    except TypeError as e:
        logger.error('Type Error: %s', str(e))
        return {'statusCode': 500, 'body': json.dumps(f'Type Error: {str(e)}')}
    except KeyError as e:
        logger.error('Key Error: %s', str(e))
        return {'statusCode': 500, 'body': json.dumps(f'Key Error: {str(e)}')}
    except json.JSONDecodeError as e:
        logger.error('JSON Decode Error: %s', str(e))
        return {'statusCode': 500, 'body': json.dumps(f'JSON Decode Error: {str(e)}')}
    except Exception as e:
        logger.error('An unexpected error occurred: %s', str(e))
        return {'statusCode': 500, 'body': json.dumps(f'An unexpected error occurred: {str(e)}')}
