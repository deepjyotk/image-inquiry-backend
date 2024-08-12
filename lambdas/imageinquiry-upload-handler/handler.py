import json
import os
import logging
import requests
from datetime import datetime
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests.auth import HTTPBasicAuth
from requests_toolbelt.multipart import decoder

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def update_document(user_id, object_key, final_labels):
    logger.info("Starting the document update process.")
    
    es_label = f"photo-label-{user_id}"
    # Define the search endpoint
    search_url = f'https://{os.environ["OPENSEARCH_HOST_ENDPOINT"]}/{es_label}/_search'
    logger.info(f"Search URL: {search_url}")
    
    # Define the search query
    search_query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"user_id": user_id}},
                    {"match": {"objectKey": object_key}}
                ]
            }
        }
    }
    logger.info(f"Search query: {json.dumps(search_query, indent=2)}")
    
    # Perform the search to get the document ID
    search_response = requests.get(search_url, headers={"Content-Type": "application/json"}, 
                                   data=json.dumps(search_query), 
                                   auth=HTTPBasicAuth(os.environ["ESUSERNAME"], os.environ["ESPASSWORD"]))
    
    logger.info(f"Search response status: {search_response.status_code}")
    search_response_data = search_response.json()
    logger.info(f"Search response data: {json.dumps(search_response_data, indent=2)}")
    
    if search_response_data['hits']['total']['value'] > 0:
        doc_id = search_response_data['hits']['hits'][0]['_id']
        logger.info(f"Document ID: {doc_id}")
        
        # Define the update endpoint
        update_url = f'https://{os.environ["OPENSEARCH_HOST_ENDPOINT"]}/{es_label}/_update/{doc_id}'
        logger.info(f"Update URL: {update_url}")
        
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"Current timestamp: {current_timestamp}")
        
        # Define the update payload
        update_payload = {
            "doc": {
                "final-labels": final_labels,
                "timestamp": current_timestamp
            }
        }
        logger.info(f"Update payload: {json.dumps(update_payload, indent=2)}")
        
        # Perform the update
        update_response = requests.post(update_url, headers={"Content-Type": "application/json"},
                                        data=json.dumps(update_payload), 
                                        auth=HTTPBasicAuth(os.environ["ESUSERNAME"], os.environ["ESPASSWORD"]))
        logger.info(f"Update response status: {update_response.status_code}")
        update_response_data = update_response.json()
        logger.info(f"Update response data: {json.dumps(update_response_data, indent=2)}")
        
        # Return the response
        return update_response_data
    else:
        logger.error("Document not found")
        return {"error": "Document not found"}
        
def fetch_item_from_dynamoDB(user_id, image_id):
    # Initialize a session using Amazon DynamoDB
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    
    # Select your DynamoDB table
    table = dynamodb.Table('imageinquiry-images')

    # Query the table using both user_id and image_id
    response = table.get_item(
        Key={
            'user_id': user_id,
            'image_id': image_id
        }
    )

    return response.get('Item')

def lambda_handler(event, context):
    if isinstance(event, str):
        event = json.loads(event)
    
    logger.info("Lambda function invoked")
    logger.info(f"Event received: {event}")
    
    try:
        user_sub = event.get('requestContext', {}).get('authorizer', {}).get('claims', {}).get('sub',{})
        logger.info(f"User sub is: {user_sub}")
        
        fetch_item_from_dynamoDB(image_id, user_id)
        
        # Handling multipart/form-data
        content_type = event['headers'].get('Content-Type', '')
        body = event['body']
        if isinstance(body, str):
            body = body.encode('utf-8')
        
        # Decode multipart form data
        multipart_data = decoder.MultipartDecoder(body, content_type)
        
        image_id = None
        final_labels = None
        
        for part in multipart_data.parts:
            content_disposition = part.headers[b'Content-Disposition'].decode('utf-8')
            
            if 'name="image_id"' in content_disposition:
                image_id = part.text
                logger.info(f"Extracted image_id: {image_id}")
            
            if 'name="final_labels"' in content_disposition:
                final_labels = part.text
                logger.info(f"Extracted final_labels: {final_labels}")
        
        upload_date = datetime.now().isoformat()
        logger.info(f'Upload Date: {upload_date}')
        
        # Initialize Elasticsearch client
        es_client = Elasticsearch(
            hosts=[{'host': os.environ["OPENSEARCH_HOST_ENDPOINT"], 'port': 443}],
            http_auth=(os.environ['ESUSERNAME'], os.environ['ESPASSWORD']),
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )
        logger.info('Connected to OpenSearch')
        
        # Update the document in OpenSearch
        update_document(user_sub, image_id, final_labels)
        logger.info('Record updated in OpenSearch')
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Record updated successfully'})
        }
    except KeyError as e:
        logger.error('Key Error: %s', str(e))
        return {'statusCode': 500, 'body': json.dumps(f'Key Error: {str(e)}')}
    except Exception as e:
        logger.error('An unexpected error occurred: %s', str(e))
        return {'statusCode': 500, 'body': json.dumps(f'An unexpected error occurred: {str(e)}')}