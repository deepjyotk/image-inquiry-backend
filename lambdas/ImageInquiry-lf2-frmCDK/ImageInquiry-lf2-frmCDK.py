import os
import json
import logging
import datetime
import random
import boto3
from opensearchpy.connection.http_requests import RequestsHttpConnection
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

import utils as opensearch_utils

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

headers = { "Content-Type": "application/json" }
region = 'us-east-1'
lex = boto3.client('lex-runtime', region_name=region)


def extract_nouns_after_keywords(text, keywords=["images of", "photos of"]):
    comprehend = boto3.client(service_name='comprehend', region_name='us-east-1')
    # Convert text to lowercase for case-insensitive matching
    lower_text = text.lower()

    # Find the position of any keyword
    keyword_position = -1
    for keyword in keywords:
        pos = lower_text.find(keyword)
        if pos != -1:
            keyword_position = pos
            keyword_length = len(keyword)
            break

    if keyword_position != -1:
        # Extract the substring after the keyword
        relevant_text = text[keyword_position + keyword_length:].strip()
    else:
        # Use the entire text if no keyword is found
        relevant_text = text.strip()
    
    # Call the detect_syntax method
    response = comprehend.detect_syntax(Text=relevant_text, LanguageCode='en')

    # Extract nouns
    nouns = [token['Text'] for token in response['SyntaxTokens'] if token['PartOfSpeech']['Tag'] in ['NOUN', 'PROPN']]
    return nouns

def lambda_handler(event, context):
    try:
        body = json.loads(event['body'])
        print("Hello from lambda")
        print("Event is: ", event)
        # Example Prompts
        prompt = f"{body['query']}"
        custom_label  = body['custom_label']
        custom_label_list = custom_label.split(',')
        custom_label_list = [item.strip() for item in custom_label_list]
        
        
        # Apply the function to each prompt
        nouns = extract_nouns_after_keywords(prompt)
        print(f"Prompt: '{prompt}' -> Nouns: '{nouns}'")
        
        combined_list = custom_label_list+nouns
        combined_list = list(set(combined_list))
        print("combined_list is: ", combined_list)

        # labels_list  = [item.strip() for item in labels.split(',')]
        if len(combined_list) != 0:
            img_paths = get_photo_path(combined_list)
            print(f"img_paths is: {img_paths}")
        
        return {
        'headers': {
            "Access-Control-Allow-Origin" : "*", 
        },
            'statusCode': 200,
            'body': json.dumps( list(img_paths))
        }
    
    except KeyError as e:
        logger.error(f"Key error: {str(e)} - Event may not have the expected structure.")
        return {'statusCode': 500, 'body': json.dumps('Key Error!')}
    except boto3.exceptions.Boto3Error as e:
        logger.error(f"AWS Boto3 error: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps('AWS Service Error!')}
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps('An unexpected error occurred.')}


def get_photo_path(word_list):
    host = "search-imageinquiry-domain-d67sch3jzpmlzyp65dcc34notu.aos.us-east-1.on.aws"
    auth = ("deepjyot", "Deep@123")

    try:
        # Initialize Elasticsearch client
        es = Elasticsearch(
            hosts=[{'host': host, 'port': 443}],
            use_ssl=True,
            http_auth=auth,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )

        # Check connection
        if not es.ping():
            logger.error("Could not connect to Elasticsearch")
            return []

        logger.info(f"Connected to OpenSearch at {host}")

        # Create the query
        should_clauses = [{"match": {"labels": word.lower()}} for word in word_list]
        query = {
            "query": {
                "bool": {
                    "should": should_clauses
                }
            }
        }

        INDEX_NAME = "photo-label"

        # Perform the search
        response = es.search(index=INDEX_NAME, body=query)
        logger.info("Search query executed successfully")

        # Process the response
        output = []
        hits = response.get('hits', {}).get('hits', [])

        for hit in hits:
            source = hit.get('_source', {})
            bucket = source.get('bucket')
            object_key = source.get('objectKey')
            if bucket and object_key:
                entire_obj_path = f"https://{bucket}.s3.amazonaws.com/{object_key}"
                if entire_obj_path not in output:
                    output.append(entire_obj_path)

        return output

    except Exception as e:
        logger.error(f"Error occurred: {e}")
        return []