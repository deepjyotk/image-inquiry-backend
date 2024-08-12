import json
import logging
import datetime
import boto3
from elasticsearch import Elasticsearch
from boto3.dynamodb.conditions import Key
from opensearchpy import RequestsHttpConnection

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

headers = { "Content-Type": "application/json" }
region = 'us-east-1'
lex = boto3.client('lex-runtime', region_name=region)

# Assuming the S3 bucket name is 'imageinquiry-images'
S3_URL_PREFIX = "https://s3.amazonaws.com/"

def isRequestRateLimited(user_id, allowed_requests, time_window):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('image-inquiry-rate-limiter')
    
    # Current timestamp in seconds as a string
    current_timestamp = str(int(datetime.datetime.now().timestamp()))
    window_start_timestamp = str(int(datetime.datetime.now().timestamp()) - time_window)

    # Fetch requests within the current sliding window
    response = table.query(
        KeyConditionExpression=Key('userId').eq(user_id) & 
                               Key('requestTimestamp').gt(window_start_timestamp)
    )
    print("Response from Rate Limiter table: ", response)

    request_count = len(response['Items'])

    if request_count < allowed_requests:
        # Allow the request and record the new timestamp
        table.put_item(
            Item={
                'userId': user_id,
                'requestTimestamp': current_timestamp
            }
        )
        return False, request_count, allowed_requests - request_count - 1, time_window
    else:
        # Deny the request
        return True, request_count, 0, time_window
        

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
        user_sub = event.get('requestContext', {}).get('authorizer').get('claims').get('sub')
        print("user sub is: ", user_sub)
        allowed_requests = 2
        time_window = 60
        
        is_rate_limited, request_count, remaining_requests, time_window = isRequestRateLimited(user_sub, allowed_requests, time_window)
        
        if is_rate_limited:
            body = {
            'message': 'Rate limit exceeded. Try again later.',
            'currentRequestCount': request_count,
            'remainingRequests': remaining_requests,
            'timeWindow': time_window
            }
            return {
                'statusCode': 200,
                'body': json.dumps(body)
            }
            
        custom_label  = body['query']
        custom_label_list = custom_label.split(',')
        custom_label_list = [item.strip() for item in custom_label_list]
        
        combined_list = custom_label_list
        combined_list = list(set(combined_list))
        print("combined_list is: ", combined_list)

        if len(combined_list) != 0:
            print("combined_list: ", combined_list)
            img_paths = get_photo_path(custom_label, user_sub)
            print(f"img_paths is: {img_paths}")
        
        return {
        'headers': {
            "Access-Control-Allow-Origin" : "*", 
        },
            'statusCode': 200,
            'body': json.dumps(list(img_paths))
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


def construct_query(query_string):
    and_parts = []
    or_parts = []
    
    # Parse the input string to separate AND and OR parts
    # This is a simple parsing logic; it may need to be adjusted based on the actual input format
    if " OR " in query_string:
        and_or_split = query_string.split(" OR ")
        for part in and_or_split:
            if " AND " in part:
                and_parts.append(part.split(" AND "))
            else:
                or_parts.append(part)
    else:
        and_parts.append(query_string.split(" AND "))

    # Construct the must and should clauses
    must_clauses = []
    should_clauses = []

    for and_group in and_parts:
        must_clauses.extend([{"match": {"final-labels": word.lower()}} for word in and_group])
    
    should_clauses.extend([{"match": {"final-labels": word.lower()}} for word in or_parts])
    
    # Build the query
    query = {
        "query": {
            "bool": {}
        }
    }
    
    if must_clauses:
        query["query"]["bool"]["must"] = must_clauses
    
    if should_clauses:
        query["query"]["bool"]["should"] = should_clauses

    return query

def get_photo_path(query_string, user_sub):
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
        query = construct_query(query_string)

        INDEX_NAME = f"photo-label-{user_sub}"

        # Perform the search
        response = es.search(index=INDEX_NAME, body=query)
        logger.info("Search query executed successfully")

        # Process the response
        output = []
        hits = response.get('hits', {}).get('hits', [])
        logger.info(f"Hits: {hits}")
        
        for hit in hits:
            source = hit.get('_source', {})
            logger.info(f"Source: {source}")
            s3_path = source.get('s3-path')
            if s3_path:
                # The s3_path already includes the bucket name, so we only need to prepend the URL prefix
                full_s3_url = f"{S3_URL_PREFIX}{s3_path}"
                if full_s3_url not in output:
                    output.append(full_s3_url)

        return output

    except Exception as e:
        logger.error(f"Error occurred: {e}")
        return []