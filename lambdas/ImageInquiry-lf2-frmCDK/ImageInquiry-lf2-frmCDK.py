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

def lambda_handler(event, context):
    try:
        print("Event is: ", event)
        print(f"{event['query']}" )
        query = event['query']
        labels_list = get_labels(query)
        print(f"labels_list is: {labels_list}")
        

        # labels_list  = [item.strip() for item in labels.split(',')]
        if len(labels_list) != 0:
            img_paths = get_photo_path(labels_list)
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

def get_labels(query):
    sample_string = 'pqrstuvwxyabdsfbc'
    userid = ''.join((random.choice(sample_string)) for x in range(8))
    
    print(f"Query to lex is: {query}")
    
    response = lex.post_text(
        botName='FindKeywords',                 
        botAlias='alias',
        userId=userid,           
        inputText=query
    )
    
    print(f"response from lex is: : {response}")
    # print("lex-response", response)
    
    labels = []
    if 'slots' not in response:
        print("No photo collection for query {}".format(query))
    else:
        print ("slot: ",response['slots'])
        slot_val = response['slots']
        for key,value in slot_val.items():
            if value!=None:
                labels.append(value.strip())
    return labels


def get_photo_path(keys):
    host= os.environ["OPENSEARCH_HOST_ENDPOINT"]
    print(f"Host is: {host}")
    
    auth = (
            os.environ['ESUSERNAME'],
           os.environ['ESPASSWORD']
    )

    
    es = Elasticsearch(
        hosts=[{'host': host, 'port':443}],
        use_ssl=True,
        http_auth=auth,
        verify_certs=True,
        connection_class=RequestsHttpConnection)
    
    es.info()
    logger.info(f"Connected to OpenSearch at {es}")
    
    resp = []
    for key in keys:
        print(f"Key is: {key}")
        if (key is not None) and key != '':
            searchData = es.search({"query": {"match": {"labels": key}}})
            resp.append(searchData)

    output = []
    for r in resp:
        print(f"response is: {r}")
        if 'hits' in r:
             for val in r['hits']['hits']:
                entire_obj_path = f"https://{val['_source']['bucket']}.s3.amazonaws.com/{val['_source']['objectKey']}"
                if entire_obj_path not in output:
                    
                    output.append(entire_obj_path)
                    # output.append('s3://pipebucketcloud/'+key)
    # print (output)
    return output  


# if __name__ == '__main__':
#     # photos = get_photo_path(['dog' , 'blazer'])
#     labels = get_labels("show images of cat and dog")
#     # print(f'Photos:{photos}', )



