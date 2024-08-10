import json
import boto3
import logging
from botocore.exceptions import BotoCoreError, ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# AWS Cognito setup
cognito_client = boto3.client('cognito-idp', region_name='us-east-1')
user_pool_id = 'us-east-1_Q2ZaH09ua'
client_id = '30sqqaokoisa63028gl8ivg4uu'
# client_secret = 'aj110699gj925r2ak0hh261l0ph6v032toon9fhdl5nl3jhru4i'

def create_response(status_code, body, headers=None):
    """Helper function to create a response with CORS headers."""
    if headers is None:
        headers = {}
    cors_headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': '*',
        'Access-Control-Allow-Methods': 'OPTIONS, POST, GET, PUT, DELETE'
    }
    headers.update(cors_headers)
    return {
        'statusCode': status_code,
        'headers': headers,
        'body': json.dumps(body)
    }

def lambda_handler(event, context):
    print("Received event:", json.dumps(event))
    try:
        if event['path'] == '/auth/register':
            print("Processing registration")
            return signup(event)
        elif event['path'] == '/auth/login':
            print("Processing login")
            return login(event)
        elif event['path'] == '/auth/confirm':
            print("Processing confirmation")
            return confirm_signup(event)
        elif event['path'] == '/auth/request_confirm_code':
            print("Processing resend_confirmation_code")
            return resend_confirmation_code(event)
        else:
            print("Invalid request path received")
            return {'statusCode': 400, 'body': json.dumps('Invalid request path')}
    except Exception as e:
        print("Error processing the request:", str(e))
        return {'statusCode': 500, 'body': json.dumps(str(e))}


def add_to_db(data):
    # Insert the user into the USER table
    dynamodb = boto3.resource('dynamodb')
    # Extract the name from the dictionary
    full_name = data['name']

    # Split the full name into first name and last name
    name_parts = full_name.split()

    # Ensure there are at least two parts; if not, handle the case (e.g., missing last name)
    first_name = name_parts[0] if len(name_parts) > 0 else 'Fizz'
    last_name = name_parts[1] if len(name_parts) > 1 else 'Buzz'

    print("First Name:", first_name)
    print("Last Name:", last_name)
        
    # Reference to the USERS table
    # table = dynamodb.Table('USERS')
    # # Add a new user
    # table.put_item(
    #         Item={
    #         'user_id': data['email'],
    #         'user_first_name': first_name,
    #         'user_last_name': last_name,
    #         'user_email': data['email'],

    #         }
    # )
        
        
def signup(event):
    data = json.loads(event['body'])
    logger.info("Signup data received: %s", data)
    print("Signup data received: %s", data)
    user_attributes = [
        {'Name': 'email', 'Value': data['email']},
        {'Name': 'name', 'Value': data['name']}
    ]
    try:

        response = cognito_client.sign_up(
            ClientId=client_id,
            Username=data['email'],
            Password=data['password'],
            UserAttributes=user_attributes
        )
        
        # Add to the USER table
        # add_to_db(data)
         
        logger.info("Registration successful: %s", response)
        return create_response(200, {'message': 'User registered', 'data': response})
    except ClientError as error:
        error_code = error.response['Error']['Code']
        if error_code == 'UsernameExistsException':
            logger.warning("User already exists: %s", error)
            return create_response(409, {'message': 'User already exists'})
        else:
            logger.error("AWS Cognito error during registration: %s", error)
            return create_response(400, {'message': f'Cognito error: {error_code}'})
    except Exception as e:
        logger.error("Unexpected error during registration: %s", e)
        return create_response(500, {'message': 'Internal server error'})


def login(event):
    """
    Handle user login requests using AWS Cognito.

    Args:
        event (dict): The event dictionary containing details of the HTTP request.

    Returns:
        dict: A dictionary with the HTTP status code and the body of the response.
    """
    try:
        data = json.loads(event['body'])
        logger.info("Login data received: %s", data)

        response = cognito_client.admin_initiate_auth(
            UserPoolId=user_pool_id,
            ClientId=client_id,
            AuthFlow='ADMIN_NO_SRP_AUTH',
            AuthParameters={
                'USERNAME': data['email'],
                'PASSWORD': data['password']
            }
        )

        logger.info("Login successful: %s", response)
        return create_response(200, {'message': 'User logged in successfully', 'data': response})

    except ClientError as error:
        error_code = error.response['Error']['Code']
        if error_code == 'UserNotConfirmedException':
            logger.warning("User not confirmed: %s", error)
            return create_response(401, {'message': 'User account not confirmed'})
        else:
            logger.error("AWS error during login: %s", error)
            return create_response(500, {'message': 'Internal server error during login'})

    except BotoCoreError as aws_error:
        logger.error("AWS error during login: %s", aws_error)
        return create_response(500, {'message': 'Internal server error during login'})

    except KeyError as e:
        logger.error("Missing required parameters: %s", e)
        return create_response(400, {'message': 'Bad request - Missing required parameters'})

    except json.JSONDecodeError as json_error:
        logger.error("JSON decoding error: %s", json_error)
        return create_response(400, {'message': 'Bad request - Invalid JSON in request body'})

    except Exception as e:
        logger.error("Unexpected error during login: %s", e)
        return create_response(500, {'message': 'Internal server error'})

def confirm_signup(event):
    """
    Confirm user signup with a confirmation code received via email or SMS.

    Args:
        event (dict): The event dictionary containing details of the HTTP request.

    Returns:
        dict: A dictionary with the HTTP status code and the body of the response.
    """
    try:
        data = json.loads(event['body'])
        logger.info("called Confirmation data received: %s", data)

        response = cognito_client.confirm_sign_up(
            ClientId=client_id,
            Username=data['email'],
            ConfirmationCode=data['confirmation_code']
        )

        logger.info("User confirmed successfully: %s", response)
        return create_response(200, {'message': 'User confirmed successfully', 'data': response})

    except ClientError as error:
        error_code = error.response['Error']['Code']
        error_message = error.response['Error']['Message']
        logger.error("AWS Cognito error during confirmation: %s", error)

        # Check for NotAuthorizedException and specific message
        if error_code == 'NotAuthorizedException' and 'User cannot be confirmed. Current status is CONFIRMED' in error_message:
            logger.info("User already confirmed: %s", error_message)
            return create_response(200, {'message': 'User is already confirmed.'})

        return create_response(400, {'message': f"Cognito error: {error_code} - {error_message}"})

    except json.JSONDecodeError as json_error:
        logger.error("JSON decoding error: %s", json_error)
        return create_response(400, {'message': 'Invalid JSON in request body'})

    except Exception as e:
        logger.error("Unexpected error during confirmation: %s", e)
        return create_response(500, {'message': 'Internal server error'})

def resend_confirmation_code(event):
    """
    Resend confirmation code to the user's email or phone number.

    Args:
        event (dict): The event dictionary containing details of the HTTP request, such as the username.

    Returns:
        dict: A dictionary with the HTTP status code and the body of the response.
    """
    try:
        data = json.loads(event['body'])
        logger.info("Resend confirmation code request received for: %s", data['email'])

        response = cognito_client.resend_confirmation_code(
            ClientId=client_id,
            Username=data['email']
        )

        logger.info("Confirmation code resent successfully: %s", response)
        return create_response(200, {'message': 'Confirmation code resent successfully', 'data': response})

    except ClientError as error:
        error_code = error.response['Error']['Code']
        error_message = error.response['Error']['Message']
        logger.error("AWS Cognito error during resending confirmation code: %s", error)
        if error_code == 'NotAuthorizedException' and 'Current status is CONFIRMED' in error_message:
            return create_response(200, {'message': 'User is already confirmed'})
        else:
            return create_response(400, {'message': f"Cognito error: {error_code}"})

    except KeyError as e:
        logger.error("Missing required parameters: %s", e)
        return create_response(400, {'message': 'Bad request - Missing required parameters'})

    except json.JSONDecodeError as json_error:
        logger.error("JSON decoding error: %s", json_error)
        return create_response(400, {'message': 'Bad request - Invalid JSON in request body'})

    except Exception as e:
        logger.error("Unexpected error during resending confirmation code: %s", e)
        return create_response(500, {'message': 'Internal server error'})