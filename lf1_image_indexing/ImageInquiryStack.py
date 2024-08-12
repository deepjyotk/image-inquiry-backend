from aws_cdk import (
    aws_s3 as s3,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_lambda_event_sources as lambda_event_sources,
    aws_dynamodb as dynamodb,
    RemovalPolicy,
    Stack,
    Duration
)
from aws_cdk.aws_iam import PolicyStatement, AnyPrincipal
from dotenv import load_dotenv
from constructs import Construct
import os

class ImageInquiryStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        load_dotenv()
        
        # Define the S3 bucket
        bucket = s3.Bucket(self, "imageinquiry-images",
                           bucket_name="imageinquiry-images",
                           public_read_access=False,
                           removal_policy=RemovalPolicy.DESTROY
                           )
        
        # Define the bucket policy
        policy_statement = PolicyStatement(
            actions=["s3:*"],
            resources=[
                bucket.bucket_arn,
                bucket.arn_for_objects("*")
            ],
            principals=[iam.AccountPrincipal("533267413906")]
        )
        bucket.add_to_resource_policy(policy_statement)
        
        # Define the DynamoDB table
        rate_limiter_table = dynamodb.Table(self, "RateLimiterTable",
            table_name="imageinquiry-rate-limiter",
            partition_key=dynamodb.Attribute(
                name="id",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY
        )

        # Create IAM roles for Lambda functions
        lambda_role1 = iam.Role(self, "LambdaExecutionRole1",
                               assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
                               managed_policies=[
                                   iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                                   iam.ManagedPolicy.from_aws_managed_policy_name("AmazonRekognitionFullAccess"),
                                   iam.ManagedPolicy.from_aws_managed_policy_name("AmazonOpenSearchServiceFullAccess"),
                                   iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                                   iam.ManagedPolicy.from_aws_managed_policy_name("AmazonDynamoDBFullAccess")
                               ])
        auth_role = iam.Role(self, "AuthRole",
                     assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
                     managed_policies=[
                         iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                         iam.ManagedPolicy.from_aws_managed_policy_name("AmazonRekognitionFullAccess"),
                         iam.ManagedPolicy.from_aws_managed_policy_name("AmazonOpenSearchServiceFullAccess"),
                         iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                         iam.ManagedPolicy.from_aws_managed_policy_name("AmazonDynamoDBFullAccess"),
                         iam.ManagedPolicy.from_aws_managed_policy_name("AmazonCognitoPowerUser")  # Full access to Cognito
                     ])

        
        
        search_handler_role = iam.Role(self, "LambdaExecutionRole2",
                               assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
                               managed_policies=[
                                   iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                                   iam.ManagedPolicy.from_aws_managed_policy_name("AmazonRekognitionFullAccess"),
                                   iam.ManagedPolicy.from_aws_managed_policy_name("AmazonOpenSearchServiceFullAccess"),
                                   iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                                   iam.ManagedPolicy.from_aws_managed_policy_name("AmazonLexFullAccess"),
                                   iam.ManagedPolicy.from_aws_managed_policy_name("AmazonDynamoDBFullAccess")
                               ])
        
        # Add DynamoDB permissions to the Lambda roles
        dynamodb_policy_statement = PolicyStatement(
            actions=["dynamodb:Query"],
            resources=[rate_limiter_table.table_arn]
        )
        lambda_role1.add_to_policy(dynamodb_policy_statement)
        search_handler_role.add_to_policy(dynamodb_policy_statement)
        
        upload_layer = lambda_.LayerVersion(self, "upload_layer",
                                     code=lambda_.Code.from_asset("layers/upload-packages/"),
                                     compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
                                     description="A layer that contains custom packages",
                                     layer_version_name="upload-layer-v1")
        
        generate_ai_labels_layer = lambda_.LayerVersion(self, "generate-ai-labels-layer",
                                     code=lambda_.Code.from_asset("layers/generate-ai-labels-packages/"),
                                     compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
                                     description="A layer that contains custom packages",
                                     layer_version_name="generate_ai_labels_layer-v1")
        
        search_lambda_layer = lambda_.LayerVersion(self, "search-lambda-layer",
                                     code=lambda_.Code.from_asset("layers/search-handler-packages/"),
                                     compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
                                     description="A layer that contains custom packages",
                                     layer_version_name="search_lambda_layer-v1")

        # Define Lambda functions
        auth_handler = lambda_.Function(self, "imageinquiry-auth-handler",
                                        function_name="imageinquiry-auth-handler",
                                        runtime=lambda_.Runtime.PYTHON_3_12,
                                        handler="handler.lambda_handler",
                                        code=lambda_.Code.from_asset("./lambdas/imageinquiry-auth-handler"),
                                        role=auth_role,
                                        # layers=[layer1],
                                        timeout=Duration.minutes(10),
                                        environment={
                                            'OPENSEARCH_HOST_ENDPOINT': os.environ['OPENSEARCH_HOST_ENDPOINT'],
                                            'ESUSERNAME': os.environ['ESUSERNAME'],
                                            'ESPASSWORD': os.environ['ESPASSWORD'],
                                        })

        upload_handler = lambda_.Function(self, "imageinquiry-upload-handler",
                                          function_name="imageinquiry-upload-handler",
                                          runtime=lambda_.Runtime.PYTHON_3_12,
                                          handler="handler.lambda_handler",
                                          code=lambda_.Code.from_asset("./lambdas/imageinquiry-upload-handler"),
                                          role=lambda_role1,
                                          layers=[upload_layer],
                                          timeout=Duration.minutes(10),
                                          environment={
                                              'OPENSEARCH_HOST_ENDPOINT': os.environ['OPENSEARCH_HOST_ENDPOINT'],
                                              'ESUSERNAME': os.environ['ESUSERNAME'],
                                              'ESPASSWORD': os.environ['ESPASSWORD'],
                                          })

        generate_ai_labels_handler = lambda_.Function(self, "imageinquiry-generate-ai-labels-handler",
                                                     function_name="imageinquiry-generate-ai-labels-handler",
                                                     runtime=lambda_.Runtime.PYTHON_3_12,
                                                     handler="handler.lambda_handler",
                                                     code=lambda_.Code.from_asset("./lambdas/imageinquiry-generate-ai-labels-handler"),
                                                     role=lambda_role1,
                                                     layers=[generate_ai_labels_layer],
                                                     timeout=Duration.minutes(10),
                                                     environment={
                                                         'OPENSEARCH_HOST_ENDPOINT': os.environ['OPENSEARCH_HOST_ENDPOINT'],
                                                         'ESUSERNAME': os.environ['ESUSERNAME'],
                                                         'ESPASSWORD': os.environ['ESPASSWORD'],
                                                     })

        search_handler = lambda_.Function(self, "imageinquiry-search-handler",
                                          function_name="imageinquiry-search-handler",
                                          runtime=lambda_.Runtime.PYTHON_3_12,
                                          handler="handler.lambda_handler",
                                          code=lambda_.Code.from_asset("./lambdas/imageinquiry-search-handler"),
                                          role=search_handler_role,
                                          layers=[search_lambda_layer],
                                          timeout=Duration.minutes(10),
                                          environment={
                                              'OPENSEARCH_HOST_ENDPOINT': os.environ['OPENSEARCH_HOST_ENDPOINT'],
                                              'ESUSERNAME': os.environ['ESUSERNAME'],
                                              'ESPASSWORD': os.environ['ESPASSWORD'],
                                          })

        # Set up the trigger from the S3 bucket to the Lambda
        upload_handler.add_event_source(lambda_event_sources.S3EventSource(
            bucket,
            events=[s3.EventType.OBJECT_CREATED],
        ))
        
        # Grant the Lambda functions permissions to interact with the S3 bucket
        bucket.grant_read_write(upload_handler)
        bucket.grant_read_write(search_handler)
        bucket.grant_read_write(generate_ai_labels_handler)
        bucket.grant_read_write(auth_handler)
        
        # Grant DynamoDB permissions to the Lambda functions
        rate_limiter_table.grant_read_write_data(upload_handler)
        rate_limiter_table.grant_read_write_data(search_handler)
        rate_limiter_table.grant_read_write_data(generate_ai_labels_handler)
        rate_limiter_table.grant_read_write_data(auth_handler)