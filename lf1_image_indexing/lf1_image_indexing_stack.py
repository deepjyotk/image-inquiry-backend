from aws_cdk import (
    aws_s3 as s3,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_lambda_event_sources as lambda_event_sources,
    RemovalPolicy,
    Stack,
    Duration
)
from aws_cdk.aws_iam import PolicyStatement, AnyPrincipal

from constructs import Construct


class Lf1ImageIndexingStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        bucket = s3.Bucket(self, "ImageInquiry-B2-frmCDK",
                           bucket_name="imageinquiry-b2-frmcdk",
                            #  blockPublicAccess: BucketAccessControl.BLOCK_ACLS,
                            # accessControl: BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
                           public_read_access=False,
                           removal_policy=RemovalPolicy.DESTROY
                           
                           )  # Adjust the policy as necessary
                # Define the bucket policy
        policy_statement = PolicyStatement(
            actions=["s3:*"],
            resources=[
                bucket.bucket_arn,
                bucket.arn_for_objects("*")
            ],
            principals=[AnyPrincipal()]
        )

        # Attach the policy to the bucket
        bucket.add_to_resource_policy(policy_statement)
        
        # layer = lambda_.LayerVersion(self, "MyPythonDependencies",
        #                              code=lambda_.Code.from_asset("layers/"),
        #                              compatible_runtimes=[lambda_.Runtime.PYTHON_3_9],
        #                              description="A layer that contains Python dependencies")
        
        
      # Create an IAM role for the Lambda function with specific permissions
        lambda_role1 = iam.Role(self, "LambdaExecutionRole1",
                               assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
                               managed_policies=[
                                   iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                           iam.ManagedPolicy.from_aws_managed_policy_name("AmazonRekognitionFullAccess"),  # Full access to Rekognition
                           iam.ManagedPolicy.from_aws_managed_policy_name("AmazonOpenSearchServiceFullAccess"),  # Full access to OpenSearch,
                           iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess")
                               ])
        lambda_role2 = iam.Role(self, "LambdaExecutionRole2",
                               assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
                               managed_policies=[
                                   iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                           iam.ManagedPolicy.from_aws_managed_policy_name("AmazonRekognitionFullAccess"),  # Full access to Rekognition
                           iam.ManagedPolicy.from_aws_managed_policy_name("AmazonOpenSearchServiceFullAccess"),  # Full access to OpenSearch,
                           iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                             iam.ManagedPolicy.from_aws_managed_policy_name("AmazonLexFullAccess")
                               ])

        # Explicit permissions for Lambda to interact with the S3 bucket
        policy_statements = [
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:PutObject"],
                resources=[bucket.bucket_arn + "/*"],
            )
        ]        
        
        
        
        layer1= lambda_.LayerVersion(self, "custom-package-layer",
                                     code=lambda_.Code.from_asset("layers/lf1-local-packages/"),
                                     compatible_runtimes=[lambda_.Runtime.PYTHON_3_11],
                                     description="A layer that contains custom packages",
                                     layer_version_name="lf1-layer-v1")
        
        layer2 = lambda_.LayerVersion(self, "custom-package-layer2",
                                     code=lambda_.Code.from_asset("layers/lf2-local-packages/"),
                                     compatible_runtimes=[lambda_.Runtime.PYTHON_3_11],
                                     description="A layer that contains custom packages",
                                     layer_version_name="lf2-layer-v1")


        # Create the Lambda function
        lambda_fn1 = lambda_.Function(self, "ImageInquiry-lf1-frmCDK",
                                     function_name="ImageInquiry-lf1-frmCDK",
                                     runtime=lambda_.Runtime.PYTHON_3_11,
                                     handler="ImageInquiry-lf1-frmCDK.lambda_handler",
                                     code=lambda_.Code.from_asset("./lambdas/ImageInquiry-lf1-frmCDK"),
                                     role= lambda_role1,
                                     layers=[layer1],
                                     timeout= Duration.minutes(10),
                                       environment={
                                         'OPENSEARCH_HOST_ENDPOINT': os.environ['OPENSEARCH_HOST_ENDPOINT'],
                                         'ESUSERNAME': os.environ['ESUSERNAME'],
                                         'ESPASSWORD': os.environ['ESPASSWORD'],
                                     }
                                     )
        lambda_fn2 = lambda_.Function(self, "ImageInquiry-lf2-frmCDK",
                                     function_name="ImageInquiry-lf2-frmCDK",
                                     runtime=lambda_.Runtime.PYTHON_3_11,
                                     handler="ImageInquiry-lf2-frmCDK.lambda_handler",
                                     code=lambda_.Code.from_asset("./lambdas/ImageInquiry-lf2-frmCDK"),
                                     role= lambda_role2,
                                     layers=[layer2],
                                     timeout= Duration.minutes(10),
                                       environment={
                                          'OPENSEARCH_HOST_ENDPOINT': os.environ['OPENSEARCH_HOST_ENDPOINT'],
                                         'ESUSERNAME': os.environ['ESUSERNAME'],
                                         'ESPASSWORD': os.environ['ESPASSWORD'],
                                     }
                                     )

      

        # Set up the trigger from the S3 bucket to the Lambda
        lambda_fn1.add_event_source(lambda_event_sources.S3EventSource(
            bucket,
            events=[s3.EventType.OBJECT_CREATED],
        ))
        
          # Grant the Lambda function permissions to put objects in the bucket
        bucket.grant_read_write(lambda_fn1)
        bucket.grant_read_write(lambda_fn2)