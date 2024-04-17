from aws_cdk import (
    # Duration,
    Stack,
    # aws_sqs as sqs,
    Stage,
    Environment,
    pipelines,
    aws_codepipeline as codepipeline
)
from constructs import Construct
from lf1_image_indexing.lf1_image_indexing_stack import Lf1ImageIndexingStack


class DeployStage(Stage):
    def __init__(self, scope: Construct, id: str, env: Environment, **kwargs) -> None:
        super().__init__(scope, id, env=env, **kwargs)
        Lf1ImageIndexingStack(self, 'Lf1ImageIndexingStack', env=env, stack_name="Lf1ImageIndexingStack-frmPipeline")


class AwsCodepipelineStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        git_input = pipelines.CodePipelineSource.connection(
            repo_string="deepjyotk/lf1-image-indexing",
            branch="main",
            connection_arn="arn:aws:codestar-connections:us-east-1:533267413906:connection/51ff67d1-753f-40e9-a102-819dea5b2511"
        )

        code_pipeline = codepipeline.Pipeline(
            self, "Pipeline",
            pipeline_name="lf1-image-indexing-stack-pipeline",
            cross_account_keys=False
        )

        synth_step = pipelines.ShellStep(
            id="Synth",
            install_commands=[
                'pip install -r requirements.txt',
                'pip install -r ./lambdas/ImageInquiry-lf1-frmCDK/requirements.txt --target=./layers/lf1-local-packages/python/lib/python3.11/site-packages',
                'pip install -r ./lambdas/ImageInquiry-lf2-frmCDK/requirements.txt --target=./layers/lf-local-packages/python/lib/python3.11/site-packages'
            ],
            commands=[
                'npx cdk synth'
            ],
            input=git_input
        )

        pipeline = pipelines.CodePipeline(
            self, 'CodePipeline',
            self_mutation=True,
            code_pipeline=code_pipeline,
            synth=synth_step
        )

        deployment_wave = pipeline.add_wave("DeploymentWave")

        deployment_wave.add_stage(DeployStage(
            self, 'DeployStage',
            env=(Environment(account='533267413906', region='us-east-1'))
        ))