import aws_cdk as core
import aws_cdk.assertions as assertions

from lf1_image_indexing.ImageInquiryStack import Lf1ImageIndexingStack

# example tests. To run these tests, uncomment this file along with the example
# resource in lf1_image_indexing/lf1_image_indexing_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = Lf1ImageIndexingStack(app, "lf1-image-indexing")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
