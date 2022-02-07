from aws_cdk import core as cdk
import aws_cdk.aws_lakeformation as lakeformation
from plugin.infrastructure.resource.aws.services.main import SDK


class Taxonomy(cdk.Stack):
    """
    TODO
    """

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

    @staticmethod
    def create_classifications(cloudservice: SDK, region: str):
        """
        At the time this code was committed, the feature of creating lf-tags using CloudFormation/CDK has not yet been implemented.
        There is already an open issue (https://github.com/aws-cloudformation/cloudformation-coverage-roadmap/issues/920)
        and, for now, we will use the boto3 SDK and as soon as the feature is available for CDK, the migration will be performed.
        """
        cloudservice.create_classifications(cloudservice.account_id, region)

    def register_repositories(self, account: str, datalake_name: str):
        self.register_location(f"{account}-raw-{datalake_name}")
        self.register_location(f"{account}-clean-{datalake_name}")

    def register_location(self, repository: str):
        lakeformation.CfnResource(
            self,
            f'lf-resource-{repository}',
            resource_arn=f'arn:aws:s3:::{repository}',
            use_service_linked_role=True
        )
