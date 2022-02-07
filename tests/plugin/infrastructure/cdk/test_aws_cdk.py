import boto3
import json
from datetime import datetime
from uuid import uuid4
from botocore.stub import Stubber
from unittest import (mock, TestCase)
from plugin.domain.manifest import Manifest
from plugin.infrastructure.resource.aws.cdk.main import AwsCdk


class AwsCdkTest(TestCase, AwsCdk):

    @mock.patch("plugin.infrastructure.resource.aws.cdk.main.SDK", autospec=True)
    @mock.patch("plugin.infrastructure.resource.aws.services.lakeformation.service.boto3", autospec=True)
    @mock.patch("plugin.infrastructure.resource.aws.cdk.engine.helpers.boto3", autospec=True)
    def setUp(self, mock_boto3_cf, mock_boto3_lf, mock_sdk) -> None:
        self.manifest = Manifest({
            "datalake": {
                "name": "",
                "region": "us-east-1"
            }
        })

        self.mock_cloud_service = mock_sdk.return_value
        self.mock_cf_session = mock_boto3_cf.Session.return_value
        self.mock_cf = self.mock_cf_session.client.return_value
        self.cf_client = boto3.client(
            "cloudformation", region_name="us-east-1")
        self.stubber_cf = Stubber(self.cf_client)
        self.stubber_cf.activate()

        self.mock_lf_session = mock_boto3_lf.Session.return_value
        self.mock_lf = self.mock_lf_session.client.return_value
        self.lf_client = boto3.client(
            "lakeformation", region_name="us-east-1")
        self.stubber_lf = Stubber(self.lf_client)
        self.stubber_lf.activate()

        return super().setUp()

    def mock_create_stack(self, stack_name: str, stack_template: str):
        response = {
            'StackId': uuid4().__str__()
        }

        expected_params = {
            'StackName': stack_name,
            'TemplateBody': json.dumps(stack_template),
            'OnFailure': 'ROLLBACK',
            'Capabilities': ['CAPABILITY_IAM', 'CAPABILITY_NAMED_IAM']
        }

        self.stubber_cf.add_response('create_stack', response, expected_params)

        mocked_response = self.cf_client.create_stack(
            StackName=stack_name,
            TemplateBody=json.dumps(stack_template),
            OnFailure='ROLLBACK',
            Capabilities=['CAPABILITY_IAM', 'CAPABILITY_NAMED_IAM']
        )

        return mocked_response

    def mock_describe_stack_events(self, stack_name: str, stack_template: dict):

        resource_id = list(stack_template["Resources"].keys())[0]

        events_finish = {
            "StackEvents": [
                {
                    "StackId": uuid4().__str__(),
                    "EventId": "1",
                    "StackName": stack_name,
                    "LogicalResourceId": resource_id,
                    "PhysicalResourceId": resource_id,
                    "ResourceType": ".",
                    "Timestamp": datetime(2021, 1, 1),
                    "ResourceStatus": "CREATE_COMPLETE",
                    "ResourceStatusReason": "...",
                    "ResourceProperties": ".",
                    "ClientRequestToken": "."
                },
            ],
            "NextToken": "string"
        }

        self.stubber_cf.add_response("describe_stack_events", events_finish, {
            "StackName": stack_name})

        mocked_response = self.cf_client.describe_stack_events(
            StackName=stack_name
        )

        return mocked_response

    def mock_stack_template(self, x: int):
        return {
            "Resources": {
                f"resource-{x}": {
                    "Type": "AWS::Resource",
                    "Properties": {}
                }
            }
        }

    def mock_get_lf_tag(self, catalog_id: str, tag_key: str):

        response = {
            "CatalogId": catalog_id,
            "TagKey": tag_key,
            "TagValues": [
                "true",
                "false"
            ]
        }

        expected_params = {
            "CatalogId": catalog_id,
            "TagKey": tag_key
        }

        self.stubber_lf.add_response('get_lf_tag', response, expected_params)

        mocked_response = self.lf_client.get_lf_tag(
            CatalogId=catalog_id,
            TagKey=tag_key
        )

        return mocked_response

    @mock.patch("plugin.infrastructure.resource.aws.cdk.engine.helpers.is_created_resource", autospec=True)
    @mock.patch("plugin.infrastructure.resource.aws.services.main.boto3", autospec=True)
    @mock.patch("plugin.infrastructure.resource.aws.services.lakeformation.service.boto3", autospec=True)
    @mock.patch("plugin.infrastructure.resource.aws.cdk.engine.helpers.boto3", autospec=True)
    def test_test_create_datalake_stack_with_buckets(self, mock_boto3_cf, mock_boto3_sts, mock_boto3_lf, mock_created_resource):

        account_id = "12345678912"

        mock_sts = mock_boto3_sts.client.return_value
        mock_sts.get_caller_identity.side_effect = [
            {
                "Account": account_id
            } for _ in range(2)
        ]

        mock_cf_session = mock_boto3_cf.Session.return_value
        mock_cf = mock_cf_session.client.return_value

        mock_cf.create_stack.side_effect = [
            self.mock_create_stack(
                stack_name=self.manifest.datalake.name,
                stack_template=self.mock_stack_template(1)
            )
        ]

        mock_cf.describe_stack_events.side_effect = [
            self.mock_describe_stack_events(
                stack_name=self.manifest.datalake.name,
                stack_template=self.mock_stack_template(1)
            )
        ]

        mock_created_resource.side_effect = [True]

        tag_keys = ["data_classification",
                    "manipulated", "access_level", "private"]

        mock_lf_session = mock_boto3_lf.Session.return_value
        mock_lf = mock_lf_session.client.return_value

        mock_lf.get_lf_tag.side_effect = [
            self.mock_get_lf_tag(
                catalog_id=account_id,
                tag_key=tag_key
            ) for tag_key in tag_keys
        ]

        clean_bucket_name = f"{account_id}-clean-{self.manifest.datalake.name}"
        raw_bucket_name = f"{account_id}-raw-{self.manifest.datalake.name}"

        bucket = {"raw": raw_bucket_name,
                  "clean": clean_bucket_name, "name": "rc"}

        self.create_datalake_stack(
            datalake=self.manifest.datalake, bucket=bucket)

    @mock.patch("plugin.infrastructure.resource.aws.cdk.engine.helpers.is_created_resource", autospec=True)
    @mock.patch("plugin.infrastructure.resource.aws.services.main.boto3", autospec=True)
    @mock.patch("plugin.infrastructure.resource.aws.services.lakeformation.service.boto3", autospec=True)
    @mock.patch("plugin.infrastructure.resource.aws.cdk.engine.helpers.boto3", autospec=True)
    def test_test_create_datalake_stack_without_buckets(self, mock_boto3_cf, mock_boto3_sts, mock_boto3_lf, mock_created_resource):

        account_id = "12345678912"

        mock_sts = mock_boto3_sts.client.return_value
        mock_sts.get_caller_identity.side_effect = [
            {
                "Account": account_id
            } for _ in range(2)
        ]

        mock_cf_session = mock_boto3_cf.Session.return_value
        mock_cf = mock_cf_session.client.return_value

        mock_cf.create_stack.side_effect = [
            self.mock_create_stack(
                stack_name=self.manifest.datalake.name,
                stack_template=self.mock_stack_template(1)
            )
        ]

        mock_cf.describe_stack_events.side_effect = [
            self.mock_describe_stack_events(
                stack_name=self.manifest.datalake.name,
                stack_template=self.mock_stack_template(1)
            )
        ]

        mock_created_resource.side_effect = [True]

        tag_keys = ["data_classification",
                    "manipulated", "access_level", "private"]

        mock_lf_session = mock_boto3_lf.Session.return_value
        mock_lf = mock_lf_session.client.return_value

        mock_lf.get_lf_tag.side_effect = [
            self.mock_get_lf_tag(
                catalog_id=account_id,
                tag_key=tag_key
            ) for tag_key in tag_keys
        ]

        bucket = {"raw": "", "clean": "", "name": "rc"}

        self.create_datalake_stack(
            datalake=self.manifest.datalake, bucket=bucket)
