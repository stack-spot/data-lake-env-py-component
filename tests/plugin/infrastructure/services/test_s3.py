from __future__ import annotations
import boto3
from botocore.stub import Stubber
from datetime import datetime
from plugin.domain.manifest import Manifest, DataLake
from plugin.infrastructure.resource.aws.services.s3 import S3
from plugin.infrastructure.resource.aws.services.s3.interface import S3ResourceInterface
from plugin.infrastructure.resource.aws.services.s3.service import S3Resource
import pytest
import random
import string
from unittest import (mock, TestCase)


class MockS3Resource(S3ResourceInterface):

    def __init__(self) -> None:
        pass

    def check_buckets(self, account: str, datalake: DataLake) -> dict:
        return super().check_buckets(account=account, datalake=datalake)


class S3ResourceTest(TestCase):

    @mock.patch("plugin.infrastructure.resource.aws.services.s3.service.boto3", autospec=True)
    def setUp(self, mock_boto3) -> None:
        self.mock_session = mock_boto3.Session.return_value
        self.mock_s3 = self.mock_session.client.return_value
        self.s3_resource = S3Resource(region="us-east-1")
        self.s3_session = boto3.Session()
        self.s3_client = self.s3_session.client("s3", region_name="us-east-1")
        self.stubber = Stubber(self.s3_client)
        self.stubber.activate()

    @staticmethod
    def __random_string(letter, size: int):
        return ''.join(random.choice(letter) for _ in range(size))

    @pytest.fixture(autouse=True)
    def plugin_manifest(self):
        self.manifest = Manifest({
            "datalake": {
                "name": "default",
                "region": "us-east-1"
            }
        })

    def test_not_implemented_error(self):
        with pytest.raises(NotImplementedError):
            name = self.__random_string(
                letter=string.ascii_letters.lower(),
                size=18)
            self.manifest.datalake.name = f"{name}-test"

            s3 = MockS3Resource()
            s3.check_buckets(account="12345678912",
                             datalake=self.manifest.datalake.name)

    def get_exception_from_list_buckets(self, exception: str):
        self.stubber.add_client_error("list_buckets",
                                      service_error_code=exception,
                                      service_message=exception,
                                      http_status_code=404)
        try:
            self.s3_client.list_buckets()
        except Exception as error:
            return error

    def test_succesful_s3_resource_check_buckets(self):

        self.mock_s3.list_buckets.side_effect = [
            {
                'Buckets': [
                    {
                        'Name': f"12345678912-clean-{self.manifest.datalake.name}",
                        'CreationDate': datetime(2015, 1, 1)
                    },
                    {
                        'Name': f"12345678912-raw-{self.manifest.datalake.name}",
                        'CreationDate': datetime(2015, 1, 1)
                    },
                ],
                'Owner': {
                    'DisplayName': 'Owner',
                    'ID': 'my-id'
                }
            }
        ]

        self.s3_resource.check_buckets(account="12345678912",
                              datalake=self.manifest.datalake)

    
    @mock.patch("plugin.infrastructure.resource.aws.services.s3.service.boto3", autospec=True)
    def test_succesful_s3_check_buckets(self, mock_boto3_s3):

        s3_session = mock_boto3_s3.Session.return_value
        mock_s3 = s3_session.client.return_value

        mock_s3.list_buckets.side_effect = [
            {
                'Buckets': [
                    {
                        'Name': f"12345678912-clean-{self.manifest.datalake.name}",
                        'CreationDate': datetime(2015, 1, 1)
                    },
                    {
                        'Name': f"12345678912-raw-{self.manifest.datalake.name}",
                        'CreationDate': datetime(2015, 1, 1)
                    },
                ],
                'Owner': {
                    'DisplayName': 'Owner',
                    'ID': 'my-id'
                }
            }
        ]

        s3 = S3()

        s3.check_buckets(account="12345678912",
                         datalake=self.manifest.datalake)

    def test_error_when_check_buckets(self):

        self.mock_s3.list_buckets.side_effect = [
            self.get_exception_from_list_buckets(
                exception="EntityNotFoundException")
        ]

        self.s3_resource.check_buckets(account="12345678912",
                              datalake=self.manifest.datalake)
