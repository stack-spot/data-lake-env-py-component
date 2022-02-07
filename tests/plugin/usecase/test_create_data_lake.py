import string
import random
import pytest
from datetime import datetime
from unittest import (mock, TestCase)
from plugin.infrastructure.resource.aws.cdk.main import AwsCdk
from plugin.usecase.datalake.interface import DataLakeInterface
from plugin.usecase.datalake.main import DataLakeUseCase
from plugin.domain.manifest import Manifest, DataLake


class MockDataLakeUseCase(DataLakeInterface):

    def __init__(self) -> None:
        pass

    def create(self, datalake: DataLake):
        return super().create(datalake=datalake)


class CreateDataLakeTest(TestCase):

    @staticmethod
    def __random_string(letter, size: int):
        return ''.join(random.choice(letter) for i in range(size))

    def test_not_implemented_error(self):
        with pytest.raises(NotImplementedError):
            data_lake_use_case_mock = MockDataLakeUseCase()
            data_lake_use_case_mock.create(
                datalake=self.manifest.datalake)

    @pytest.fixture(autouse=True)
    def plugin_manifest(self):
        self.manifest = Manifest({
            "datalake": {
                "name": "default",
                "region": "us-east-1"
            }
        })

    @mock.patch("plugin.infrastructure.resource.aws.services.main.boto3", autospec=True)
    @mock.patch("plugin.infrastructure.resource.aws.services.s3.service.boto3", autospec=True)
    @mock.patch("plugin.infrastructure.resource.aws.cdk.main.AwsCdk.create_datalake_stack", return_value=None)
    def test_if_create_data_lake_works(self, _cdk_datalake, mock_boto3_s3, mock_boto3_sts):
        name = self.__random_string(
            letter=string.ascii_letters,
            size=18)
        self.manifest.datalake.name = f"{name}_test"

        mock_sts = mock_boto3_sts.client.return_value

        mock_sts.get_caller_identity.side_effect = [
            {
                "Account": "12345678912"
            }
        ]
        
        mock_session = mock_boto3_s3.Session.return_value
        mock_s3 = mock_session.client.return_value

        mock_s3.list_buckets.side_effect = [
            {
                'Buckets': [
                    {
                        'Name': '12345678912-clean-flowers',
                        'CreationDate': datetime(2015, 1, 1)
                    },
                    {
                        'Name': '12345678912-raw-flowers',
                        'CreationDate': datetime(2015, 1, 1)
                    },
                ],
                'Owner': {
                    'DisplayName': 'Owner',
                    'ID': 'my-id'
                }
            }
        ]

        result = DataLakeUseCase(cloud=AwsCdk()).create(
            datalake=self.manifest.datalake)
        assert result == True
