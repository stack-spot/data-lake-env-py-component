import string
import random
import pytest
from aws_cdk import core
from unittest import (mock, TestCase)
from plugin.domain.manifest import Manifest
from plugin.infrastructure.resource.aws.cdk.stacks.taxonomy import Taxonomy


class CreateTaxonomy(TestCase):

    @pytest.fixture(autouse=True)
    def cdk_app(self):
        self.app = core.App()
        self.stack_name = f"create-taxonomy-stack-{self.__random_string(letter=string.ascii_letters,size=10)}"
        self.stack = Taxonomy(self.app, self.stack_name)

    @staticmethod
    def __random_string(letter, size: int):
        return ''.join(random.choice(letter) for _ in range(size))

    @pytest.fixture(autouse=True)
    def plugin_manifest(self):
        self.manifest = Manifest({
            "datalake": {
                "name": "",
                "region": "us-east-1"
            }
        })

    @mock.patch('plugin.infrastructure.resource.aws.cdk.stacks.taxonomy.SDK', autospec=True)
    def test_if_create_taxonomy_works(self, mock_cloud_service):
        name = self.__random_string(
            letter=string.ascii_letters,
            size=18)
        self.manifest.datalake.name = f"{name}_test"

        mock_cloud_service.account_id = "123456789"

        self.stack.register_repositories(
            mock_cloud_service.account_id, self.manifest.datalake.name)
        self.stack.create_classifications(
            mock_cloud_service, self.manifest.datalake.region)
