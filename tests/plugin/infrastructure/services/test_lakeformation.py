import boto3
from botocore.stub import Stubber
from plugin.infrastructure.resource.aws.services.lakeformation.interface import LakeFormationResourceInterface
from plugin.infrastructure.resource.aws.services.lakeformation.service import LakeFormationResource, ClientError
from plugin.infrastructure.resource.aws.services.lakeformation import LakeFormation
import pytest
from unittest import (mock, TestCase)


class MockLakeFormationResource(LakeFormationResourceInterface):

    def __init__(self) -> None:
        pass

    def create_classifications(self, account_id: str) -> None:
        return super().create_classifications(account_id)


class LakeFormationResourceTest(TestCase):

    @mock.patch("plugin.infrastructure.resource.aws.services.lakeformation.service.boto3", autospec=True)
    def setUp(self, mock_boto3) -> None:
        self.mock_session = mock_boto3.Session.return_value
        self.mock_lf = self.mock_session.client.return_value
        self.lakeformation_resource = LakeFormationResource("us-east-1")
        self.lf_client = boto3.client(
            "lakeformation", region_name="us-east-1")
        self.stubber = Stubber(self.lf_client)
        self.stubber.activate()

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

        self.stubber.add_response('get_lf_tag', response, expected_params)

        mocked_response = self.lf_client.get_lf_tag(
            CatalogId=catalog_id,
            TagKey=tag_key
        )

        return mocked_response

    def mock_create_or_update_lf_tag(self) -> dict:
        return {}

    def get_exception_from_update_lf_tag(self, exception: str):
        self.stubber.add_client_error("update_lf_tag",
                                      service_error_code=exception,
                                      service_message=exception,
                                      http_status_code=404)
        try:
            self.lf_client.update_lf_tag(
                CatalogId="123456789",
                TagKey="foo",
                TagValuesToAdd=["bar"]
            )
        except Exception as error:
            return error

    def get_exception_from_create_lf_tag(self, exception: str):
        self.stubber.add_client_error("create_lf_tag",
                                      service_error_code=exception,
                                      service_message=exception,
                                      http_status_code=404)
        try:
            self.lf_client.create_lf_tag(
                CatalogId="123456789",
                TagKey="foo",
                TagValues=["bar"]
            )
        except Exception as error:
            return error

    def get_exception_from_get_lf_tag(self, exception: str):
        self.stubber.add_client_error("get_lf_tag",
                                      service_error_code=exception,
                                      service_message=exception,
                                      http_status_code=404)
        try:
            self.lf_client.get_lf_tag(
                CatalogId="123456789",
                TagKey="foo"
            )
        except Exception as error:
            return error

    def test_not_implemented_error(self):
        with pytest.raises(NotImplementedError):
            lakeformation = MockLakeFormationResource()
            lakeformation.create_classifications("123456789")

    def test_update_classification_exceptions(self):
        """
        Testing expected exceptions and one unexpected when updating classification
        """

        for exception in [
            "AccessDeniedException",
            "ConcurrentModificationException",
            "InternalServiceException",
            "InvalidInputException",
            "OperationTimeoutException",
                "UnexpectedException"]:

            self.mock_lf.get_lf_tag.side_effect = [
                self.mock_get_lf_tag(catalog_id="123456789", tag_key="pii")]

            update_error = self.get_exception_from_update_lf_tag(
                exception=exception)
            self.mock_lf.update_lf_tag.side_effect = [update_error]

            with pytest.raises(ClientError):
                lakeformation_response = self.lakeformation_resource._LakeFormationResource__update_or_create_classification(
                    catalog_id="123456789",
                    key="pii",
                    values=["true", "false", "Hi-Z"]
                )

    def test_create_classification_exceptions(self):
        """
        Testing expected exceptions and one unexpected when creating classification
        """

        for exception in [
            "AccessDeniedException",
            "InternalServiceException",
            "InvalidInputException",
            "OperationTimeoutException",
            "ResourceNumberLimitExceededException",
                "UnexpectedException"]:

            self.mock_lf.get_lf_tag.side_effect = [
                {
                    "TagValues": []
                }
            ]

            entity_not_found_error = self.get_exception_from_update_lf_tag(
                exception="EntityNotFoundException")
            self.mock_lf.update_lf_tag.side_effect = [entity_not_found_error]

            creation_error = self.get_exception_from_create_lf_tag(
                exception=exception)
            self.mock_lf.create_lf_tag.side_effect = [creation_error]

            with pytest.raises(ClientError):
                lakeformation_response = self.lakeformation_resource._LakeFormationResource__update_or_create_classification(
                    catalog_id="123456789",
                    key="data_classification",
                    values=["business", "technical"]
                )

    def test_get_classification_exceptions(self):
        """
        Testing expected exceptions and one unexpected when getting classification
        """

        entity_not_found_error = self.get_exception_from_get_lf_tag(
            exception="EntityNotFoundException")
        self.mock_lf.get_lf_tag.side_effect = [entity_not_found_error]

        resource_response = self.lakeformation_resource._LakeFormationResource__get_classification(
            catalog_id="123456789",
            key="data_classification"
        )

        assert resource_response["TagValues"] == []

        for exception in [
            "AccessDeniedException",
            "InternalServiceException",
            "InvalidInputException",
            "OperationTimeoutException",
                "UnexpectedException"]:

            get_lf_tag_error = self.get_exception_from_get_lf_tag(
                exception=exception)
            self.mock_lf.get_lf_tag.side_effect = [get_lf_tag_error]

            with pytest.raises(ClientError):
                lakeformation_response = self.lakeformation_resource._LakeFormationResource__get_classification(
                    catalog_id="123456789",
                    key="data_classification"
                )

    def test_succesfully_get_classification(self):
        self.mock_lf.get_lf_tag.side_effect = [
            self.mock_get_lf_tag(catalog_id="123456789", tag_key="pii")]

        lakeformation_response = self.lakeformation_resource._LakeFormationResource__get_classification(
            catalog_id="123456789",
            key="pii"
        )

        assert lakeformation_response["TagKey"] == "pii"
        assert lakeformation_response["TagValues"] == [
            "true",
            "false"
        ]

    def test_succesfully_update_classification(self):

        self.mock_lf.get_lf_tag.side_effect = [
            self.mock_get_lf_tag(catalog_id="123456789", tag_key="pii")]
        self.mock_lf.update_lf_tag.side_effect = [
            self.mock_create_or_update_lf_tag()]

        lakeformation_response = self.lakeformation_resource._LakeFormationResource__update_or_create_classification(
            catalog_id="123456789",
            key="pii",
            values=["true", "false", "Hi-Z"]
        )

        assert lakeformation_response == None

    def test_succesfully_create_classification(self):

        self.mock_lf.get_lf_tag.side_effect = [
            self.mock_get_lf_tag(catalog_id="123456789", tag_key="pii")
        ]

        client_error = self.get_exception_from_update_lf_tag(
            exception="EntityNotFoundException")
        self.mock_lf.update_lf_tag.side_effect = [client_error]

        lakeformation_response = self.lakeformation_resource._LakeFormationResource__update_or_create_classification(
            catalog_id="123456789",
            key="data_classification",
            values=["business", "technical"]
        )

        assert lakeformation_response == None

    @mock.patch("plugin.infrastructure.resource.aws.services.lakeformation.service.boto3", autospec=True)
    def test_succesfully_create_all_classifications(self, mock_boto3):
        mock_session = mock_boto3.Session.return_value
        mock_lf = mock_session.client.return_value

        mock_lf.get_lf_tag.side_effect = [
            {
                "TagValues": []
            },
            {
                "TagValues": []
            },
            {
                "TagValues": []
            },
            {
                "TagValues": []
            },
            {
                "TagValues": []
            }
        ]

        client_error = self.get_exception_from_update_lf_tag(
            exception="EntityNotFoundException")
        mock_lf.update_lf_tag.side_effect = [
            client_error, client_error, client_error, client_error, client_error]

        lf = LakeFormation()

        lf.create_classifications("123456789", "us-east-1")
