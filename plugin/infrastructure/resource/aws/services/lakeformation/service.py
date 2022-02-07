from .interface import LakeFormationResourceInterface
from botocore.client import ClientError
import boto3
from plugin.utils.logging import logger


class LakeFormationResource(LakeFormationResourceInterface):
    """
    TO DO

    Args:
        LakeFormationResourceInterface ([type]): [description]
    """

    def __init__(self, region: str):
        session = boto3.Session()
        self.lakeformation_client = session.client(
            "lakeformation", region_name=region)

    def __update_or_create_classification(self, catalog_id: str, key: str, values: list):

        classification = self.__get_classification(
            catalog_id=catalog_id, key=key)

        try:
            values_to_add = [
                value for value in values if not value in classification['TagValues']
            ]

            if len(values_to_add) > 0:
                self.lakeformation_client.update_lf_tag(
                    CatalogId=catalog_id,
                    TagKey=key,
                    TagValuesToAdd=values_to_add
                )
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                self.__create_classification(
                    catalog_id=catalog_id, key=key, values=values)
            else:
                if e.response['Error']['Code'] in set(['InvalidInputException',
                                                       'InternalServiceException',
                                                       'OperationTimeoutException',
                                                       'ConcurrentModificationException',
                                                       'AccessDeniedException']):

                    logger.error(
                        'An error occurred while updating classification: %s', e)
                else:
                    logger.error(
                        'Unexpected error occurred while updating classification: %s', e)

                raise ClientError(
                    e.response, 'UpdateLFTag') from e

    def __create_classification(self, catalog_id: str, key: str, values: list):
        try:
            logger.info(
                "Creating classification '%s' with values: %s", key, values)
            self.lakeformation_client.create_lf_tag(
                CatalogId=catalog_id,
                TagKey=key,
                TagValues=values
            )
        except ClientError as e:
            if e.response['Error']['Code'] in set(['InvalidInputException',
                                                  'ResourceNumberLimitExceededException',
                                                   'InternalServiceException',
                                                   'OperationTimeoutException',
                                                   'AccessDeniedException']):
                logger.error(
                    "An error occurred while creating classification: %s", e)
                raise ClientError(
                    e.response, "CreateLFTag") from e

            logger.error(
                "Unexpected error while creating classification: %s", e)
            raise ClientError(
                e.response, 'CreateLFTag') from e

    def create_classifications(self, account_id: str):
        self.__update_or_create_classification(
            catalog_id=account_id,
            key="pii",
            values=["true", "false"])
        self.__update_or_create_classification(
            catalog_id=account_id,
            key="data_classification",
            values=["business", "technical"])
        self.__update_or_create_classification(
            catalog_id=account_id,
            key="manipulated",
            values=["true", "false"])
        self.__update_or_create_classification(
            catalog_id=account_id,
            key="access_level",
            values=["full", "sensitive", "partial", "restrict"])
        self.__update_or_create_classification(
            catalog_id=account_id,
            key="private",
            values=["true", "false"])

    def __get_classification(self, catalog_id: str, key: str):
        try:
            return self.lakeformation_client.get_lf_tag(
                CatalogId=catalog_id,
                TagKey=key
            )
        except ClientError as e:

            if e.response['Error']['Code'] == 'EntityNotFoundException':
                return {
                    "TagValues": []
                }

            if e.response['Error']['Code'] in set(['InvalidInputException',
                                                  'InternalServiceException',
                                                   'OperationTimeoutException',
                                                   'AccessDeniedException']):
                logger.error(
                    "An error occurred while getting classification: %s", e)
                raise ClientError(
                    e.response, "GetLFTag") from e

            logger.error("Unexpected error while getting classification: %s", e)
            raise ClientError(
                e.response, 'GetLFTag') from e
