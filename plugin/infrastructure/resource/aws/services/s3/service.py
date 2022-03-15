from __future__ import annotations
from plugin.domain.manifest import DataLake
from .interface import S3ResourceInterface
from botocore.client import ClientError
import boto3
from plugin.utils.logging import logger


class S3Resource(S3ResourceInterface):
    """
    TO DO

    Args:
        S3ResourceInterface ([type]): [description]
    """

    def __init__(self, region: str):
        session = boto3.Session()
        self.s3 = session.client(
            "s3", region_name=region)

    def check_buckets(self, account: str, datalake: DataLake) -> dict:

        datalake_name = datalake.name.replace('_', '-')

        clean_bucket_name = f"{account}-clean-{datalake_name}"
        raw_bucket_name = f"{account}-raw-{datalake_name}"

        try:
            res = self.s3.list_buckets()
            buckets = res["Buckets"]

            response = {"raw": raw_bucket_name,
                        "clean": clean_bucket_name, "name": ""}

            for bucket in buckets:
                if bucket["Name"] == clean_bucket_name:
                    response["clean"] = ""
                if bucket["Name"] == raw_bucket_name:
                    response["raw"] = ""

            response["name"] += "r" if response["raw"] != "" else ""
            response["name"] += "c" if response["clean"] != "" else ""

            return response
        except ClientError as e:
            logger.error(
                'An error occurred while checking resources: %s', e)

            response = {"raw": raw_bucket_name,
                        "clean": clean_bucket_name, "name": ""}

            return response
