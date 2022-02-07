from __future__ import annotations
from plugin.domain.manifest import DataLake
from .service import S3Resource


class S3:
    """
    TO DO
    """

    @staticmethod
    def check_buckets(account: str, datalake: DataLake) -> dict:
        s3 = S3Resource(datalake.region)
        return s3.check_buckets(account, datalake)
