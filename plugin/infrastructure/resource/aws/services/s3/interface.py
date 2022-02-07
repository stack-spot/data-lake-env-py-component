from __future__ import annotations
from abc import ABCMeta, abstractmethod

from plugin.domain.manifest import DataLake

class S3ResourceInterface(metaclass=ABCMeta):
    """
    TO DO
    """

    @abstractmethod
    def check_buckets(self, account: str, datalake: DataLake) -> dict:
        raise NotImplementedError
