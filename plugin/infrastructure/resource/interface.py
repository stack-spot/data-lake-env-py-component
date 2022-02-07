from abc import ABCMeta, abstractmethod
from plugin.domain.manifest import DataLake


class DataLakeCloudInterface(metaclass=ABCMeta):
    """
    A Data Lake stack is an infrastructure base that creates an environment
    to store data in a structured and/or semi-structured way, this plugin
    has the following features:

        Data repository
        Taxonomy
    """
    @abstractmethod
    def create_datalake_stack(self, datalake: DataLake, bucket: dict):
        """
        TO DO
        """
        raise NotImplementedError
