from abc import ABCMeta, abstractmethod
from plugin.domain.manifest import DataLake




class DataLakeInterface(metaclass=ABCMeta):
    """
    A Data Lake stack is an infrastructure base that creates an environment
    to store data in a structured and/or semi-structured way, this plugin
    has the following features:

        Data repository
        Taxonomy
    """
    @abstractmethod
    def create(self, datalake: DataLake):
        raise NotImplementedError
