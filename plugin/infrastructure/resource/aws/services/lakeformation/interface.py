from abc import ABCMeta, abstractmethod

class LakeFormationResourceInterface(metaclass=ABCMeta):
    """
    TO DO
    """

    @abstractmethod
    def create_classifications(self, account_id: str) -> None:
        raise NotImplementedError
