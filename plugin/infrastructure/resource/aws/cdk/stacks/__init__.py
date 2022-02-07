from aws_cdk import core as cdk

from .data_repository import DataRepository
from .taxonomy import Taxonomy


class Stack(DataRepository, Taxonomy):
    """
    TO DO
    Args:
        DataRepository ([type]): [description]
        Taxonomy ([type]): [description]
    """

    def __init__(self, scope: cdk.Construct,
                 construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
