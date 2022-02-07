from plugin.infrastructure.resource.interface import DataLakeCloudInterface
from plugin.usecase.datalake.interface import DataLakeInterface
from plugin.domain.manifest import DataLake
from plugin.infrastructure.resource.aws.services.main import SDK


class DataLakeUseCase(DataLakeInterface):
    """
    TODO
    """
    cloud: DataLakeCloudInterface

    def __init__(self, cloud: DataLakeCloudInterface) -> None:
        self.cloud = cloud

    def create(self, datalake: DataLake):
        cloud_service = SDK()
        bucket = cloud_service.check_buckets(cloud_service.account_id, datalake)
        self.cloud.create_datalake_stack(datalake, bucket)
        return True
