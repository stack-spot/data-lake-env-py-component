from plugin.infrastructure.resource.interface import DataLakeCloudInterface
from .stacks import Stack
from .engine.main import CDKEngine
from plugin.domain.manifest import DataLake
from plugin.infrastructure.resource.aws.services.main import SDK
from plugin.utils.logging import logger


class AwsCdk(CDKEngine, DataLakeCloudInterface):
    """
    TODO
    """

    def create_datalake_stack(self, datalake: DataLake, bucket: dict):
        self.new_app()
        cloud_service = SDK()
        account_id = cloud_service.account_id
        stack_name = f'create-{datalake.name}-{bucket["name"]}-datalake-stack'.replace(
            '_', '-')
        stack = Stack(self.app, stack_name)
        if bucket["raw"] != "":
            stack.create_bucket(bucket["raw"])
        else:
            logger.info("bucket %s-raw-%s already exists.", account_id, datalake.name)

        if bucket["clean"] != "":
            stack.create_bucket(bucket["clean"], versioned=True)
        else:
            logger.info("bucket %s-clean-%s already exists.", account_id, datalake.name)
        stack.register_repositories(account_id, datalake.name)
        stack.create_classifications(cloud_service, datalake.region)
        if bucket["name"] != "":
            self.deploy(stack_name, datalake.region)

    
