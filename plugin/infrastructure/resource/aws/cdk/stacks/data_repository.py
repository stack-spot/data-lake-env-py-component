from aws_cdk import aws_s3 as s3
from aws_cdk import core as cdk


class DataRepository(cdk.Stack):
    """
    This class is responsible for creating the buckets. A bucket can have the following configurations:
        versioned: True or False
        lifecycle_configuration: 
            Transitions: STANDARD_IA 30 days and GLACIER 60 days
            NoncurrentVersionTransition:
                (Only if the versioned flag is True)
                STANDARD_IA 30 days and GLACIER 60 days
        PublicAccessBlockConfiguration:
            (Only if the block_public_access flag is True)
            block_public_acls=True,
            block_public_policy=True,
            ignore_public_acls=True,
            restrict_public_buckets=True
        RemovalPolicy: DESTROY
        BucketEncryption: AES256
    """

    def create_bucket(self, name: str, versioned=False, block_public_access=True):
        bucket = s3.CfnBucket(
            self,
            id=f"dp-{name}",
            bucket_name=name,
            versioning_configuration=s3.CfnBucket.VersioningConfigurationProperty(
                status="Enabled"
            ) if versioned else None,
            bucket_encryption=s3.CfnBucket.BucketEncryptionProperty(
                server_side_encryption_configuration=[
                    s3.CfnBucket.ServerSideEncryptionRuleProperty(
                        server_side_encryption_by_default=s3.CfnBucket.
                        ServerSideEncryptionByDefaultProperty(
                            sse_algorithm="AES256"
                        )
                    )
                ]
            ),
            lifecycle_configuration=s3.CfnBucket.LifecycleConfigurationProperty(
                rules=[
                    s3.CfnBucket.RuleProperty(
                        status="Enabled",
                        id="default_trusted",
                        noncurrent_version_transitions=[
                            s3.CfnBucket.NoncurrentVersionTransitionProperty(
                                storage_class="STANDARD_IA",
                                transition_in_days=30
                            ),
                            s3.CfnBucket.NoncurrentVersionTransitionProperty(
                                storage_class="GLACIER",
                                transition_in_days=60
                            )
                        ] if versioned else None,
                        transitions=[
                            s3.CfnBucket.TransitionProperty(
                                storage_class="STANDARD_IA",
                                transition_in_days=30
                            ),
                            s3.CfnBucket.TransitionProperty(
                                storage_class="GLACIER",
                                transition_in_days=60
                            )
                        ]
                    )
                ]
            ),
            public_access_block_configuration=s3.CfnBucket.
            PublicAccessBlockConfigurationProperty(
                block_public_acls=True,
                block_public_policy=True,
                ignore_public_acls=True,
                restrict_public_buckets=True
            ) if block_public_access else None,
        )
        bucket.apply_removal_policy(
            default=cdk.RemovalPolicy.DESTROY
        )

    def create_buckets(self, account: str, datalake_name: str):
        self.create_bucket(f"{account}-raw-{datalake_name}")
        self.create_bucket(
            f"{account}-clean-{datalake_name}",
            versioned=True)
