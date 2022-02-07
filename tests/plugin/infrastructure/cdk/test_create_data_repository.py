from aws_cdk import (core, assertions)
from plugin.infrastructure.resource.aws.cdk.stacks.data_repository import DataRepository


def test_aws_cdk_create_bucket_raw():
    app = core.App()
    stack_name = "create-data-repository-stack"
    data_stack = DataRepository(app, stack_name)
    bucket_name = "12345-raw-bucket"
    data_stack.create_bucket(bucket_name)

    template = assertions.Template.from_stack(data_stack)
    template.has_resource_properties("AWS::S3::Bucket", {
        "BucketName": bucket_name,
        "LifecycleConfiguration": {
            "Rules": [
                {
                    "Id": "default_trusted",
                    "Status": "Enabled",
                    "Transitions": [
                        {
                            "StorageClass": "STANDARD_IA",
                            "TransitionInDays": 30
                        },
                        {
                            "StorageClass": "GLACIER",
                            "TransitionInDays": 60
                        }
                    ]
                }
            ]
        }
    })


def test_aws_cdk_create_bucket_clean():
    app = core.App()
    stack_name = "create-data-repository-stack"
    data_stack = DataRepository(app, stack_name)
    bucket_name = "12345-clean-bucket"
    data_stack.create_bucket(bucket_name, versioned=True)

    template = assertions.Template.from_stack(data_stack)
    template.has_resource_properties("AWS::S3::Bucket", {
        "BucketEncryption": {
            "ServerSideEncryptionConfiguration": [
                {
                    "ServerSideEncryptionByDefault": {
                        "SSEAlgorithm": "AES256"
                    }
                }
            ]
        },
        "BucketName": bucket_name,
        "LifecycleConfiguration": {
            "Rules": [
                {
                    "Id": "default_trusted",
                    "NoncurrentVersionTransitions": [
                        {
                            "StorageClass": "STANDARD_IA",
                            "TransitionInDays": 30
                        },
                        {
                            "StorageClass": "GLACIER",
                            "TransitionInDays": 60
                        }
                    ],
                    "Status": "Enabled",
                    "Transitions": [
                        {
                            "StorageClass": "STANDARD_IA",
                            "TransitionInDays": 30
                        },
                        {
                            "StorageClass": "GLACIER",
                            "TransitionInDays": 60
                        }
                    ]
                }
            ]
        },
        "PublicAccessBlockConfiguration": {
            "BlockPublicAcls": True,
            "BlockPublicPolicy": True,
            "IgnorePublicAcls": True,
            "RestrictPublicBuckets": True
        },
        "VersioningConfiguration": {
            "Status": "Enabled"
        }
    })


def test_aws_cdk_create_buckets():
    app = core.App()
    stack_name = "create-data-repository-stack"
    data_stack = DataRepository(app, stack_name)
    account_id = "12345"
    name = "data-name"
    data_stack.create_buckets(account_id, name)

    template = assertions.Template.from_stack(data_stack)
    template.has_resource_properties("AWS::S3::Bucket", {
        "BucketName": account_id + "-raw-" + name,
        "LifecycleConfiguration": {
            "Rules": [
                {
                    "Id": "default_trusted",
                    "Status": "Enabled",
                    "Transitions": [
                        {
                            "StorageClass": "STANDARD_IA",
                            "TransitionInDays": 30
                        },
                        {
                            "StorageClass": "GLACIER",
                            "TransitionInDays": 60
                        }
                    ]
                }
            ]
        },
    })
    template.has_resource_properties("AWS::S3::Bucket", {
        "BucketName": account_id + "-clean-" + name,
        "LifecycleConfiguration": {
            "Rules": [
                {
                    "Id": "default_trusted",
                    "NoncurrentVersionTransitions": [
                        {
                            "StorageClass": "STANDARD_IA",
                            "TransitionInDays": 30
                        },
                        {
                            "StorageClass": "GLACIER",
                            "TransitionInDays": 60
                        }
                    ],
                    "Status": "Enabled",
                    "Transitions": [
                        {
                            "StorageClass": "STANDARD_IA",
                            "TransitionInDays": 30
                        },
                        {
                            "StorageClass": "GLACIER",
                            "TransitionInDays": 60
                        }
                    ]
                }
            ]
        },
    })
