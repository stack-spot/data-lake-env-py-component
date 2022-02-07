from botocore.exceptions import ClientError
import pytest
import os
import string
import secrets
from unittest import (mock, TestCase)
from plugin.infrastructure.resource.aws.services.main import SDK


def get_random_secret() -> str:
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(30))


class SDKTest(TestCase):

    @mock.patch.dict(os.environ, {
        "AWS_ACCESS_KEY_ID": get_random_secret(),
        "AWS_SECRET_ACCESS_KEY": get_random_secret(),
        "AWS_SESSION_TOKEN": get_random_secret()
    })
    def test_raises_error_if_invalid_credentials(self):

        with pytest.raises(SystemExit):
            sdk = SDK()
            account_id = sdk.account_id

    @mock.patch("plugin.infrastructure.resource.aws.services.main.boto3", autospec=True)
    def test_succesfully_get_account_id(self, mock_boto3_sts):

        mock_account = {
            "Account": "12345678912"
        }

        sts_client = mock_boto3_sts.client.return_value
        sts_client.get_caller_identity.side_effect = [
            mock_account
        ]

        sdk = SDK()
        account_id = sdk.account_id

        assert account_id == mock_account["Account"]
