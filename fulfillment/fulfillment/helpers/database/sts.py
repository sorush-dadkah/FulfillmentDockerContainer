import logging

from dataclasses import dataclass

import boto3

from fulfillment.constants.database.database_config import DB_PROPS


@dataclass
class STS:
    sts_props: dict
    client_type: str = "client"

    def __post_init__(self) -> None:
        self.session_client = self.get_session_client()

    def get_session_client(self) -> Any:
        logging.info("[STS][get_session_client] Start")

        temporary_credentials = boto3.client("sts").assume_role(
            RoleArn=self.sts_props["role_arn"],
            RoleSessionName=self.sts_props["role_session_name"],
        )["Credentials"]

        session_client = boto3.client(
            self.sts_props["boto3_client"],
            region_name=self.sts_props["region_name"],
            aws_access_key_id=temporary_credentials["AccessKeyId"],
            aws_secret_access_key=temporary_credentials["SecretAccessKey"],
            aws_session_token=temporary_credentials["SessionToken"],
        )

        logging.info("[STS][get_session_client] End")

        return session_client
