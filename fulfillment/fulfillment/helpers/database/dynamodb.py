import logging

from dataclasses import dataclass, field
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError


@dataclass
class DynamoDB:
    table_name: str
    client_type: str = "client"
    db_props: dict[str, Any] = field(default_factory=lambda: DB_PROPS[get_stage()]["dynamodb"])

    def __post_init__(self):
        self.client = self.get_client()

    def get_client(self):
        return STS(sts_props=self.db_props["sts"], client_type=self.client_type).session_client

    def python_obj_to_dynamo_obj(self, python_obj: dict) -> dict:
        serializer = TypeSerializer()
        return {k: serializer.serialize(v) for k, v in python_obj.items()}

    def dynamo_obj_to_python_obj(self, dynamo_obj: dict) -> dict:
        deserializer = TypeDeserializer()
        return {k: deserializer.deserialize(v) for k, v in dynamo_obj.items()}

    def put_item(self, item: dict) -> dict[str, int]:
        try:
            resp = self.client.put_item(
                TableName=self.table_name, Item=self.python_obj_to_dynamo_obj(item)
            )
            if resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
                logging.critical(resp)
                raise

            return resp
        except ClientError as error:
            logging.info(error.response["Error"]["Code"], error.response["Error"]["Message"])
            raise

    def put_items(self, items: list[dict[any, any]]) -> dict[str, int]:
        for item in items:
            self.put_item(item=item)

        return {"HTTPStatusCode": 200}

    def write_in_parallel(self, item: dict[any, any]) -> None:
        ddb = DynamoDB(table_name=self.table_name)
        ddb.put_item(item=item)

    def get_item(self, item: dict) -> dict[str, any]:
        try:
            return self.client.get_item(
                TableName=self.table_name, Key=self.python_obj_to_dynamo_obj(item)
            )
        except ClientError as error:
            logging.info(error.response["Error"]["Code"], error.response["Error"]["Message"])
            raise

    def query(self, **kwargs) -> dict:
        return self.client.Table(self.table_name).query(**kwargs)

    def scan(self, **kwargs) -> dict:
        return self.client.Table(self.table_name).scan(**kwargs)
