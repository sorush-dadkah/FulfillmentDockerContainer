import logging
import os
import ssl
from dataclasses import dataclass, field

import pymysql
from pymysql.connections import Connection
from tenacity import retry, stop_after_attempt, wait_exponential

from fulfillment.constants.database.database_config import DB_PROPS
from fulfillment.helpers.database.database import Database
from fulfillment.helpers.database.sts import STS
from fulfillment.helpers.environment.env import get_stage


@dataclass
class MySQL(Database):
    db_props: dict[str, any] = field(default_factory=lambda: DB_PROPS[get_stage()]["mysql"])
    instance: str = "read"

    def __post_init__(self) -> None:
        self.instance = "".join(filter(str.isalnum, self.instance.lower()))
        self.connection = self.get_connection()

    def get_password(self) -> str:
        session_client = STS(sts_props=self.db_props["sts"]).session_client

        logging.info("[MySQL][get_password] Start")

        password = session_client.generate_db_auth_token(
            DBHostname=self.db_props["host_mapping"][self.instance],
            Port=self.db_props["port"],
            DBUsername=self.db_props["user"],
        )

        logging.info("[MySQL][get_password] End")

        return password

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=8, min=5, max=120),
    )
    def get_connection(self) -> Connection:
        # The connection will retry 4 times (8, 24, 56, 120 seconds)

        logging.info("[MySQL][get_connection] Start")

        kwargs = {
            "database": self.db_props["database"],
            "host": self.db_props["host_mapping"][self.instance],
            "user": self.db_props["user"],
            "password": self.get_password(),
            "port": self.db_props["port"],
            "ssl": ssl.SSLContext(),
        }

        connection = pymysql.connect(**kwargs)

        logging.info("[MySQL][get_connection] End")

        return connection
