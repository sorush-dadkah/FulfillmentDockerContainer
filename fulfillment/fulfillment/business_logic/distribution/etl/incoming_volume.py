import logging
from dataclasses import dataclass

import pandas as pd

from fulfillment.business_logic.distribution.etl.incoming_volume import IncomingVolume
from fulfillment.helpers.database.dynamodb import DynamoDB
from fulfillment.helpers.database.postgresql import PostgreSQL
from fulfillment.helpers.logging.timer import timing
from fulfillment.helpers.threading.thread_pool_executor import (
    start_thread_pool_executor,
)


@dataclass
class IncomingVolume:
    stage: str = get_stage()

    def get_incoming_volume(self, database: Database) -> pd.DataFrame:
        logging.info("[IncomingVolume][get_incoming_volume] Start")

        query = get_incoming_volume_query(stage=self.stage)
        df = database.get_dataframe(query=query)

        logging.info("[IncomingVolume][get_incoming_volume] End")

        return df

    def parse_data(self, df: pd.DataFrame) -> list[dict[any, any]]:
        logging.info("[IncomingVolume][parse_data] Start")

        df = df.rename(
            columns={
                "fc": "partition",
                "expected_datetime": "sourceTimestamp",
                "temp_zone": "tempZone",
            }
        )
        data = df.to_dict(orient="records")

        logging.info("[IncomingVolume][parse_data] End")

        return data
