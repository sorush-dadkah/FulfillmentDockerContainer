import logging

from fulfillment.business_logic.distribution.etl.present_employees import PresentEmployees
from fulfillment.helpers.database.dynamodb import DynamoDB
from fulfillment.helpers.database.postgresql import PostgreSQL
from fulfillment.helpers.logging.timer import timing
from fulfillment.helpers.threading.thread_pool_executor import (
    start_thread_pool_executor,
)

@timing()
def handler(event: any, context: any) -> dict[str, str] | None:
    logging.info("[present_employees][handler] Start")

    etl = PresentEmployees()
    postgresql = PostgreSQL()
    df = etl.get_present_employees(database=postgresql)

    if df.empty:
        logging.warning("[present_employees][handler] No data returned")
        return None
    else:
        items = etl.parse_data(df=df)

        logging.info(f"[present_employees][handler] Writing {len(items)} items to DynamoDB")

        start_thread_pool_executor(
            max_workers=100,
            func=DynamoDB(table_name="metrics").write_in_parallel,
            items=items,
        )

        logging.info(f"[present_employees][handler] Finished writing {len(items)} to DynamoDB")

    logging.info("[present_employees][handler] End")

    return {"Stage": etl.stage}
