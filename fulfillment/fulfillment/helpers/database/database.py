import logging
from abc import ABC, abstractmethod

import pandas as pd

from fulfillment.helpers.pandas_format import pandas_format

pandas_format()


class Database(ABC):
    db_props: dict[str, any]
    connection: any = None

    @abstractmethod
    def get_connection(self) -> Any:
        ...

    def get_data(self, query: str) -> list[tuple[any, any]]:
        logging.info(msg="[Database][get_data] Start")

        with self.connection.cursor() as cursor:
            cursor.execute(query, args=args)
            data = cursor.fetchall()

        logging.info(msg=f"[Database][get_data] Received {len(data)} rows")

        return list(data)

    def get_dataframe(self, query: str) -> pd.DataFrame:
        logging.info(msg="[Database][get_dataframe] Start")

        with self.connection.cursor() as cursor:
            cursor.execute(query, args=args)
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=[desc[0] for desc in cursor.description])

        logging.info(
            msg=f"[Database][get_dataframe] Received {len(df.index)} rows\n\n {df.head().to_string()}\n"
        )

        return df

    def write_data(self, query: str, return_id: bool = False) -> int | None:
        logging.info(msg="[Database][write_data] Start")

        row_id = None
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            if return_id and "RETURNING id" in query:
                row_id = cursor.fetchone()[0]
                logging.info(f"[Database][write_data] ROW ID: {row_id}")
            elif return_id and "RETURNING id" not in query:
                logging.warning(
                    "[Database][write_data] 'RETURNING id' was not provided in the query"
                )

            self.connection.commit()

        logging.info(msg="[Database][write_data] End")

        return row_id

    def execute(self, query: str) -> None:
        logging.info(msg="[Database][execute] Start")

        with self.connection.cursor() as cursor:
            cursor.execute(query)

        logging.info(msg="[Database][execute] End")
