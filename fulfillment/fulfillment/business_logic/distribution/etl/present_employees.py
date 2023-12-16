import logging
from dataclasses import dataclass

import pandas as pd
import pendulum

from fulfillment.business_logic.distribution.etl.queries.roster import get_roster_query
from fulfillment.business_logic.distribution.etl.queries.present_employees import (
    get_present_employees_query,
)
from fulfillment.helpers.batches import get_batches
from fulfillment.helpers.database.database import Database
from fulfillment.helpers.environment.env import get_stage


@dataclass
class PresentEmployees:
    stage: str = get_stage()

    def get_present_employees(self, database: Database) -> pd.DataFrame:
        logging.info("[PresentEmployees][get_present_employees] Start")

        df = database.get_dataframe(query=get_present_employees_query())

        logging.info("[PresentEmployees][get_present_employees] End")

        return df

    def get_roster(self, database: Database) -> pd.DataFrame:
        logging.info("[PresentEmployees][get_roster] Start")

        df = database.get_dataframe(query=get_roster_query())

        logging.info("[PresentEmployees][get_roster] End")

        return df

    def parse_data(
            self,
            present_employees_df: pd.DataFrame,
            roster_df: pd.DataFrame) -> list[dict[any, any]]:
        logging.info("[PresentEmployees][parse_data] Start")

        present_employees_df = present_employees_df.astype({"employee_id": int})
        df = pd.merge(left=present_employees_df,
                      right=roster_df,
                      on=["employee_id", "warehouse_id"],
                      how="inner")

        dfs = []
        for site, grouped_df in df.groupby(by="warehouse_id"):
            grouped_df = grouped_df.sort_values(by="login_name")

            partition = f"{self.stage.upper()}#{site}#FULFILLMENT#PRESENT_EMPLOYEES"
            entity = "PRESENT_EMPLOYEES#BATCH"

            batch_data = []
            for idx, batch in enumerate(get_batches(data=grouped_df, n=100), start=1):
                employee_ids = " | ".join(batch["employee_id"].astype(str))
                login_names = " | ".join(batch["login_name"])
                event_datetime = " | ".join(batch["event_datetime"])
                batch_data.append(
                    {
                        "partition": partition,
                        "entity": f"{entity}{idx}",
                        "employeeId": employee_ids,
                        "loginName": login_names,
                        "eventDatetime": event_datetime,
                    }
                )

            final_df = pd.DataFrame(batch_data)
            while len(final_df) < 3:
                final_df = pd.concat(
                    [
                        final_df,
                        pd.DataFrame(
                            [
                                {
                                    "partition": partition,
                                    "entity": f"{entity}{len(final_df) + 1}",
                                    "employeeId": "",
                                    "loginName": "",
                                    "eventDatetime": "",
                                }
                            ]
                        ),
                    ],
                    ignore_index=True,
                )

            final_df = final_df
            dfs.append(final_df)

        merged_dfs = pd.concat(dfs)
        logging.info(merged_dfs.head())
        data = merged_dfs.to_dict(orient="records")

        logging.info("[PresentEmployees][parse_data] End")

        return data
