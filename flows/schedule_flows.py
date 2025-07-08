from prefect import flow
import datetime
from typing import Sequence, Tuple, Optional, List

import dlt
from dlt.sources.sql_database import sql_database, sql_table, Table, remove_nullability_adapter
from prefect import flow, task
from prefect.logging import get_run_logger

@task
def schedule_refresh(
        dataset_name: str,
        source_name: str
) -> None:

    logger = get_run_logger()
    pipeline = dlt.pipeline(
        pipeline_name="daily_import",
        destination='postgres',
        dataset_name=dataset_name,
    )

    schedule_table = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="sched_template_shift_assignments",
        included_columns=["id",
                          "template_id",
                          "user_id",
                          "shift_id",
                          "start_time",
                          "end_time",
                          "schedule_id",
                          "unit_id",
                          "vehicle_id",
                          "slot",
                          "date_line",
                          "modified",
                          "cost_center_id",
                          "earning_code_id"],
        write_disposition="merge",
        primary_key="id",
        incremental=dlt.sources.incremental("modified",
                                            initial_value=datetime.datetime(2025, 1, 1, 0, 0, 0))
    )

    info = pipeline.run(schedule_table, write_disposition="merge")
    logger.info(f"Finished loading table {info}")

@flow
def schedule_refresh_flow(
        dataset_name: str,
        source_name: str
) -> None:
    schedule_refresh(dataset_name, source_name)

if __name__ == "__main__":
    schedule_refresh_flow("traumasoft_tn","tn_database")

