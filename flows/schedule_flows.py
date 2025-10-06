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
        pipeline_name=f"sched_template_{dataset_name}",
        destination='postgres',
        dataset_name=dataset_name,
    )

    schedule_table = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="sched_template_shift_assignments",
    )

    schedule_table.apply_hints(primary_key="id")


    info = pipeline.run(schedule_table, write_disposition="replace")
    logger.info(f"Finished loading table {info}")

@flow
def schedule_refresh_flow(
        dataset_name: str,
        source_name: str
) -> None:
    schedule_refresh(dataset_name, source_name)

if __name__ == "__main__":
    schedule_refresh_flow("traumasoft_tn","tn_database")
    schedule_refresh_flow("traumasoft_il", "il_database")
    schedule_refresh_flow("traumasoft_mi", "mi_database")

