from prefect import flow
import time
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
        pipeline_name=f"sched_template_{dataset_name}_{int(time.time())}",
        destination='postgres',
        dataset_name=dataset_name,
    )

    def filter_by_date(query, table):
        if table.name == "sched_template_shift_assignments":
            # Only select rows where the column customer_id has value 1
            return query.where(table.c.date_line > '2025-01-01')
        # Use the original query for other tables
        if table.name == "timesheet":
            return query.where(table.c.date_created > '2025-01-01')
        return query

    schedule_table = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="sched_template_shift_assignments",
        query_adapter_callback = filter_by_date,
    )


    schedule_table.apply_hints(
        primary_key="id")

    timesheet_table = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="timesheet",
        query_adapter_callback=filter_by_date,
    )
    timesheet_table.apply_hints(primary_key="time_id")

    # Remove the diagnostic logging that consumes the iterators
    logger.info("Starting pipeline run with schedule and timesheet tables...")

    info = pipeline.run([schedule_table, timesheet_table], write_disposition="replace")
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

