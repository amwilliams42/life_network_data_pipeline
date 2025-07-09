from prefect import flow
import datetime
from typing import Sequence, Tuple, Optional, List

import dlt
from dlt.sources.sql_database import sql_database, sql_table, Table, remove_nullability_adapter
from prefect import flow, task
from prefect.logging import get_run_logger

@task
def export_batches(
        dataset_name: str,
        source_name: str
) -> None:

    logger = get_run_logger()
    pipeline = dlt.pipeline(
        pipeline_name="daily_import",
        destination='postgres',
        dataset_name=dataset_name,
    )

    batches = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="epcr_v3_submit_batches",
    )
    batches_results = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="epcr_v3_submit_batches_results",
    )
    export_trigger = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="epcr_v3_export_trigger_log",
    )

    info = pipeline.run([batches,batches_results,export_trigger])
    logger.info(f"Finished loading table {info}")

@flow
def export_flow(
        dataset_name: str,
        source_name: str
) -> None:
    export_batches(dataset_name, source_name)

if __name__ == "__main__":
    export_flow("traumasoft_tn","tn_database")

