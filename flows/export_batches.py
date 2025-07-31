from prefect import flow
import datetime
from typing import Sequence, Tuple, Optional, List

import dlt
from dlt.sources.sql_database import sql_database, sql_table, Table, remove_nullability_adapter
from prefect import flow, task
from prefect.logging import get_run_logger

def transform_zero_dates(item):
    """Traumasoft database is fucking stupid, so we need to set 0000-00-00 00:00:00 to NULL"""
    if isinstance(item, dict):
        for key, value in item.items():
            if key == 'finalize_date' and (value == '0000-00-00 00:00:00' or str(value).startswith('0000-00-00')):
                item[key] = None
    return item

@task
def export_batches(
        dataset_name: str,
        source_name: str
) -> None:

    logger = get_run_logger()
    pipeline = dlt.pipeline(
        pipeline_name=f"daily_import_{dataset_name}",
        destination='postgres',
        dataset_name=dataset_name,
    )
    batches = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="epcr_v3_submit_batches",
    )
    batches.apply_hints(
        primary_key="id",
        write_disposition="merge",
    )
    batches_results = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="epcr_v3_submit_batches_results",
    )
    batches_results.apply_hints(
        primary_key="batch_id",
        write_disposition="merge",
    )
    export_trigger = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="epcr_v3_export_trigger_log",
    )
    export_trigger.apply_hints(
        primary_key="id",
        write_disposition="merge",
    )
    epcr_runs = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="epcr_v2_runs",
    )

    epcr_runs.add_map(transform_zero_dates)
    epcr_runs.apply_hints(
        columns={"finalize_date": {"nullable": True}},
        primary_key="id",
        write_disposition="merge",
    )
    info = pipeline.run([batches,batches_results,export_trigger, epcr_runs])
    logger.info(f"Finished loading table {info}")

@flow
def export_flow(
        dataset_name: str,
        source_name: str
) -> None:
    export_batches(dataset_name, source_name)

if __name__ == "__main__":
    export_flow("traumasoft_tn","tn_database")
    export_flow("traumasoft_il","il_database")
    export_flow("traumasoft_mi","mi_database")