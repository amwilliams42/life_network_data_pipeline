from prefect import flow
import datetime
from typing import Sequence, Tuple, Optional, List

import dlt
from dlt.sources.sql_database import sql_database, sql_table, Table, remove_nullability_adapter
from prefect import flow, task
from prefect.logging import get_run_logger

tables: list[tuple[str, str | None, list[str] | None]] = [
    # (tablename, modified column, columns to pull, None for all)
    ('sched_unit_types', None, None),
    ('ibd_level_of_service', None, None),
    ('ibd_subzones', None, None),
    ('cad_sources', None, None),
    ('users', None, ['user_id', 'employee_num', 'employee_level', 'employee_licensure', 'username', 'job_title',
                     'job_title_id', 'first_name', 'last_name', 'preferred_first_name', 'email_address', 'user_group',
                     'user_division', 'division', 'disabled', 'deactivated']),
    ('sched_units', None, None),
]

@task
def daily_import_pipeline(
        dataset_name: str,
        source_name: str
) -> None:
    """
    Load tables from database with flexible configuration.

    Args:
        dataset_name: Name of the dataset to load into
        source_name: Name of the source configuration
        tables: List of tuples containing:
            - table_name: Name of the table to load
            - modified_column: Column name for incremental loading (None for full reload)
            - columns_to_pull: List of specific columns to pull (None for all columns)
    """
    logger = get_run_logger()
    pipeline = dlt.pipeline(
        pipeline_name="daily_import",
        destination='postgres',
        dataset_name=dataset_name,
    )
    table_sources = []
    for table_name, modified_column, columns_to_pull in tables:
        table_source = sql_table(
            credentials=dlt.secrets[f"sources.{source_name}.credentials"],
            table=table_name,
            included_columns=columns_to_pull
        )
        table_sources.append(table_source)
        logger.info(f"Configured table: {table_name}")

    info = pipeline.run(table_sources, write_disposition="merge")
    logger.info(f"Finished loading table {info}")

@flow
def daily_import(
        dataset_name: str,
        source_name: str,
):
    daily_import_pipeline(dataset_name, source_name)

if __name__ == "__main__":
    daily_import("traumasoft_tn", "tn_database")