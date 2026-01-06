"""
BigQuery Export Flow

Exports models from the dbt 'bigquery' schema in PostgreSQL to Google BigQuery.
Runs every 4 hours to keep BigQuery data current for PowerBI consumption.
"""

import time

import dlt
from dlt.sources.sql_database import sql_database
from prefect import flow
from prefect.logging import get_run_logger


# Tables in the bigquery schema to export
BIGQUERY_TABLES = [
    "daily_dashboard",
]


@flow
def export_to_bigquery(
    postgres_schema: str = "bigquery",
    bigquery_dataset: str = "lan_analytics",
) -> None:
    """
    Export tables from PostgreSQL bigquery schema to Google BigQuery.

    Args:
        postgres_schema: The PostgreSQL schema containing the dbt bigquery models
                        (dbt prefixes schema names with the database name)
        bigquery_dataset: The BigQuery dataset to write to
    """
    logger = get_run_logger()

    logger.info(f"Starting BigQuery export from {postgres_schema} to {bigquery_dataset}")

    # Create pipeline with BigQuery destination
    pipeline = dlt.pipeline(
        pipeline_name=f"bigquery_export_{int(time.time())}",
        destination="bigquery",
        dataset_name=bigquery_dataset,
    )

    # Source from local PostgreSQL (the dbt output database)
    source = sql_database(
        credentials=dlt.secrets["sources.local_postgres.credentials"],
        schema=postgres_schema,
        table_names=BIGQUERY_TABLES,
    )

    # Run the pipeline - replace tables each time for clean data
    info = pipeline.run(source, write_disposition="replace")

    logger.info(f"BigQuery export complete: {info}")


@flow
def export_all_to_bigquery() -> None:
    """Export all bigquery schema tables to BigQuery."""
    export_to_bigquery()


if __name__ == "__main__":
    export_all_to_bigquery()
