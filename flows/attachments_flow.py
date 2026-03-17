import time
import datetime
import dlt
from dlt.sources.sql_database import sql_table
from prefect import flow, task
from prefect.logging import get_run_logger


@task
def load_attachments_pipeline(
        dataset_name: str,
        source_name: str
) -> None:
    """
    Incrementally load attachment-related tables.

    - attachments: incremental on 'date' column
    - attachments_log: incremental on 'timestamp' column
    - cad_trip_leg_attachments: links attachments to trip legs
    - cad_trip_leg_attachment_types: links attachments to attachment types
    """
    logger = get_run_logger()
    pipeline = dlt.pipeline(
        pipeline_name=f"attachments_{dataset_name}_{int(time.time())}",
        destination='postgres',
        dataset_name=dataset_name,
    )

    # attachments table - incremental on 'date'
    attachments = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="attachments",
    )
    attachments.apply_hints(
        primary_key="id",
        write_disposition="merge",
        incremental=dlt.sources.incremental(
            "date",
            initial_value=datetime.datetime(2026, 1, 1, 0, 0, 0)
        ),
    )

    # attachments_log table - incremental on 'timestamp'
    attachments_log = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="attachments_log",
    )
    attachments_log.apply_hints(
        primary_key="id",
        write_disposition="merge",
        incremental=dlt.sources.incremental(
            "timestamp",
            initial_value=datetime.datetime(2026, 1, 1, 0, 0, 0)
        ),
    )

    # cad_trip_leg_attachments - links attachments to trip legs
    leg_attachments = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="cad_trip_leg_attachments",
    )
    leg_attachments.apply_hints(
        primary_key="id",
        write_disposition="merge",
    )

    # cad_trip_leg_attachment_types - junction table (composite key: trip_leg_attachment_id, type_id)
    leg_attachment_types = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="cad_trip_leg_attachment_types",
    )
    leg_attachment_types.apply_hints(
        primary_key=["trip_leg_attachment_id", "type_id"],
        write_disposition="merge",
    )

    info = pipeline.run([attachments, attachments_log, leg_attachments, leg_attachment_types])
    logger.info(f"Finished loading attachments tables: {info}")


@flow
def load_attachments(
        dataset_name: str,
        source_name: str
) -> None:
    """Load attachments and attachments_log for a single database."""
    load_attachments_pipeline(dataset_name, source_name)


if __name__ == "__main__":
    load_attachments("traumasoft_tn", "tn_database")
    load_attachments("traumasoft_il", "il_database")
    load_attachments("traumasoft_mi", "mi_database")