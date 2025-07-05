import datetime
import dlt
from dlt.sources.sql_database import sql_database, sql_table, Table
from prefect import flow, task
from prefect.logging import get_run_logger


@flow
def load_cad_trips(
        dataset_name: str,
        source_name: str,
) -> None:
    logger = get_run_logger()
    pipeline = dlt.pipeline(
        pipeline_name="load_cad_trips",
        destination='postgres',
        dataset_name=dataset_name,
    )

    tn_db = sql_database(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table_names=dlt.config[f"sources.{source_name}.tables"],
    )

    tn_db.cad_trip_legs.apply_hints(
        incremental=dlt.sources.incremental(
            "created",
            initial_value=datetime.datetime(2025,6,1,0,0,0)
        ),primary_key="id"
    )
    tn_db.cad_trip_legs_rev.apply_hints(
        incremental=dlt.sources.incremental(
            "modified",
            initial_value=datetime.datetime(2025,6,1,0,0,0)
        ),primary_key="leg_id"
    )

    info = pipeline.run(tn_db)
    logger.info(info)