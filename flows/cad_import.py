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
        pipeline_name=f"load_cad_trips_{dataset_name}",
        destination='postgres',
        dataset_name=dataset_name,
    )

    cad_trip_legs_rev = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="cad_trip_legs_rev",
    )

    cad_trip_legs_rev.apply_hints(
        incremental=dlt.sources.incremental(
            "modified",
            initial_value=datetime.datetime(2025,6,1,0,0,0)
        ),primary_key="leg_id"
    )

    cad_trip_legs = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="cad_trip_legs",
    )

    cad_trip_legs.apply_hints(
        incremental=dlt.sources.incremental(
            'created',
            initial_value=datetime.datetime(2024,1,1,0,0,0)
        ),primary_key="id"
    )



    info = pipeline.run([cad_trip_legs_rev, cad_trip_legs])
    logger.info(info)

if __name__ == "__main__":
    load_cad_trips("traumasoft_tn","tn_database")
    load_cad_trips("traumasoft_mi","mi_database")
    load_cad_trips("traumasoft_il","il_database")