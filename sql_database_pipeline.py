# flake8: noqa
import datetime
from typing import Sequence, Tuple

import dlt
from dlt.sources.sql_database import sql_database, sql_table, Table
from prefect import flow, task


@task
def load_cad_trips(
        dataset_name: str,
        source_name: str,
) -> None:
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
    print(info)


@flow
def trips_pipeline(
        name_tuples: Sequence[Tuple[str,str]]
) -> None:
    for tup in name_tuples:
        dataset, source = tup
        load_cad_trips(dataset, source, )

if __name__ == "__main__":
    trips_pipeline([("traumasoft_tn","tn_database")])

