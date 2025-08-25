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
            initial_value=datetime.datetime(2025,6,1,0,0,0),
        ),primary_key="leg_id"
    )

    cad_trip_legs = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="cad_trip_legs",
    )

    cad_trip_legs.apply_hints(
        incremental=dlt.sources.incremental(
            'created',
            initial_value=datetime.datetime(2024,1,1,0,0,0),
        ),primary_key="id"
    )

    cad_trips = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="cad_trips",
    )

    cad_trips.apply_hints(
        incremental=dlt.sources.incremental(
            'modified',
            initial_value=datetime.datetime(2024,1,1,0,0,0),
        ),primary_key="id"
    )

    qa_status = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="epcr_v2_qaqr_run_status",
    )

    qa_status.apply_hints(
        incremental=dlt.sources.incremental(
            'status_date',
            initial_value=datetime.datetime(2025,6,1,0,0,0)
        ),primary_key="run_id"
    )

    epcr_cad_legs = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="epcr_v2_cad_legs"
    )

    cad_trip_leg_shift_assignments = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="cad_trip_leg_shift_assignments"
    )

    patients = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="ibd_patients",
        included_columns=["patient_id", "first_name", "last_name", "dob"]
    )

    cancel_reasons = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table='cad_trip_cancel_reason',
    )

    lost_call_reasons = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table='cad_lost_call_status',
    )

    cad_trip_history_log = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table='cad_trip_history_log',
    )

    cad_trip_history_log.apply_hints(
        incremental=dlt.sources.incremental(
            'timestamp',
            initial_value=datetime.datetime(2025,6,1,0,0,0)
        ),primary_key="id"
    )

    info = pipeline.run([cad_trip_legs_rev,
                         cad_trip_legs,
                         cad_trips,
                         qa_status,
                         epcr_cad_legs,
                         cad_trip_leg_shift_assignments,
                         patients,
                         cancel_reasons,
                         lost_call_reasons,
                         cad_trip_history_log
                         ], write_disposition="replace")
    logger.info(info)

if __name__ == "__main__":
    load_cad_trips("traumasoft_tn","tn_database")
    load_cad_trips("traumasoft_mi","mi_database")
    load_cad_trips("traumasoft_il","il_database")