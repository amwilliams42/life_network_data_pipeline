import datetime
import time

import dlt
from dlt.sources.sql_database import sql_database, sql_table, Table
from prefect import flow, task
from prefect import flow, task
from prefect.logging import get_run_logger


@task
def schedule_refresh(
        dataset_name: str,
        source_name: str
) -> None:

    logger = get_run_logger()
    pipeline = dlt.pipeline(
        pipeline_name=f"sched_template_{dataset_name}",
        destination='postgres',
        dataset_name=dataset_name,
    )

    schedule_table = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="sched_template_shift_assignments",
    )
    schedule_table.apply_hints(primary_key="id")

    timesheet_table = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="timesheet",
    )
    timesheet_table.apply_hints(primary_key="time_id")

    # Add diagnostic logging to check if tables have data
    logger.info("Checking data before pipeline run...")
    
    # Try to get a count of records
    try:
        schedule_data = list(schedule_table)
        timesheet_data = list(timesheet_table)
        
        logger.info(f"Schedule table records found: {len(schedule_data)}")
        logger.info(f"Timesheet table records found: {len(timesheet_data)}")
        
        if len(schedule_data) == 0:
            logger.warning("Schedule table is empty!")
        if len(timesheet_data) == 0:
            logger.warning("Timesheet table is empty!")
            
    except Exception as e:
        logger.error(f"Error checking table data: {e}")

    info = pipeline.run([schedule_table,timesheet_table], write_disposition="replace")
    logger.info(f"Finished loading table {info}")

@flow
def load_cad_trips(
        dataset_name: str,
        source_name: str,
) -> None:
    logger = get_run_logger()
    pipeline = dlt.pipeline(
        pipeline_name=f"load_cad_trips_{dataset_name}_{int(time.time())}",
        destination='postgres',
        dataset_name=dataset_name,
    )
    def filter_by_date(query, table):
        if table.name == "cad_trip_legs_rev":
            # Only select rows where the column customer_id has value 1
            return query.where(table.c.leg_date > '2025-08-01')
        # Use the original query for other tables
        if table.name == "cad_trip_legs":
            return query.where(table.c.created > '2025-08-01')
        if table.name == "cad_trips":
            return query.where(table.c.created > '2025-08-01')
        if table.name == "cad_trip_history_log":
            return query.where(table.c.timestamp > '2025-08-01')
        return query


    cad_trip_legs_rev = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="cad_trip_legs_rev",
        query_adapter_callback = filter_by_date,
    ).apply_hints(primary_key="leg_id")

    cad_trip_legs = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="cad_trip_legs",
    ).apply_hints(primary_key="id")

    cad_trips = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="cad_trips",
    ).apply_hints(primary_key="id")


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
    ).apply_hints(primary_key="id")

    info = pipeline.run([cad_trip_legs_rev,
                         cad_trip_legs,
                         cad_trips,
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