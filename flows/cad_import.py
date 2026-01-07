"""
CAD Import Flow - Tiered Reconciliation

This module implements a tiered data reconciliation strategy:
1. Recent (every 10 min): Last 24 hours to 7 days ahead
2. Weekly (Sunday 0200): Last full week (Sun-Sat) to 7 days ahead + reference tables
3. Monthly (1st of month 0200): Last month, processed week by week

Tables with date filtering:
- cad_trip_legs_rev (modified) - uses composite PK (leg_id, rev)
- cad_trip_legs (created)
- cad_trips (trip_date)
- cad_trip_history_log (timestamp)
"""

import datetime
import time
from typing import Callable

import dlt
from dlt.sources.sql_database import sql_table
from prefect import flow
from prefect.logging import get_run_logger


# Date column mapping for filtered tables
# Recent/weekly use 'modified' to catch updates; monthly uses 'leg_date' for full period coverage
DATE_COLUMNS_RECENT = {
    "cad_trip_legs_rev": "modified",
    "cad_trip_legs": "created",
    "cad_trips": "trip_date",
    "cad_trip_history_log": "timestamp",
}

DATE_COLUMNS_MONTHLY = {
    "cad_trip_legs_rev": "leg_date",
    "cad_trip_legs": "created",
    "cad_trips": "trip_date",
    "cad_trip_history_log": "timestamp",
}


def create_date_filter(
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    use_leg_date: bool = False,
) -> Callable:
    """Create a query adapter callback that filters tables by their date columns.

    Args:
        start_date: Start of date range (inclusive)
        end_date: End of date range (exclusive)
        use_leg_date: If True, use leg_date for cad_trip_legs_rev (for monthly).
                      If False, use modified (for recent/weekly).
    """
    date_columns = DATE_COLUMNS_MONTHLY if use_leg_date else DATE_COLUMNS_RECENT

    def filter_by_date(query, table):
        date_col = date_columns.get(table.name)
        if date_col:
            return query.where(
                (table.c[date_col] >= start_date) &
                (table.c[date_col] < end_date)
            )
        return query
    return filter_by_date


def get_date_filtered_tables(source_name: str, date_filter: Callable) -> list:
    """Create the four date-filtered tables with the given filter."""
    cad_trip_legs_rev = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="cad_trip_legs_rev",
        query_adapter_callback=date_filter,
    ).apply_hints(primary_key=["leg_id", "rev"])

    cad_trip_legs = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="cad_trip_legs",
        query_adapter_callback=date_filter,
    ).apply_hints(primary_key="id")

    cad_trips = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="cad_trips",
        query_adapter_callback=date_filter,
    ).apply_hints(primary_key="id")

    cad_trip_history_log = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="cad_trip_history_log",
        query_adapter_callback=date_filter,
    ).apply_hints(primary_key="id")

    return [cad_trip_legs_rev, cad_trip_legs, cad_trips, cad_trip_history_log]


def get_reference_tables(source_name: str) -> list:
    """Create the reference/lookup tables (no date filtering)."""
    epcr_cad_legs = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="epcr_v2_cad_legs"
    )

    cad_trip_leg_shift_assignments = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="cad_trip_leg_shift_assignments"
    )

    cancel_reasons = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="cad_trip_cancel_reason",
    )

    lost_call_reasons = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="cad_lost_call_status",
    )

    return [epcr_cad_legs, cad_trip_leg_shift_assignments, cancel_reasons, lost_call_reasons]


@flow
def load_cad_recent(
    dataset_name: str,
    source_name: str,
) -> None:
    """
    Load CAD trip data from the last 24 hours to 7 days ahead.

    Intended to run every 10 minutes for near-real-time reconciliation.
    """
    logger = get_run_logger()

    now = datetime.datetime.now()
    start_date = now - datetime.timedelta(hours=24)
    end_date = now + datetime.timedelta(days=7)

    logger.info(f"Loading recent CAD data: {start_date} to {end_date}")

    pipeline = dlt.pipeline(
        pipeline_name=f"cad_recent_{dataset_name}_{int(time.time())}",
        destination='postgres',
        dataset_name=dataset_name,
    )

    date_filter = create_date_filter(start_date, end_date)
    tables = get_date_filtered_tables(source_name, date_filter)

    info = pipeline.run(tables, write_disposition="merge")
    logger.info(f"Recent load complete: {info}")


@flow
def load_cad_weekly(
    dataset_name: str,
    source_name: str,
) -> None:
    """
    Load CAD trip data for the last full week (Sunday through Saturday) to 7 days ahead.
    Also refreshes all reference/lookup tables.

    Intended to run Sunday at 0200.
    """
    logger = get_run_logger()

    today = datetime.date.today()
    # Find last Saturday (end of previous week)
    days_since_saturday = (today.weekday() + 2) % 7
    if days_since_saturday == 0:
        days_since_saturday = 7  # If today is Saturday, go back a full week
    last_saturday = today - datetime.timedelta(days=days_since_saturday)
    # Last Sunday is 6 days before last Saturday
    last_sunday = last_saturday - datetime.timedelta(days=6)
    # Include a week into the future for scheduled trips
    future_end = today + datetime.timedelta(days=7)

    start_date = datetime.datetime.combine(last_sunday, datetime.time.min)
    end_date = datetime.datetime.combine(future_end, datetime.time.max)

    logger.info(f"Loading weekly CAD data: {start_date} to {end_date}")

    # Load date-filtered tables
    pipeline = dlt.pipeline(
        pipeline_name=f"cad_weekly_{dataset_name}_{int(time.time())}",
        destination='postgres',
        dataset_name=dataset_name,
    )

    date_filter = create_date_filter(start_date, end_date)
    date_filtered_tables = get_date_filtered_tables(source_name, date_filter)

    info = pipeline.run(date_filtered_tables, write_disposition="merge")
    logger.info(f"Weekly date-filtered load complete: {info}")

    # Load reference tables (full replace)
    ref_pipeline = dlt.pipeline(
        pipeline_name=f"cad_weekly_ref_{dataset_name}_{int(time.time())}",
        destination='postgres',
        dataset_name=dataset_name,
    )

    reference_tables = get_reference_tables(source_name)

    ref_info = ref_pipeline.run(reference_tables, write_disposition="replace")
    logger.info(f"Weekly reference tables load complete: {ref_info}")


@flow
def load_cad_monthly(
    dataset_name: str,
    source_name: str,
) -> None:
    """
    Load CAD trip data for the last full month, processed week by week.

    Intended to run on the 1st of each month at 0200.
    """
    logger = get_run_logger()

    today = datetime.date.today()
    # First day of current month
    first_of_current_month = today.replace(day=1)
    # Last day of previous month
    last_of_prev_month = first_of_current_month - datetime.timedelta(days=1)
    # First day of previous month
    first_of_prev_month = last_of_prev_month.replace(day=1)

    logger.info(f"Loading monthly CAD data for: {first_of_prev_month} to {last_of_prev_month}")

    # Process week by week
    current_start = first_of_prev_month
    week_num = 1

    while current_start <= last_of_prev_month:
        # End of this week segment (7 days later or end of month)
        week_end = min(current_start + datetime.timedelta(days=6), last_of_prev_month)

        start_datetime = datetime.datetime.combine(current_start, datetime.time.min)
        end_datetime = datetime.datetime.combine(week_end, datetime.time.max)

        logger.info(f"Processing week {week_num}: {current_start} to {week_end}")

        pipeline = dlt.pipeline(
            pipeline_name=f"cad_monthly_w{week_num}_{dataset_name}_{int(time.time())}",
            destination='postgres',
            dataset_name=dataset_name,
        )

        # Use leg_date for monthly reconciliation to capture all runs in the period
        date_filter = create_date_filter(start_datetime, end_datetime, use_leg_date=True)
        tables = get_date_filtered_tables(source_name, date_filter)

        info = pipeline.run(tables, write_disposition="merge")
        logger.info(f"Week {week_num} complete: {info}")

        # Move to next week
        current_start = week_end + datetime.timedelta(days=1)
        week_num += 1

    logger.info(f"Monthly load complete - processed {week_num - 1} weeks")


@flow
def load_all_cad_recent() -> None:
    """Load recent CAD data for all regions."""
    load_cad_recent("traumasoft_tn", "tn_database")
    load_cad_recent("traumasoft_mi", "mi_database")
    load_cad_recent("traumasoft_il", "il_database")


@flow
def load_all_cad_weekly() -> None:
    """Load weekly CAD data for all regions."""
    load_cad_weekly("traumasoft_tn", "tn_database")
    load_cad_weekly("traumasoft_mi", "mi_database")
    load_cad_weekly("traumasoft_il", "il_database")


@flow
def load_all_cad_monthly() -> None:
    """Load monthly CAD data for all regions."""
    load_cad_monthly("traumasoft_tn", "tn_database")
    load_cad_monthly("traumasoft_mi", "mi_database")
    load_cad_monthly("traumasoft_il", "il_database")


if __name__ == "__main__":
    # For testing, run the recent load
    load_all_cad_recent()