"""
Schedule Flows - Tiered Reconciliation with Weekly Snapshots

This module implements a tiered data reconciliation strategy for schedule data:
1. Recent (every 10 min): Last 24 hours to 7 days ahead
2. Weekly (Sunday 0200): Last full week (Sun-Sat) to 7 days ahead
3. Monthly (1st of month 0200): Last month, processed week by week
4. Snapshot (Saturday 2359): Capture upcoming week's schedule before it starts

Tables:
- sched_template_shift_assignments (date_line)
- timesheet (date_created)
"""

import datetime
import time
from typing import Callable

import dlt
from dlt.sources.sql_database import sql_table
from prefect import flow
from prefect.logging import get_run_logger


# Date column mapping for filtered tables
DATE_COLUMNS = {
    "sched_template_shift_assignments": "date_line",
    "timesheet": "date_created",
}


def create_date_filter(start_date: datetime.datetime, end_date: datetime.datetime) -> Callable:
    """Create a query adapter callback that filters tables by their date columns."""
    def filter_by_date(query, table):
        date_col = DATE_COLUMNS.get(table.name)
        if date_col:
            return query.where(
                (table.c[date_col] >= start_date) &
                (table.c[date_col] < end_date)
            )
        return query
    return filter_by_date


def get_schedule_tables(source_name: str, date_filter: Callable) -> list:
    """Create the schedule tables with the given filter."""
    schedule_table = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="sched_template_shift_assignments",
        query_adapter_callback=date_filter,
    ).apply_hints(primary_key="id")

    timesheet_table = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="timesheet",
        query_adapter_callback=date_filter,
    ).apply_hints(primary_key="time_id")

    return [schedule_table, timesheet_table]


def get_snapshot_table(source_name: str, date_filter: Callable, snapshot_week_start: datetime.date):
    """Create the schedule table for snapshots with snapshot_week_start added."""

    def add_snapshot_column(doc):
        doc["snapshot_week_start"] = snapshot_week_start
        return doc

    schedule_table = sql_table(
        credentials=dlt.secrets[f"sources.{source_name}.credentials"],
        table="sched_template_shift_assignments",
        query_adapter_callback=date_filter,
    ).apply_hints(primary_key=["id", "snapshot_week_start"])

    # Add the snapshot column to each record
    return schedule_table.add_map(add_snapshot_column)


@flow
def load_schedule_recent(
    dataset_name: str,
    source_name: str,
) -> None:
    """
    Load schedule data from the last 24 hours to 7 days ahead.

    Intended to run every 10 minutes for near-real-time reconciliation.
    """
    logger = get_run_logger()

    now = datetime.datetime.now()
    start_date = now - datetime.timedelta(hours=24)
    end_date = now + datetime.timedelta(days=7)

    logger.info(f"Loading recent schedule data: {start_date} to {end_date}")

    pipeline = dlt.pipeline(
        pipeline_name=f"schedule_recent_{dataset_name}_{int(time.time())}",
        destination='postgres',
        dataset_name=dataset_name,
    )

    date_filter = create_date_filter(start_date, end_date)
    tables = get_schedule_tables(source_name, date_filter)

    info = pipeline.run(tables, write_disposition="merge")
    logger.info(f"Recent schedule load complete: {info}")


@flow
def load_schedule_weekly(
    dataset_name: str,
    source_name: str,
) -> None:
    """
    Load schedule data for the last full week (Sunday through Saturday) to 7 days ahead.

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
    # Include a week into the future for scheduled shifts
    future_end = today + datetime.timedelta(days=7)

    start_date = datetime.datetime.combine(last_sunday, datetime.time.min)
    end_date = datetime.datetime.combine(future_end, datetime.time.max)

    logger.info(f"Loading weekly schedule data: {start_date} to {end_date}")

    pipeline = dlt.pipeline(
        pipeline_name=f"schedule_weekly_{dataset_name}_{int(time.time())}",
        destination='postgres',
        dataset_name=dataset_name,
    )

    date_filter = create_date_filter(start_date, end_date)
    tables = get_schedule_tables(source_name, date_filter)

    info = pipeline.run(tables, write_disposition="merge")
    logger.info(f"Weekly schedule load complete: {info}")


@flow
def load_schedule_monthly(
    dataset_name: str,
    source_name: str,
) -> None:
    """
    Load schedule data for the last full month, processed week by week.

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

    logger.info(f"Loading monthly schedule data for: {first_of_prev_month} to {last_of_prev_month}")

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
            pipeline_name=f"schedule_monthly_w{week_num}_{dataset_name}_{int(time.time())}",
            destination='postgres',
            dataset_name=dataset_name,
        )

        date_filter = create_date_filter(start_datetime, end_datetime)
        tables = get_schedule_tables(source_name, date_filter)

        info = pipeline.run(tables, write_disposition="merge")
        logger.info(f"Week {week_num} complete: {info}")

        # Move to next week
        current_start = week_end + datetime.timedelta(days=1)
        week_num += 1

    logger.info(f"Monthly schedule load complete - processed {week_num - 1} weeks")


@flow
def snapshot_schedule_weekly(
    dataset_name: str,
    source_name: str,
) -> None:
    """
    Take a snapshot of the upcoming week's schedule.

    Captures the schedule as it exists before the week starts, for comparison
    with actuals later. Writes to sched_template_shift_assignments_snapshot table.

    Intended to run Saturday at 2359.
    """
    logger = get_run_logger()

    today = datetime.date.today()
    # Calculate days until next Sunday (weekday 6 = Sunday in Python)
    # If today is Saturday (5), next Sunday is 1 day away
    # If today is Sunday (6), next Sunday is 7 days away
    days_until_sunday = (6 - today.weekday()) % 7
    if days_until_sunday == 0:
        days_until_sunday = 7
    next_sunday = today + datetime.timedelta(days=days_until_sunday)
    next_saturday = next_sunday + datetime.timedelta(days=6)

    start_date = datetime.datetime.combine(next_sunday, datetime.time.min)
    end_date = datetime.datetime.combine(next_saturday, datetime.time.max)

    logger.info(f"Taking schedule snapshot for week: {next_sunday} to {next_saturday}")

    pipeline = dlt.pipeline(
        pipeline_name=f"schedule_snapshot_{dataset_name}_{int(time.time())}",
        destination='postgres',
        dataset_name=dataset_name,
    )

    date_filter = create_date_filter(start_date, end_date)
    snapshot_table = get_snapshot_table(source_name, date_filter, next_sunday)

    # Rename the table to the snapshot table name
    snapshot_table.apply_hints(table_name="sched_template_shift_assignments_snapshot")

    info = pipeline.run(snapshot_table, write_disposition="append")
    logger.info(f"Schedule snapshot complete: {info}")


# Convenience flows for all regions
@flow
def load_all_schedule_recent() -> None:
    """Load recent schedule data for all regions."""
    load_schedule_recent("traumasoft_tn", "tn_database")
    load_schedule_recent("traumasoft_mi", "mi_database")
    load_schedule_recent("traumasoft_il", "il_database")


@flow
def load_all_schedule_weekly() -> None:
    """Load weekly schedule data for all regions."""
    load_schedule_weekly("traumasoft_tn", "tn_database")
    load_schedule_weekly("traumasoft_mi", "mi_database")
    load_schedule_weekly("traumasoft_il", "il_database")


@flow
def load_all_schedule_monthly() -> None:
    """Load monthly schedule data for all regions."""
    load_schedule_monthly("traumasoft_tn", "tn_database")
    load_schedule_monthly("traumasoft_mi", "mi_database")
    load_schedule_monthly("traumasoft_il", "il_database")


@flow
def snapshot_all_schedule_weekly() -> None:
    """Take weekly schedule snapshots for all regions."""
    snapshot_schedule_weekly("traumasoft_tn", "tn_database")
    snapshot_schedule_weekly("traumasoft_mi", "mi_database")
    snapshot_schedule_weekly("traumasoft_il", "il_database")


if __name__ == "__main__":
    # For testing, run the recent load
    load_all_schedule_recent()