"""
Schedule Report Flows - Pay Period Schedule and Comparison Reports

Generates Excel reports for pay period schedules and comparisons:
- Schedule report: Generated day before pay period starts
- Comparison report: Generated day after pay period ends

Reports are emailed to configured recipients and stored in the database.

Scheduling:
- IL: Biweekly Saturday (different pay period cycle)
- MI/Memphis/Nashville: Biweekly Saturday (same pay period cycle)

One-off usage:
    from flows.schedule_reports import run_schedule_report, run_comparison_report
    run_schedule_report(region="tn_memphis", date="2026-03-01")
"""

import datetime
from io import BytesIO
from pathlib import Path

import psycopg2
from openpyxl import Workbook
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.logging import get_run_logger

from flows.email import get_email_recipients, send_report_email
from flows.generate_pay_period_schedule import (
    create_summary_sheet,
    create_daily_sheet,
    create_comparison_summary_sheet,
    create_comparison_daily_sheet,
)

# Database connection - loaded from Prefect secret
DB_CONFIG_SECRET_NAME = "warehouse-db-config"

# Region configurations
REGIONS = ["il", "mi", "tn_memphis", "tn_nashville"]

REGION_TO_SOURCE = {
    "il": "il",
    "mi": "mi",
    "tn_memphis": "tn",
    "tn_nashville": "tn",
}

REGION_DISPLAY_NAMES = {
    "il": "Illinois",
    "mi": "Michigan",
    "tn_memphis": "Memphis",
    "tn_nashville": "Nashville",
}


# =============================================================================
# Database Tasks
# =============================================================================

@task
def get_db_config() -> dict:
    """Load database configuration from Prefect secret."""
    logger = get_run_logger()
    secret = Secret.load(DB_CONFIG_SECRET_NAME)
    config = secret.get()
    logger.info("Loaded database configuration")
    return config


@task
def fetch_pay_period_info(db_config: dict, region: str, target_date: datetime.date) -> dict | None:
    """Fetch pay period information for the given date."""
    logger = get_run_logger()
    source_db = REGION_TO_SOURCE.get(region, region)

    query = """
    SELECT pay_period_number, start_date, end_date
    FROM staging.stg_pay_periods
    WHERE source_database = %s
      AND start_date <= %s
      AND end_date >= %s
    LIMIT 1
    """

    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    cur.execute(query, (source_db, target_date, target_date))
    result = cur.fetchone()
    cur.close()
    conn.close()

    if not result:
        logger.warning(f"No pay period found for {region} containing {target_date}")
        return None

    pp_info = {
        "pay_period_number": result[0],
        "start_date": result[1],
        "end_date": result[2],
        "source_database": source_db,
        "region": region,
    }
    logger.info(f"Pay period {pp_info['pay_period_number']}: {pp_info['start_date']} to {pp_info['end_date']}")
    return pp_info


@task
def fetch_shift_data(db_config: dict, region: str, pp_info: dict) -> list[dict]:
    """Fetch shift data for the pay period."""
    logger = get_run_logger()

    query = """
    SELECT
        s.assignment_id,
        s.user_id,
        s.assigned_name,
        s.shift_date,
        s.day_name,
        s.shift_start,
        s.shift_end,
        CASE
            WHEN s.scheduled_hours > 0 THEN s.scheduled_hours
            WHEN s.shift_start IS NOT NULL AND s.shift_end IS NOT NULL
                THEN EXTRACT(EPOCH FROM (s.shift_end - s.shift_start)) / 3600.0
            ELSE 0
        END AS scheduled_hours,
        s.actual_hours_worked,
        s.unit_name,
        s.cost_center_name,
        s.level_of_service,
        s.is_open,
        s.is_assigned,
        s.is_training,
        s.is_special_event,
        s.is_orientation,
        s.is_field_shift,
        s.region,
        s.source_database,
        EXTRACT(DOW FROM s.shift_date) AS day_of_week,
        CASE
            WHEN s.shift_date < %s + 7 THEN 1
            ELSE 2
        END AS pp_week
    FROM bigquery.bq_shifts s
    WHERE s.source_database = %s
      AND s.region = %s
      AND s.shift_date >= %s
      AND s.shift_date <= %s
      AND (s.is_field_shift = true OR s.is_orientation = true OR s.is_special_event = true)
      AND NOT (s.is_open = true AND s.is_training = true)
      AND s.unit_name IS NOT NULL
    ORDER BY s.shift_date, s.shift_start, s.unit_name, s.assigned_name
    """

    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    cur.execute(query, (
        pp_info["start_date"],
        pp_info["source_database"],
        region,
        pp_info["start_date"],
        pp_info["end_date"],
    ))
    columns = [desc[0] for desc in cur.description]
    data = cur.fetchall()
    cur.close()
    conn.close()

    shifts = [dict(zip(columns, row)) for row in data]
    logger.info(f"Fetched {len(shifts)} shifts for {region}")
    return shifts


@task
def save_report_to_database(
    db_config: dict,
    excel_bytes: bytes,
    region: str,
    pp_info: dict,
    report_type: str,
) -> None:
    """Save report to the database."""
    logger = get_run_logger()

    create_table = """
    CREATE TABLE IF NOT EXISTS analytics.schedule_reports (
        id SERIAL PRIMARY KEY,
        region VARCHAR(50) NOT NULL,
        pay_period_number INTEGER NOT NULL,
        pay_period_start DATE NOT NULL,
        pay_period_end DATE NOT NULL,
        report_type VARCHAR(20) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        excel_data BYTEA NOT NULL,
        UNIQUE(region, pay_period_number, report_type)
    )
    """

    upsert = """
    INSERT INTO analytics.schedule_reports
        (region, pay_period_number, pay_period_start, pay_period_end, report_type, excel_data)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (region, pay_period_number, report_type)
    DO UPDATE SET
        excel_data = EXCLUDED.excel_data,
        created_at = CURRENT_TIMESTAMP
    """

    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    cur.execute(create_table)
    cur.execute(upsert, (
        region,
        pp_info["pay_period_number"],
        pp_info["start_date"],
        pp_info["end_date"],
        report_type,
        psycopg2.Binary(excel_bytes),
    ))
    conn.commit()
    cur.close()
    conn.close()

    logger.info(f"Saved {report_type} report to database: {region} PP{pp_info['pay_period_number']}")


# =============================================================================
# Workbook Building Tasks
# =============================================================================

@task
def build_schedule_workbook(shifts: list[dict], pp_info: dict) -> Workbook:
    """Build the schedule report workbook."""
    from collections import defaultdict

    logger = get_run_logger()

    wb = Workbook()
    create_summary_sheet(
        wb, shifts,
        pp_info["pay_period_number"],
        pp_info["start_date"],
        pp_info["end_date"],
    )

    shifts_by_date = defaultdict(list)
    for shift in shifts:
        shifts_by_date[shift["shift_date"]].append(shift)

    for date in sorted(shifts_by_date.keys()):
        create_daily_sheet(wb, date, shifts_by_date[date])

    logger.info(f"Built schedule workbook with {len(shifts_by_date) + 1} sheets")
    return wb


@task
def build_comparison_workbook(shifts: list[dict], pp_info: dict) -> Workbook:
    """Build the comparison report workbook."""
    from collections import defaultdict

    logger = get_run_logger()

    wb = Workbook()
    create_comparison_summary_sheet(
        wb, shifts,
        pp_info["pay_period_number"],
        pp_info["start_date"],
        pp_info["end_date"],
    )

    shifts_by_date = defaultdict(list)
    for shift in shifts:
        shifts_by_date[shift["shift_date"]].append(shift)

    for date in sorted(shifts_by_date.keys()):
        create_comparison_daily_sheet(wb, date, shifts_by_date[date])

    logger.info(f"Built comparison workbook with {len(shifts_by_date) + 1} sheets")
    return wb


@task
def workbook_to_bytes(workbook: Workbook) -> bytes:
    """Convert workbook to bytes."""
    buffer = BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


@task
def save_workbook_to_file(excel_bytes: bytes, filename: str) -> Path:
    """Save workbook bytes to a file."""
    logger = get_run_logger()
    filepath = Path(filename)
    filepath.write_bytes(excel_bytes)
    logger.info(f"Saved report to {filepath}")
    return filepath


# =============================================================================
# Report Generation Flows
# =============================================================================

@flow
def generate_schedule_report(
    region: str,
    target_date: datetime.date | None = None,
    save_to_db: bool = True,
    email_variable: str | None = None,
) -> Path | None:
    """
    Generate schedule report for a region.

    Args:
        region: Region code (il, mi, tn_memphis, tn_nashville)
        target_date: Date within pay period (defaults to tomorrow)
        save_to_db: Whether to save report to database
        email_variable: Prefect variable name containing recipient list
    """
    logger = get_run_logger()
    logger.info(f"Generating schedule report for {region}")

    if target_date is None:
        target_date = datetime.date.today() + datetime.timedelta(days=1)

    db_config = get_db_config()
    pp_info = fetch_pay_period_info(db_config, region, target_date)

    if not pp_info:
        logger.error(f"No pay period found for {region}")
        return None

    shifts = fetch_shift_data(db_config, region, pp_info)

    if not shifts:
        logger.warning(f"No shift data found for {region}")
        return None

    workbook = build_schedule_workbook(shifts, pp_info)
    excel_bytes = workbook_to_bytes(workbook)

    # Generate filename
    display_name = REGION_DISPLAY_NAMES.get(region, region)
    start_str = pp_info["start_date"].strftime("%m%d")
    end_str = pp_info["end_date"].strftime("%m%d")
    filename = f"Schedule_{display_name}_PP{pp_info['pay_period_number']}_{start_str}-{end_str}.xlsx"

    filepath = save_workbook_to_file(excel_bytes, filename)

    if save_to_db:
        save_report_to_database(db_config, excel_bytes, region, pp_info, "schedule")

    if email_variable:
        recipients = get_email_recipients(email_variable)
        if recipients:
            subject = f"Schedule Report - {display_name} - PP{pp_info['pay_period_number']} ({start_str}-{end_str})"
            body = f"""
Pay Period Schedule Report

Market: {display_name}
Pay Period: {pp_info['pay_period_number']}
Dates: {pp_info['start_date'].strftime('%B %d')} - {pp_info['end_date'].strftime('%B %d, %Y')}

This report contains the scheduled shifts for the upcoming pay period.
"""
            send_report_email(filepath, recipients, subject, body)

    return filepath


@flow
def generate_comparison_report(
    region: str,
    target_date: datetime.date | None = None,
    save_to_db: bool = True,
    email_variable: str | None = None,
) -> Path | None:
    """
    Generate comparison report for a region.

    Args:
        region: Region code (il, mi, tn_memphis, tn_nashville)
        target_date: Date within pay period (defaults to yesterday)
        save_to_db: Whether to save report to database
        email_variable: Prefect variable name containing recipient list
    """
    logger = get_run_logger()
    logger.info(f"Generating comparison report for {region}")

    if target_date is None:
        target_date = datetime.date.today() - datetime.timedelta(days=1)

    db_config = get_db_config()
    pp_info = fetch_pay_period_info(db_config, region, target_date)

    if not pp_info:
        logger.error(f"No pay period found for {region}")
        return None

    shifts = fetch_shift_data(db_config, region, pp_info)

    if not shifts:
        logger.warning(f"No shift data found for {region}")
        return None

    workbook = build_comparison_workbook(shifts, pp_info)
    excel_bytes = workbook_to_bytes(workbook)

    # Generate filename
    display_name = REGION_DISPLAY_NAMES.get(region, region)
    start_str = pp_info["start_date"].strftime("%m%d")
    end_str = pp_info["end_date"].strftime("%m%d")
    filename = f"Comparison_{display_name}_PP{pp_info['pay_period_number']}_{start_str}-{end_str}.xlsx"

    filepath = save_workbook_to_file(excel_bytes, filename)

    if save_to_db:
        save_report_to_database(db_config, excel_bytes, region, pp_info, "comparison")

    if email_variable:
        recipients = get_email_recipients(email_variable)
        if recipients:
            subject = f"Comparison Report - {display_name} - PP{pp_info['pay_period_number']} ({start_str}-{end_str})"
            body = f"""
Pay Period Comparison Report

Market: {display_name}
Pay Period: {pp_info['pay_period_number']}
Dates: {pp_info['start_date'].strftime('%B %d')} - {pp_info['end_date'].strftime('%B %d, %Y')}

This report compares scheduled hours vs actual hours worked for the completed pay period.
Variance shows the difference (Actual - Scheduled).
"""
            send_report_email(filepath, recipients, subject, body)

    return filepath


# =============================================================================
# Batch Flows - All Regions
# =============================================================================

@flow
def generate_all_schedule_reports(
    email_variable: str = "schedule_report_recipients",
    regions: list[str] | None = None,
) -> dict[str, Path | None]:
    """Generate schedule reports for all (or specified) regions."""
    logger = get_run_logger()

    if regions is None:
        regions = REGIONS

    results = {}
    for region in regions:
        logger.info(f"Processing {region}...")
        results[region] = generate_schedule_report(
            region=region,
            save_to_db=True,
            email_variable=email_variable,
        )

    return results


@flow
def generate_all_comparison_reports(
    email_variable: str = "schedule_report_recipients",
    regions: list[str] | None = None,
) -> dict[str, Path | None]:
    """Generate comparison reports for all (or specified) regions."""
    logger = get_run_logger()

    if regions is None:
        regions = REGIONS

    results = {}
    for region in regions:
        logger.info(f"Processing {region}...")
        results[region] = generate_comparison_report(
            region=region,
            save_to_db=True,
            email_variable=email_variable,
        )

    return results


# =============================================================================
# Scheduled Flows - Biweekly
# =============================================================================

@flow
def scheduled_il_schedule_report(email_variable: str = "schedule_report_recipients"):
    """
    IL schedule report - runs biweekly Saturday before pay period starts.

    Deploy with:
        flow.serve(cron="0 8 * * 6")  # Every Saturday at 8am
        # Filter in flow logic based on IL pay period dates
    """
    logger = get_run_logger()

    # Check if tomorrow is start of IL pay period
    tomorrow = datetime.date.today() + datetime.timedelta(days=1)
    db_config = get_db_config()
    pp_info = fetch_pay_period_info(db_config, "il", tomorrow)

    if pp_info and pp_info["start_date"] == tomorrow:
        logger.info("Tomorrow is IL pay period start - generating schedule report")
        generate_schedule_report(region="il", email_variable=email_variable)
    else:
        logger.info("Not IL pay period start - skipping")


@flow
def scheduled_il_comparison_report(email_variable: str = "schedule_report_recipients"):
    """
    IL comparison report - runs biweekly Saturday after pay period ends.

    Deploy with:
        flow.serve(cron="0 8 * * 6")  # Every Saturday at 8am
    """
    logger = get_run_logger()

    # Check if yesterday was end of IL pay period
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    db_config = get_db_config()
    pp_info = fetch_pay_period_info(db_config, "il", yesterday)

    if pp_info and pp_info["end_date"] == yesterday:
        logger.info("Yesterday was IL pay period end - generating comparison report")
        generate_comparison_report(region="il", email_variable=email_variable)
    else:
        logger.info("Not IL pay period end - skipping")


@flow
def scheduled_other_schedule_reports(email_variable: str = "schedule_report_recipients"):
    """
    MI/Memphis/Nashville schedule reports - runs biweekly Saturday before pay period starts.

    These regions share the same pay period cycle.
    """
    logger = get_run_logger()

    tomorrow = datetime.date.today() + datetime.timedelta(days=1)
    db_config = get_db_config()

    # Check MI (representative for MI/TN pay period cycle)
    pp_info = fetch_pay_period_info(db_config, "mi", tomorrow)

    if pp_info and pp_info["start_date"] == tomorrow:
        logger.info("Tomorrow is MI/TN pay period start - generating schedule reports")
        for region in ["mi", "tn_memphis", "tn_nashville"]:
            generate_schedule_report(region=region, email_variable=email_variable)
    else:
        logger.info("Not MI/TN pay period start - skipping")


@flow
def scheduled_other_comparison_reports(email_variable: str = "schedule_report_recipients"):
    """
    MI/Memphis/Nashville comparison reports - runs biweekly Saturday after pay period ends.

    These regions share the same pay period cycle.
    """
    logger = get_run_logger()

    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    db_config = get_db_config()

    # Check MI (representative for MI/TN pay period cycle)
    pp_info = fetch_pay_period_info(db_config, "mi", yesterday)

    if pp_info and pp_info["end_date"] == yesterday:
        logger.info("Yesterday was MI/TN pay period end - generating comparison reports")
        for region in ["mi", "tn_memphis", "tn_nashville"]:
            generate_comparison_report(region=region, email_variable=email_variable)
    else:
        logger.info("Not MI/TN pay period end - skipping")


@flow
def saturday_schedule_check():
    """
    Master Saturday flow - checks all pay periods and generates appropriate reports.

    This single flow can be scheduled every Saturday and will automatically
    determine which reports need to be generated based on pay period dates.

    Deploy with:
        saturday_schedule_check.serve(cron="0 8 * * 6")
    """
    logger = get_run_logger()
    logger.info("Running Saturday schedule check")

    # Run IL checks
    scheduled_il_schedule_report()
    scheduled_il_comparison_report()

    # Run MI/TN checks
    scheduled_other_schedule_reports()
    scheduled_other_comparison_reports()


# =============================================================================
# One-Off Flows - Manual/Ad-hoc Report Generation
# =============================================================================

def parse_date(date_str: str) -> datetime.date:
    """Parse date string in YYYY-MM-DD format."""
    return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()


@flow
def run_schedule_report(
    region: str,
    date: str,
    email_variable: str | None = None,
    save_to_db: bool = True,
) -> Path | None:
    """
    One-off flow to generate a schedule report for a specific pay period.

    Args:
        region: Market region (il, mi, tn_memphis, tn_nashville)
        date: Any date within the pay period (YYYY-MM-DD format)
        email_variable: Optional Prefect variable name for email recipients
        save_to_db: Whether to save report to database (default True)

    Example:
        prefect run flows/schedule_reports.py:run_schedule_report \
            --region tn_memphis --date 2026-03-01

        # Or from Python:
        from flows.schedule_reports import run_schedule_report
        run_schedule_report(region="tn_memphis", date="2026-03-01")
    """
    logger = get_run_logger()

    if region not in REGIONS:
        logger.error(f"Invalid region '{region}'. Must be one of: {REGIONS}")
        return None

    target_date = parse_date(date)
    logger.info(f"Running one-off schedule report for {region}, date={target_date}")

    return generate_schedule_report(
        region=region,
        target_date=target_date,
        save_to_db=save_to_db,
        email_variable=email_variable,
    )


@flow
def run_comparison_report(
    region: str,
    date: str,
    email_variable: str | None = None,
    save_to_db: bool = True,
) -> Path | None:
    """
    One-off flow to generate a comparison report for a specific pay period.

    Args:
        region: Market region (il, mi, tn_memphis, tn_nashville)
        date: Any date within the pay period (YYYY-MM-DD format)
        email_variable: Optional Prefect variable name for email recipients
        save_to_db: Whether to save report to database (default True)

    Example:
        prefect run flows/schedule_reports.py:run_comparison_report \
            --region tn_memphis --date 2026-03-01

        # Or from Python:
        from flows.schedule_reports import run_comparison_report
        run_comparison_report(region="tn_memphis", date="2026-03-01")
    """
    logger = get_run_logger()

    if region not in REGIONS:
        logger.error(f"Invalid region '{region}'. Must be one of: {REGIONS}")
        return None

    target_date = parse_date(date)
    logger.info(f"Running one-off comparison report for {region}, date={target_date}")

    return generate_comparison_report(
        region=region,
        target_date=target_date,
        save_to_db=save_to_db,
        email_variable=email_variable,
    )


@flow
def run_both_reports(
    region: str,
    date: str,
    email_variable: str | None = None,
    save_to_db: bool = True,
) -> dict[str, Path | None]:
    """
    One-off flow to generate both schedule and comparison reports for a pay period.

    Args:
        region: Market region (il, mi, tn_memphis, tn_nashville)
        date: Any date within the pay period (YYYY-MM-DD format)
        email_variable: Optional Prefect variable name for email recipients
        save_to_db: Whether to save report to database (default True)

    Returns:
        Dict with 'schedule' and 'comparison' keys containing file paths.
    """
    logger = get_run_logger()

    if region not in REGIONS:
        logger.error(f"Invalid region '{region}'. Must be one of: {REGIONS}")
        return {"schedule": None, "comparison": None}

    target_date = parse_date(date)
    logger.info(f"Running both reports for {region}, date={target_date}")

    return {
        "schedule": generate_schedule_report(
            region=region,
            target_date=target_date,
            save_to_db=save_to_db,
            email_variable=email_variable,
        ),
        "comparison": generate_comparison_report(
            region=region,
            target_date=target_date,
            save_to_db=save_to_db,
            email_variable=email_variable,
        ),
    }


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Usage: python schedule_reports.py <region> <date> [schedule|comparison|both]")
        print("Regions: il, mi, tn_memphis, tn_nashville")
        print("Date format: YYYY-MM-DD")
        print()
        print("Examples:")
        print("  python -m flows.schedule_reports tn_memphis 2026-03-01")
        print("  python -m flows.schedule_reports tn_memphis 2026-03-01 comparison")
        print("  python -m flows.schedule_reports tn_memphis 2026-03-01 both")
        sys.exit(1)

    region = sys.argv[1]
    date = sys.argv[2]
    report_type = sys.argv[3] if len(sys.argv) > 3 else "schedule"

    if report_type == "comparison":
        run_comparison_report(region=region, date=date, save_to_db=False)
    elif report_type == "both":
        run_both_reports(region=region, date=date, save_to_db=False)
    else:
        run_schedule_report(region=region, date=date, save_to_db=False)