import os
from prefect import flow, get_run_logger
from prefect_shell import ShellOperation

# Extract constants for better maintainability
DBT_COMMANDS = ["dbt deps", "dbt build"]
DEFAULT_DBT_DIR = "./lan_dbt"


@flow(name="dbt-flow", log_prints=True)
def run_dbt(dbt_directory: str = None):
    logger = get_run_logger()

    working_dir = dbt_directory or DEFAULT_DBT_DIR

    logger.info(f"Running dbt commands in directory: {working_dir}")

    shell_operation = ShellOperation(
        commands=DBT_COMMANDS,
        working_dir=working_dir
    )

    try:
        output = shell_operation.run()
        for line in output:
            logger.info(line)
        logger.info("dbt flow completed successfully")
    except Exception as e:
        logger.error(f"dbt flow failed: {str(e)}")
        raise


if __name__ == "__main__":
    run_dbt()
