import os

from prefect import flow
from prefect.logging import get_run_logger
from prefect_dbt.cli.commands import trigger_dbt_cli_command


@flow
def run_dbt():
    logger = get_run_logger()
    logger.info(os.system("uv pip list"))
    trigger_dbt_cli_command(
        command="dbt deps", project_dir="./lan_dbt/",
    )



if __name__ == "__main__":
    run_dbt()