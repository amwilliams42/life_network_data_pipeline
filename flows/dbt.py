import os
from pathlib import Path

from prefect import flow, task, get_run_logger
from prefect_dbt import PrefectDbtSettings, PrefectDbtRunner


@task
def run_dbt_commands(commands: list[str], project_dir: Path):

    logger = get_run_logger()
    logger.info(f"Running dbt commands: {commands}")

    settings = PrefectDbtSettings (
        project_dir=project_dir,
        profiles_dir=project_dir,
    )

    runner = PrefectDbtRunner(settings=settings, raise_on_failure=False)

    for command in commands:
        logger.info(f"Running dbt command: {command}")
        runner.invoke(command.split())
        logger.info(f"Finished running dbt command: {command}")

@flow(name="dbt-flow", log_prints=True)
def trigger_dbt_cli_command():

    project_dir = Path(__file__).resolve().parent / "lan_dbt"

    run_dbt_commands(["deps"], project_dir)


if __name__ == "__main__":
    trigger_dbt_cli_command()