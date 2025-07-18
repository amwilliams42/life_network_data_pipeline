from prefect import flow
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings
from prefect_dbt.cli.commands import trigger_dbt_cli_command


@flow
def run_dbt():

    trigger_dbt_cli_command(
        command="dbt deps", project_dir="./lan_dbt/",
    )

    PrefectDbtRunner(
        settings=PrefectDbtSettings(
            project_dir="lan_dbt",
            profiles_dir="lan_dbt"
        )
    ).invoke(["build"])



if __name__ == "__main__":
    run_dbt()