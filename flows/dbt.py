from prefect import flow
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings


@flow
def run_dbt():
    PrefectDbtRunner(
        settings=PrefectDbtSettings(
            project_dir="lan_dbt",
            profiles_dir="lan_dbt"
        )
    ).invoke(["build"])


if __name__ == "__main__":
    run_dbt()