import os
from pathlib import Path
from prefect import flow, get_run_logger
from prefect_shell import ShellOperation
from prefect.blocks.system import Secret

# Extract constants for better maintainability
DBT_COMMANDS = ["dbt deps", "dbt build --target prod"]
DEFAULT_DBT_DIR = "./lan_dbt"

@flow(name="dbt-flow", log_prints=True)
def run_dbt():
    logger = get_run_logger()

    db_user = Secret.load("warehouse-user")
    db_password = Secret.load("warehouse-password")
    
    # Validate directory exists and contains dbt_project.yml
    dbt_path = Path(DEFAULT_DBT_DIR).resolve()
    if not dbt_path.exists():
        logger.error(f"dbt directory does not exist: {dbt_path}")
    
    logger.info(f"Running dbt commands in directory: {dbt_path}")
    
    # Run commands sequentially with individual error handling
    for command in DBT_COMMANDS:
        logger.info(f"Executing: {command}")
        try:
            shell_operation = ShellOperation(
                commands=[command],
                working_dir=str(dbt_path),
                env={"DBT_PROFILES_DIR": str(dbt_path),
                     "WAREHOUSE_USER": db_user.get(),
                     "WAREHOUSE_PASS": db_password.get()}
            )
            result = shell_operation.run()
            
            # Handle different return types from ShellOperation
            if hasattr(result, 'stdout') and result.stdout:
                for line in result.stdout.split('\n'):
                    if line.strip():
                        logger.info(line)
            elif isinstance(result, (list, tuple)):
                for line in result:
                    if line and str(line).strip():
                        logger.info(str(line))
            else:
                logger.info(f"Command completed: {command}")
                
        except Exception as e:
            logger.error(f"Command '{command}' failed: {str(e)}")
            raise Exception(f"dbt flow failed at command '{command}': {str(e)}")
    
    logger.info("dbt flow completed successfully")

if __name__ == "__main__":
    run_dbt()