from prefect import Flow
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from flow_utilities.prefect_configs import set_run_config, set_storage

FLOW_NAME = "05_orchestrating_flow_modular"
PROJECT_NAME = "jaffle_shop"


with Flow(
    FLOW_NAME, storage=set_storage(FLOW_NAME), run_config=set_run_config(),
) as flow:
    extract_load_id = create_flow_run(
        flow_name="01_extract_load",
        project_name=PROJECT_NAME,
        task_args={"name": "Staging"},
    )
    extract_load_wait_task = wait_for_flow_run(
        extract_load_id, raise_final_state=True, task_args={"name": "Staging - wait"}
    )

    transform_id = create_flow_run(
        flow_name="02_dbt_snowflake",
        project_name=PROJECT_NAME,
        task_args={"name": "DBT flow"},
    )
    transform_id_wait_task = wait_for_flow_run(
        transform_id, raise_final_state=True, task_args={"name": "DBT flow - wait"}
    )
    extract_load_wait_task.set_downstream(transform_id)

    dashboards_id = create_flow_run(
        flow_name="03_dashboards",
        project_name=PROJECT_NAME,
        task_args={"name": "Dashboards"},
    )
    dashboards_wait_task = wait_for_flow_run(
        dashboards_id, raise_final_state=True, task_args={"name": "Dashboards - wait"}
    )
    transform_id_wait_task.set_downstream(dashboards_id)
