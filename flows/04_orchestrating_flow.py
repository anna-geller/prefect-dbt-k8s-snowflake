from prefect import Flow
from prefect.tasks.prefect import StartFlowRun
from flow_utilities.prefect_configs import set_run_config, set_storage

FLOW_NAME = "04_orchestrating_flow"
PROJECT_NAME = "jaffle_shop"
start_flow_run = StartFlowRun(project_name=PROJECT_NAME, wait=True)


with Flow(
    FLOW_NAME, storage=set_storage(FLOW_NAME), run_config=set_run_config(),
) as flow:
    staging = start_flow_run(flow_name="01_extract_load", task_args={"name": "Stage"})
    dbt_run = start_flow_run(flow_name="02_dbt_snowflake", task_args={"name": "DBT"})
    dashboards = start_flow_run(
        flow_name="03_dashboards", task_args={"name": "Refresh"}
    )
    staging.set_downstream(dbt_run)
    dbt_run.set_downstream(dashboards)
