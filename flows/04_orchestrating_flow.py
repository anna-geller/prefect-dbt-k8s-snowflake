from prefect import Flow
from prefect.storage import GitHub
from prefect.tasks.prefect import StartFlowRun
from prefect.run_configs import LocalRun

FLOW_NAME = "04_orchestrating_flow"
STORAGE = GitHub(
    repo="anna-geller/prefect-dbt-k8s-snowflake",
    ref="prefect-dbt-snowflake",
    path=f"flows/{FLOW_NAME}.py",
    access_token_secret="GITHUB_ACCESS_TOKEN",
)
PROJECT_NAME = "jaffle_shop"
start_flow_run = StartFlowRun(project_name=PROJECT_NAME, wait=True)


with Flow(FLOW_NAME, storage=STORAGE, run_config=LocalRun(labels=["dev"]),) as flow:
    staging = start_flow_run(flow_name="01_extract_load", task_args={"name": "Stage"})
    dbt_run = start_flow_run(flow_name="02_dbt_snowflake", task_args={"name": "DBT"})
    dashboards = start_flow_run(
        flow_name="03_dashboards", task_args={"name": "Refresh"}
    )
    staging.set_downstream(dbt_run)
    dbt_run.set_downstream(dashboards)
