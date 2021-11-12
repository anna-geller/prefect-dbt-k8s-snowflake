from prefect import Flow
from prefect.storage import GitHub
from prefect.tasks.prefect import StartFlowRun
from prefect.run_configs import KubernetesRun
from prefect.client.secrets import Secret

AWS_ACCOUNT_ID = Secret("AWS_ACCOUNT_ID").get()
FLOW_NAME = "04_orchestrating_flow"
STORAGE = GitHub(
    repo="anna-geller/flow-of-flows",
    ref="prefect-dbt-snowflake-k8s",
    path=f"flows/{FLOW_NAME}.py",
    access_token_secret="GITHUB_ACCESS_TOKEN",
)
PROJECT_NAME = "jaffle_shop"
start_flow_run = StartFlowRun(project_name=PROJECT_NAME, wait=True)


with Flow(
    FLOW_NAME,
    storage=STORAGE,
    # run_config=LocalRun(labels=["dev"]),
    run_config=KubernetesRun(
        labels=["dev"],
        image=f"{AWS_ACCOUNT_ID}.dkr.ecr.us-east-1.amazonaws.com/prefect-dbt-demo:latest",
        image_pull_secrets=["ecr"],
    ),
) as flow:
    staging = start_flow_run(flow_name="01_extract_load", task_args={"name": "Stage"})
    dbt_run = start_flow_run(flow_name="02_dbt_snowflake", task_args={"name": "DBT"})
    dashboards = start_flow_run(
        flow_name="03_dashboards", task_args={"name": "Refresh"}
    )
    staging.set_downstream(dbt_run)
    dbt_run.set_downstream(dashboards)
