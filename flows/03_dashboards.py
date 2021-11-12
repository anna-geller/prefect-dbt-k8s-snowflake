import prefect
from prefect import task, Flow
from prefect.storage import GitHub
from prefect.run_configs import LocalRun

FLOW_NAME = "03_dashboards"
STORAGE = GitHub(
    repo="anna-geller/prefect-dbt-k8s-snowflake",
    ref="prefect-dbt-snowflake",
    path=f"flows/{FLOW_NAME}.py",
    access_token_secret="GITHUB_ACCESS_TOKEN",
)


@task
def update_customers_dashboards():
    logger = prefect.context.get("logger")
    # your logic here
    logger.info("Customers dashboard extracts updated!")


@task
def update_sales_dashboards():
    logger = prefect.context.get("logger")
    # your logic here
    logger.info("Sales dashboard extracts updated!")


with Flow(FLOW_NAME, storage=STORAGE, run_config=LocalRun(labels=["dev"]),) as flow:
    customers = update_customers_dashboards()
    sales = update_sales_dashboards()
    customers.set_downstream(sales)
