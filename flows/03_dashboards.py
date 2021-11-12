import prefect
from prefect import task, Flow
from prefect.storage import GitHub
from flow_utilities.prefect_configs import set_run_config, set_storage

FLOW_NAME = "03_dashboards"


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


with Flow(
    FLOW_NAME, storage=set_storage(FLOW_NAME), run_config=set_run_config(),
) as flow:
    customers = update_customers_dashboards()
    sales = update_sales_dashboards()
    customers.set_downstream(sales)
