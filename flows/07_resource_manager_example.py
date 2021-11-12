from flow_utilities.db_utils import SnowflakeConnection
import pandas as pd
from prefect import task, Flow
from flow_utilities.prefect_configs import set_run_config, set_storage

FLOW_NAME = "07_resource_manager_example"


@task
def get_customer_data(db_conn):
    return pd.read_sql("SELECT * FROM CUSTOMERS;", db_conn)


@task
def get_order_data(db_conn):
    return pd.read_sql("SELECT * FROM ORDERS;", db_conn)


@task
def merge_data(orders_df, customers_df):
    return orders_df.merge(customers_df, how="left", on="customer_id")


@task
def load_to_csv_for_report(df):
    df.to_csv("merged_data.csv", index=False)


with Flow(
    FLOW_NAME, storage=set_storage(FLOW_NAME), run_config=set_run_config(),
) as flow:
    with SnowflakeConnection() as conn:
        customers = get_customer_data(conn)
        orders = get_order_data(conn)
    final_data = merge_data(orders, customers)
    load_to_csv_for_report(final_data)

if __name__ == "__main__":
    flow.run()
