import pandas as pd
import prefect
from prefect import task, Flow
from prefect.executors import LocalDaskExecutor

from flow_utilities.db_utils import load_df_to_snowflake
from flow_utilities.prefect_configs import set_run_config, set_storage


FLOW_NAME = "01_extract_load"


@task
def extract_and_load(dataset: str) -> None:
    logger = prefect.context.get("logger")
    file = f"https://raw.githubusercontent.com/anna-geller/jaffle_shop/main/data/{dataset}.csv"
    df = pd.read_csv(file)
    load_df_to_snowflake(df, dataset)
    logger.info("Dataset %s with %d rows loaded to DB", dataset, len(df))


with Flow(
    FLOW_NAME,
    executor=LocalDaskExecutor(),
    storage=set_storage(FLOW_NAME),
    run_config=set_run_config(),
) as flow:
    datasets = ["raw_customers", "raw_orders", "raw_payments"]
    dataframes = extract_and_load.map(datasets)
