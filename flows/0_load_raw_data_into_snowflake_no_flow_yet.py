import pandas as pd
from flow_utilities.db_utils import load_df_to_snowflake


if __name__ == "__main__":
    dataset = "raw_customers"
    file = f"https://raw.githubusercontent.com/anna-geller/jaffle_shop/main/data/{dataset}.csv"
    df = pd.read_csv(file)
    load_df_to_snowflake(df, dataset)
