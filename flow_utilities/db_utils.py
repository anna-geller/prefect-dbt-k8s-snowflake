import pandas as pd
from prefect.client import Secret
from sqlalchemy import create_engine
from prefect import resource_manager


def get_snowflake_connection_string(database: str = "DEV") -> str:
    user = Secret("SNOWFLAKE_USER").get()
    pwd = Secret("SNOWFLAKE_PASS").get()
    account_id = Secret("SNOWFLAKE_ACCOUNT_ID").get()
    return f"snowflake://{user}:{pwd}@{account_id}/{database}/JAFFLE_SHOP?warehouse=COMPUTE_WH&role=SYSADMIN"


def get_df_from_sql_query(table_or_query: str) -> pd.DataFrame:
    db = get_snowflake_connection_string()
    engine = create_engine(db)
    return pd.read_sql(table_or_query, engine)


def load_df_to_snowflake(df: pd.DataFrame, table_name: str, schema: str = "JAFFLE_SHOP") -> None:
    conn_string = get_snowflake_connection_string()
    db_engine = create_engine(conn_string)
    conn = db_engine.connect()
    # conn.execute(f"TRUNCATE TABLE DEV.{schema}.{table_name};")
    df.to_sql(table_name, schema=schema, con=db_engine, if_exists="replace", index=False)
    conn.close()


@resource_manager
class SnowflakeConnection:
    def __init__(self, database: str = "DEV"):
        self.database = database

    def setup(self):
        db_conn_string = get_snowflake_connection_string(self.database)
        db_engine = create_engine(db_conn_string)
        return db_engine.connect()

    @staticmethod
    def cleanup(conn):
        conn.close()
