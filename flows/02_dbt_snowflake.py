"""
This flow is largely inspired by https://medium.com/slateco-blog/prefect-orchestrating-dbt-10f3ca0baea9
"""
import prefect
from prefect import task, Flow, Parameter
from prefect.client import Secret
from prefect.tasks.dbt.dbt import DbtShellTask
from prefect.triggers import all_finished
import pygit2
import shutil
from flow_utilities.prefect_configs import set_run_config, set_storage
from prefect.tasks.secrets import PrefectSecret


DBT_PROJECT = "jaffle_shop"
FLOW_NAME = "02_dbt_snowflake"


@task(name="Clone DBT repo")
def pull_dbt_repo(repo_url: str):
    pygit2.clone_repository(url=repo_url, path=DBT_PROJECT)


@task(name="Delete DBT folder if exists", trigger=all_finished)
def delete_dbt_folder_if_exists():
    shutil.rmtree(DBT_PROJECT, ignore_errors=True)


@task
def get_dbt_credentials(user_name: str, password: str, role: str, account_id: str):
    return {
        "user": user_name,
        "password": password,
        "role": role,
        "account": account_id,
    }


dbt = DbtShellTask(
    return_all=True,
    profile_name=DBT_PROJECT,
    environment="dev",
    overwrite_profiles=True,
    log_stdout=True,
    helper_script=f"cd {DBT_PROJECT}",
    log_stderr=True,
    dbt_kwargs={
        "type": "snowflake",
        "schema": DBT_PROJECT,
        "database": "DEV",
        "warehouse": "COMPUTE_WH",
        "threads": 4,
        "client_session_keep_alive": False,
    },
)


@task(trigger=all_finished)
def print_dbt_output(output):
    logger = prefect.context.get("logger")
    for line in output:
        logger.info(line)


with Flow(
    FLOW_NAME, storage=set_storage(FLOW_NAME), run_config=set_run_config(),
) as flow:
    del_task = delete_dbt_folder_if_exists()
    dbt_repo = Parameter(
        "dbt_repo_url", default="https://github.com/anna-geller/jaffle_shop"
    )
    pull_task = pull_dbt_repo(dbt_repo)
    del_task.set_downstream(pull_task)

    snowflake_user = PrefectSecret("SNOWFLAKE_USER")
    snowflake_pass = PrefectSecret("SNOWFLAKE_PASS")
    snowflake_role = PrefectSecret("SNOWFLAKE_ROLE")
    snowflake_accid = PrefectSecret("SNOWFLAKE_ACCOUNT_ID")
    credentials = get_dbt_credentials(
        user_name=snowflake_user,
        password=snowflake_pass,
        role=snowflake_role,
        account_id=snowflake_accid,
    )

    dbt_run = dbt(
        command="dbt run", task_args={"name": "DBT Run"}, dbt_kwargs=credentials
    )
    dbt_run_out = print_dbt_output(dbt_run, task_args={"name": "DBT Run Output"})
    pull_task.set_downstream(dbt_run)

    dbt_test = dbt(
        command="dbt test", task_args={"name": "DBT Test"}, dbt_kwargs=credentials
    )
    dbt_test_out = print_dbt_output(dbt_test, task_args={"name": "DBT Test Output"})
    dbt_run.set_downstream(dbt_test)

    del_again = delete_dbt_folder_if_exists()
    dbt_test_out.set_downstream(del_again)

flow.set_reference_tasks([dbt_run])
