"""
This flow is largely inspired by https://medium.com/slateco-blog/prefect-orchestrating-dbt-10f3ca0baea9
"""
import prefect
from prefect import task, Flow, Parameter
from prefect.client import Secret
from prefect.tasks.dbt.dbt import DbtShellTask
from prefect.storage import GitHub
from prefect.triggers import all_finished
from prefect.run_configs import LocalRun
import pygit2
import shutil


DBT_PROJECT = "jaffle_shop"
FLOW_NAME = "02_dbt_snowflake"
STORAGE = GitHub(
    repo="anna-geller/prefect-dbt-k8s-snowflake",
    ref="prefect-dbt-snowflake",
    path=f"flows/{FLOW_NAME}.py",
    access_token_secret="GITHUB_ACCESS_TOKEN",
)


@task(name="Clone DBT repo")
def pull_dbt_repo(repo_url: str):
    pygit2.clone_repository(url=repo_url, path=DBT_PROJECT)


@task(name="Delete DBT folder if exists", trigger=all_finished)
def delete_dbt_folder_if_exists():
    shutil.rmtree(DBT_PROJECT, ignore_errors=True)


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
        "account": Secret("SNOWFLAKE_ACCOUNT_ID").get(),
        "schema": DBT_PROJECT,
        "user": Secret("SNOWFLAKE_USER").get(),
        "password": Secret("SNOWFLAKE_PASS").get(),
        "role": Secret("SNOWFLAKE_ROLE").get(),
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


with Flow(FLOW_NAME, storage=STORAGE, run_config=LocalRun(labels=["dev"]),) as flow:
    del_task = delete_dbt_folder_if_exists()
    dbt_repo = Parameter(
        "dbt_repo_url", default="https://github.com/anna-geller/jaffle_shop"
    )
    pull_task = pull_dbt_repo(dbt_repo)
    del_task.set_downstream(pull_task)

    dbt_run = dbt(command="dbt run", task_args={"name": "DBT Run"})
    dbt_run_out = print_dbt_output(dbt_run, task_args={"name": "DBT Run Output"})
    pull_task.set_downstream(dbt_run)

    dbt_test = dbt(command="dbt test", task_args={"name": "DBT Test"})
    dbt_test_out = print_dbt_output(dbt_test, task_args={"name": "DBT Test Output"})
    dbt_run.set_downstream(dbt_test)

    del_again = delete_dbt_folder_if_exists()
    dbt_test_out.set_downstream(del_again)
