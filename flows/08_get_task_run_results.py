from prefect.backend import FlowRunView
from prefect.engine.signals import signal_from_state
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run, get_task_run_result
# todo build example to build on results of a child flow

@task
def raise_flow_run_state(flow_run: FlowRunView):
    flow_run_state = flow_run.state
    if not flow_run_state.is_successful():
        exc = signal_from_state(flow_run_state)(
            f"{flow_run.flow_run_id} finished in state {flow_run_state}"
        )
        raise exc
    return flow_run


with Flow("MasterFlow") as flow:
    staging_area_id = create_flow_run(flow_name="staging_area", project_name="Flow_of_Flows", run_name="staging_area")
    staging_area_flow_run_view = wait_for_flow_run(staging_area_id)
    raise_flow_run_state(staging_area_flow_run_view)
