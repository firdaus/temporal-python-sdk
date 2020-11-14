import pytest
from temporal.workflow import workflow_method, WorkflowClient, Workflow

TASK_QUEUE = "test_workflow_workflow_id_run_id_tq"
NAMESPACE = "default"
workflow_id = None
run_id = None


class GreetingWorkflow:

    @workflow_method(task_queue=TASK_QUEUE)
    async def get_greeting(self) -> None:
        raise NotImplementedError


class GreetingWorkflowImpl(GreetingWorkflow):

    async def get_greeting(self):
        global workflow_id, run_id
        workflow_id = Workflow.get_workflow_id()
        run_id = Workflow.get_run_id()


@pytest.mark.asyncio
@pytest.mark.worker_config(NAMESPACE, TASK_QUEUE, activities=[], workflows=[GreetingWorkflowImpl])
async def test(worker):
    client = WorkflowClient.new_client(namespace=NAMESPACE)
    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow)
    await greeting_workflow.get_greeting()
    assert isinstance(workflow_id, str)
    assert isinstance(run_id, str)
