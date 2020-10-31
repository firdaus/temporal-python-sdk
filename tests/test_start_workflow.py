import pytest

from temporal.workflow import workflow_method, WorkflowClient

TASK_QUEUE = "test_start_workflow_tq"
NAMESPACE = "default"


class GreetingWorkflow:
    @workflow_method(task_queue=TASK_QUEUE)
    async def get_greeting(self) -> None:
        raise NotImplementedError


class GreetingWorkflowImpl(GreetingWorkflow):
    workflow_method_executed: bool = False

    async def get_greeting(self):
        GreetingWorkflowImpl.workflow_method_executed = True


@pytest.mark.asyncio
@pytest.mark.worker_config(NAMESPACE, TASK_QUEUE, activities=[], workflows=[GreetingWorkflowImpl])
async def test(worker):
    client = WorkflowClient.new_client(namespace=NAMESPACE)
    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow)
    await greeting_workflow.get_greeting()

    assert GreetingWorkflowImpl.workflow_method_executed
