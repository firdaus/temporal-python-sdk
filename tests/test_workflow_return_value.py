import pytest

from temporal.workflow import workflow_method, WorkflowClient

TASK_QUEUE = "test_workflow_return_value_tq"
NAMESPACE = "default"


class GreetingWorkflow:
    @workflow_method(task_queue=TASK_QUEUE)
    async def get_greeting(self) -> None:
        raise NotImplementedError


class GreetingWorkflowImpl(GreetingWorkflow):

    async def get_greeting(self):
        return "from-workflow"


@pytest.mark.asyncio
@pytest.mark.worker_config(NAMESPACE, TASK_QUEUE, activities=[], workflows=[GreetingWorkflowImpl])
async def test(worker):
    client = WorkflowClient.new_client(namespace=NAMESPACE)
    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow)
    return_value = await greeting_workflow.get_greeting()

    assert return_value == "from-workflow"
