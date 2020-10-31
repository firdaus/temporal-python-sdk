import pytest

from temporal.workflow import workflow_method, WorkflowClient

TASK_QUEUE = "test_workflow_single_argument"
NAMESPACE = "default"


class GreetingWorkflow:
    @workflow_method(task_queue=TASK_QUEUE)
    async def get_greeting(self, arg1) -> None:
        raise NotImplementedError


class GreetingWorkflowImpl(GreetingWorkflow):
    argument1: object = None

    async def get_greeting(self, arg1):
        GreetingWorkflowImpl.argument1 = arg1


@pytest.mark.asyncio
@pytest.mark.worker_config(NAMESPACE, TASK_QUEUE, activities=[], workflows=[GreetingWorkflowImpl])
async def test(worker):
    client = WorkflowClient.new_client(namespace=NAMESPACE)
    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow)
    await greeting_workflow.get_greeting("blah-blah-blah")

    assert GreetingWorkflowImpl.argument1 == "blah-blah-blah"
