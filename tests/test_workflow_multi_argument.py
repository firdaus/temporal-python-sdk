import pytest

from temporal.workflow import workflow_method, WorkflowClient

TASK_QUEUE = "test_workflow_multi_argument"
DOMAIN = "default"


class GreetingWorkflow:
    @workflow_method(task_queue=TASK_QUEUE)
    async def get_greeting(self, arg1, arg2) -> None:
        raise NotImplementedError


class GreetingWorkflowImpl(GreetingWorkflow):
    argument1: object = None
    argument2: object = None

    async def get_greeting(self, arg1, arg2):
        GreetingWorkflowImpl.argument1 = arg1
        GreetingWorkflowImpl.argument2 = arg2


@pytest.mark.asyncio
@pytest.mark.worker_config(DOMAIN, TASK_QUEUE, activities=[], workflows=[GreetingWorkflowImpl])
async def test(worker):
    client = WorkflowClient.new_client(namespace=DOMAIN)
    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow)
    await greeting_workflow.get_greeting("first-argument", "second-argument")

    assert GreetingWorkflowImpl.argument1 == "first-argument"
    assert GreetingWorkflowImpl.argument2 == "second-argument"
