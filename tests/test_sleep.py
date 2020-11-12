import pytest
import time

from temporal.workflow import workflow_method, WorkflowClient, Workflow

TASK_QUEUE = "test_sleep"
NAMESPACE = "default"


class GreetingWorkflow:

    @workflow_method(task_queue=TASK_QUEUE)
    async def get_greeting(self) -> None:
        raise NotImplementedError


class GreetingWorkflowImpl(GreetingWorkflow):

    async def get_greeting(self):
        await Workflow.sleep(5)


@pytest.mark.asyncio
@pytest.mark.worker_config(NAMESPACE, TASK_QUEUE, activities=[], workflows=[GreetingWorkflowImpl])
async def test(worker):
    client = WorkflowClient.new_client(namespace=NAMESPACE)
    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow)
    start_time = time.time()
    await greeting_workflow.get_greeting()
    end_time = time.time()
    assert end_time - start_time > 5
