import asyncio

import pytest

from temporal.workflow import workflow_method, WorkflowClient, signal_method, Workflow

TASK_QUEUE = "test_await_till_tq"
NAMESPACE = "default"
await_returned = False


class GreetingWorkflow:

    @signal_method
    async def send_message(self, message: str):
        raise NotImplementedError

    @workflow_method(task_queue=TASK_QUEUE)
    async def get_greeting(self) -> None:
        raise NotImplementedError


class GreetingWorkflowImpl(GreetingWorkflow):

    def __init__(self):
        self.message = None

    async def send_message(self, message: str):
        self.message = message

    async def get_greeting(self):
        global await_returned

        def fn():
            return self.message == "done"

        await Workflow.await_till(fn)
        await_returned = True


@pytest.mark.asyncio
@pytest.mark.worker_config(NAMESPACE, TASK_QUEUE, activities=[], workflows=[GreetingWorkflowImpl])
async def test(worker):
    global await_returned
    client = WorkflowClient.new_client(namespace=NAMESPACE)
    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow)
    context = await WorkflowClient.start(greeting_workflow.get_greeting)
    greeting_workflow = client.new_workflow_stub_from_workflow_id(GreetingWorkflow,
                                                                  workflow_id=context.workflow_execution.workflow_id)
    await greeting_workflow.send_message("first")
    await asyncio.sleep(1)
    await greeting_workflow.send_message("second")
    await asyncio.sleep(1)
    await greeting_workflow.send_message("done")
    await client.wait_for_close(context)
    assert await_returned
