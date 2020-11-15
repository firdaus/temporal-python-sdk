import asyncio

import pytest

from temporal.workflow import workflow_method, WorkflowClient, Workflow, query_method

TASK_QUEUE = "test_query_tq"
NAMESPACE = "default"
workflow_started = False


class GreetingWorkflow:

    @query_method
    async def get_status(self):
        raise NotImplementedError

    @query_method
    async def get_status_as_list(self):
        raise NotImplementedError

    @query_method
    async def get_status_as_list_with_args(self, a, b):
        raise NotImplementedError

    @workflow_method(task_queue=TASK_QUEUE)
    async def get_greeting(self) -> None:
        raise NotImplementedError


class GreetingWorkflowImpl(GreetingWorkflow):

    def __init__(self):
        self.status = None

    async def get_status(self):
        return self.status

    async def get_status_as_list(self):
        return [self.status]

    async def get_status_as_list_with_args(self, a, b):
        return [self.status, a, b]

    async def get_greeting(self):
        global workflow_started
        self.status = "STARTED"
        workflow_started = True
        await Workflow.sleep(60)
        self.status = "DONE"


@pytest.mark.asyncio
@pytest.mark.worker_config(NAMESPACE, TASK_QUEUE, activities=[], workflows=[GreetingWorkflowImpl])
async def test(worker):
    global workflow_started
    client = WorkflowClient.new_client(namespace=NAMESPACE)
    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow)
    context = await WorkflowClient.start(greeting_workflow.get_greeting)
    greeting_workflow = client.new_workflow_stub_from_workflow_id(GreetingWorkflow,
                                                                  workflow_id=context.workflow_execution.workflow_id)
    while not workflow_started:
        await asyncio.sleep(2)
    status = await greeting_workflow.get_status()
    assert status == "STARTED"
    status = await greeting_workflow.get_status_as_list()
    assert status == ["STARTED"]
    status = await greeting_workflow.get_status_as_list_with_args("1", "2")
    assert status == ["STARTED", "1", "2"]
    await client.wait_for_close(context)
