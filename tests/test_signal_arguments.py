import pytest

from temporal.workflow import workflow_method, WorkflowClient, signal_method, Workflow

TASK_QUEUE = "test_start_signal_tq"
NAMESPACE = "default"


class GreetingWorkflow:

    @signal_method
    async def hello(self, a, b):
        raise NotImplementedError

    @workflow_method(task_queue=TASK_QUEUE)
    async def get_greeting(self) -> None:
        raise NotImplementedError


class GreetingWorkflowImpl(GreetingWorkflow):

    def __init__(self):
        self.signal_arguments = []

    async def hello(self, a, b):
        self.signal_arguments = [a, b]

    async def get_greeting(self):
        def fn():
            return self.signal_arguments
        await Workflow.await_till(fn)
        return self.signal_arguments


@pytest.mark.asyncio
@pytest.mark.worker_config(NAMESPACE, TASK_QUEUE, activities=[], workflows=[GreetingWorkflowImpl])
async def test(worker):
    client = WorkflowClient.new_client(namespace=NAMESPACE)
    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow)
    context = await WorkflowClient.start(greeting_workflow.get_greeting)
    greeting_workflow = client.new_workflow_stub_from_workflow_id(GreetingWorkflow,
                                                                  workflow_id=context.workflow_execution.workflow_id)
    await greeting_workflow.hello("1", 2)
    ret_value = await client.wait_for_close(context)
    assert ret_value == ["1", 2]
