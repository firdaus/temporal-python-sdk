import pytest

from temporal.workflow import workflow_method, WorkflowClient, signal_method, Workflow

TASK_QUEUE = "test_start_signal_tq"
NAMESPACE = "default"
signal_invoked: bool = False


class GreetingWorkflow:

    @signal_method
    async def hello(self):
        raise NotImplementedError

    @workflow_method(task_queue=TASK_QUEUE)
    async def get_greeting(self) -> None:
        raise NotImplementedError


class GreetingWorkflowImpl(GreetingWorkflow):

    def __init__(self):
        self.signal_invoked = False

    async def hello(self):
        global signal_invoked
        self.signal_invoked = signal_invoked = True

    async def get_greeting(self):
        def fn():
            return self.signal_invoked
        await Workflow.await_till(fn)


@pytest.mark.asyncio
@pytest.mark.worker_config(NAMESPACE, TASK_QUEUE, activities=[], workflows=[GreetingWorkflowImpl])
async def test(worker):
    global signal_invoked
    client = WorkflowClient.new_client(namespace=NAMESPACE)
    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow)
    context = await WorkflowClient.start(greeting_workflow.get_greeting)
    greeting_workflow = client.new_workflow_stub_from_workflow_id(GreetingWorkflow,
                                                                  workflow_id=context.workflow_execution.workflow_id)
    await greeting_workflow.hello()
    await client.wait_for_close(context)
    assert signal_invoked
