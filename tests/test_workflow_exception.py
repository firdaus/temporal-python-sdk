import pytest

from temporal.workflow import workflow_method, WorkflowClient

TASK_QUEUE = "test_workflow_exception"
NAMESPACE = "default"


class GreetingException(Exception):

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class GreetingWorkflow:
    @workflow_method(task_queue=TASK_QUEUE)
    async def get_greeting(self) -> None:
        raise NotImplementedError


class GreetingWorkflowImpl(GreetingWorkflow):

    async def get_greeting(self):
        raise GreetingException("blah")


@pytest.mark.asyncio
@pytest.mark.worker_config(NAMESPACE, TASK_QUEUE, activities=[], workflows=[GreetingWorkflowImpl])
async def test(worker):
    client = WorkflowClient.new_client(namespace=NAMESPACE)
    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow)
    caught_exception = None
    try:
        await greeting_workflow.get_greeting()
    except GreetingException as e:
        caught_exception = e
    assert caught_exception
    assert isinstance(caught_exception, GreetingException)
