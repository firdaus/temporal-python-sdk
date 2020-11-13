import itertools
from uuid import UUID
import pytest
from temporal.workflow import workflow_method, WorkflowClient, Workflow

TASK_QUEUE = "test_workflow_random_uuid_tq"
NAMESPACE = "default"

a = []
b = []
c = []


class GreetingWorkflow:

    @workflow_method(task_queue=TASK_QUEUE)
    async def get_greeting(self) -> None:
        raise NotImplementedError


class GreetingWorkflowImpl(GreetingWorkflow):

    async def get_greeting(self):
        global a, b, c
        a.append(str(Workflow.random_uuid()))
        await Workflow.sleep(1)
        b.append(str(Workflow.random_uuid()))
        await Workflow.sleep(1)
        c.append(str(Workflow.random_uuid()))
        await Workflow.sleep(1)


@pytest.mark.asyncio
@pytest.mark.worker_config(NAMESPACE, TASK_QUEUE, activities=[], workflows=[GreetingWorkflowImpl])
async def test(worker):
    global a, b, c
    a, b, c = [], [], []
    client = WorkflowClient.new_client(namespace=NAMESPACE)
    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow)
    await greeting_workflow.get_greeting()
    assert len(a) >= 4 and len(set(a)) == 1
    assert len(b) >= 3 and len(set(b)) == 1
    assert len(c) >= 2 and len(set(c)) == 1
    assert len(set(itertools.chain(a, b, c))) == 3
    for d in itertools.chain(a, b, c):
        UUID(d, version=3)
