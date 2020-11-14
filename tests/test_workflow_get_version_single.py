import pytest

from temporal import DEFAULT_VERSION
from temporal.workflow import workflow_method, WorkflowClient, Workflow

TASK_QUEUE = "test_workflow_get_version_single_tq"
NAMESPACE = "default"

version_found_in_step_1_0 = None
version_found_in_step_1_1 = None
version_found_in_step_2_0 = None
version_found_in_step_2_1 = None


class GreetingWorkflow:

    @workflow_method(task_queue=TASK_QUEUE)
    async def get_greeting(self) -> None:
        raise NotImplementedError


class GreetingWorkflowImpl(GreetingWorkflow):

    async def get_greeting(self):
        global version_found_in_step_1_0, version_found_in_step_1_1
        global version_found_in_step_2_0, version_found_in_step_2_1
        version_found_in_step_1_0 = Workflow.get_version("first-item", DEFAULT_VERSION, 2)
        version_found_in_step_1_1 = Workflow.get_version("first-item", DEFAULT_VERSION, 2)
        await Workflow.sleep(60)
        version_found_in_step_2_0 = Workflow.get_version("first-item", DEFAULT_VERSION, 2)
        version_found_in_step_2_1 = Workflow.get_version("first-item", DEFAULT_VERSION, 2)


@pytest.mark.asyncio
@pytest.mark.worker_config(NAMESPACE, TASK_QUEUE, activities=[], workflows=[GreetingWorkflowImpl])
async def test(worker):
    client = WorkflowClient.new_client(namespace=NAMESPACE)
    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow)
    await greeting_workflow.get_greeting()

    assert version_found_in_step_1_0 == 2
    assert version_found_in_step_1_1 == 2
    assert version_found_in_step_2_0 == 2
    assert version_found_in_step_2_1 == 2
