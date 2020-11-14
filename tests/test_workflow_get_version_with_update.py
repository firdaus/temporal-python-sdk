import asyncio

import pytest

from temporal import DEFAULT_VERSION
from temporal.workerfactory import WorkerFactory
from temporal.workflow import workflow_method, Workflow, WorkflowClient
from tests import cleanup_worker

TASK_QUEUE = "test_get_version_with_update_tq"
NAMESPACE = "default"

v1_hits = 0
v2_hits = 0

version_found_in_v2_step_1_0 = None
version_found_in_v2_step_1_1 = None
version_found_in_v2_step_2_0 = None
version_found_in_v2_step_2_1 = None

v2_done = False


class TestWorkflowGetVersion:
    @workflow_method(task_queue=TASK_QUEUE)
    async def get_greetings(self) -> list:
        raise NotImplementedError


class TestWorkflowGetVersionImplV1(TestWorkflowGetVersion):

    def __init__(self):
        pass

    async def get_greetings(self):
        global v1_hits
        v1_hits += 1
        await Workflow.sleep(60)


class TestWorkflowGetVersionImplV2(TestWorkflowGetVersion):

    def __init__(self):
        pass

    async def get_greetings(self):
        global v2_hits
        global version_found_in_v2_step_1_0, version_found_in_v2_step_1_1
        global version_found_in_v2_step_2_0, version_found_in_v2_step_2_1
        global v2_done
        v2_hits += 1

        version_found_in_v2_step_1_0 = Workflow.get_version("first-item", DEFAULT_VERSION, 2)
        version_found_in_v2_step_1_1 = Workflow.get_version("first-item", DEFAULT_VERSION, 2)
        await Workflow.sleep(60)
        version_found_in_v2_step_2_0 = Workflow.get_version("first-item", DEFAULT_VERSION, 2)
        version_found_in_v2_step_2_1 = Workflow.get_version("first-item", DEFAULT_VERSION, 2)
        v2_done = True


@pytest.mark.asyncio
async def test_workflow_workflow_get_version():
    global v1_hits, v2_hits
    factory = WorkerFactory("localhost", 7233, NAMESPACE)
    worker = factory.new_worker(TASK_QUEUE)
    worker.register_workflow_implementation_type(TestWorkflowGetVersionImplV1)
    factory.start()

    client = WorkflowClient.new_client(namespace=NAMESPACE)
    workflow: TestWorkflowGetVersion = client.new_workflow_stub(TestWorkflowGetVersion)

    await client.start(workflow.get_greetings)
    while v1_hits == 0:
        print(".", end="")
        await asyncio.sleep(2)

    worker.register_workflow_implementation_type(TestWorkflowGetVersionImplV2)

    while not v2_done:
        print(".", end="")
        await asyncio.sleep(2)

    assert v1_hits == 1
    assert v2_hits == 1
    assert version_found_in_v2_step_1_0 == DEFAULT_VERSION
    assert version_found_in_v2_step_1_1 == DEFAULT_VERSION
    assert version_found_in_v2_step_2_0 == DEFAULT_VERSION
    assert version_found_in_v2_step_2_1 == DEFAULT_VERSION

    # TODO: Assert that there are no markers recorded

    cleanup_worker(worker)
