import logging

import pytest

from .interceptor_testing_utils import reset_counter_filter_counter, LOGGING, get_counter_filter_counter
from temporal.workflow import workflow_method, WorkflowClient, Workflow

TASK_QUEUE = "test_workflow_workflow_get_logger"
NAMESPACE = "default"


class GreetingWorkflow:

    @workflow_method(task_queue=TASK_QUEUE)
    async def get_greeting(self) -> None:
        raise NotImplementedError


class GreetingWorkflowImpl(GreetingWorkflow):

    async def get_greeting(self):
        logger = Workflow.get_logger("test-logger")
        logger.info("********Test %d", 1)
        await Workflow.sleep(10)
        logger.info("********Test %d", 2)
        await Workflow.sleep(10)
        logger.info("********Test %d", 3)
        await Workflow.sleep(10)
        logger.info("********Test %d", 4)
        await Workflow.sleep(10)
        logger.info("********Test %d", 5)


@pytest.mark.asyncio
@pytest.mark.worker_config(NAMESPACE, TASK_QUEUE, activities=[], workflows=[GreetingWorkflowImpl])
async def test(worker):
    reset_counter_filter_counter()
    logging.config.dictConfig(LOGGING)

    client = WorkflowClient.new_client(namespace=NAMESPACE)
    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow)
    await greeting_workflow.get_greeting()
    assert get_counter_filter_counter() == 5
