import asyncio

import pytest

from temporal.api.workflowservice.v1 import TerminateWorkflowExecutionRequest, TerminateWorkflowExecutionResponse
from temporal.service_helpers import get_identity
from temporal.workflow import workflow_method, WorkflowClient, cron_schedule, WorkflowExecutionContext

TASK_QUEUE = "test_cron"
NAMESPACE = "default"
invoke_count = 0


class GreetingWorkflow:
    @workflow_method(task_queue=TASK_QUEUE)
    @cron_schedule("*/1 * * * *")
    async def get_greeting(self) -> None:
        raise NotImplementedError


class GreetingWorkflowImpl(GreetingWorkflow):

    async def get_greeting(self):
        global invoke_count
        invoke_count += 1


@pytest.mark.asyncio
@pytest.mark.worker_config(NAMESPACE, TASK_QUEUE, activities=[], workflows=[GreetingWorkflowImpl])
async def test(worker):
    client = WorkflowClient.new_client(namespace=NAMESPACE)
    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow)
    context: WorkflowExecutionContext = await client.start(greeting_workflow.get_greeting)
    await asyncio.sleep(60 * 3)
    assert invoke_count >= 2
    request = TerminateWorkflowExecutionRequest()
    request.namespace = NAMESPACE
    request.identity = get_identity()
    request.workflow_execution = context.workflow_execution
    request.workflow_execution.run_id = None
    response: TerminateWorkflowExecutionResponse = await client.service.terminate_workflow_execution(request=request)
