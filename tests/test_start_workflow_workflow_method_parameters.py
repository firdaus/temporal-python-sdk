from datetime import timedelta, datetime

import pytest

from temporal.api.common.v1 import WorkflowExecution
from temporal.api.enums.v1 import WorkflowIdReusePolicy
from temporal.api.workflowservice.v1 import GetWorkflowExecutionHistoryRequest
from temporal.workflow import workflow_method, WorkflowClient, WorkflowOptions

TASK_QUEUE = "test_start_workflow_workflow_method_parameters"
NAMESPACE = "default"

workflow_id = 'blah' + str(datetime.now())

class GreetingWorkflow:
    @workflow_method(task_queue=TASK_QUEUE,
                     workflow_id=workflow_id,
                     workflow_id_reuse_policy=WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
                     workflow_execution_timeout=timedelta(seconds=1000),
                     workflow_run_timeout=timedelta(seconds=500),
                     workflow_task_timeout=timedelta(seconds=30),
                     memo={"name": "bob"},
                     search_attributes={})
    async def get_greeting(self) -> None:
        raise NotImplementedError


class GreetingWorkflowImpl(GreetingWorkflow):
    workflow_method_executed: bool = False

    async def get_greeting(self):
        pass


@pytest.mark.asyncio
@pytest.mark.worker_config(NAMESPACE, TASK_QUEUE, activities=[], workflows=[GreetingWorkflowImpl])
async def test(worker):
    client = WorkflowClient.new_client(namespace=NAMESPACE)
    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow)
    await greeting_workflow.get_greeting()
    request = GetWorkflowExecutionHistoryRequest(namespace=NAMESPACE,
                                                 execution=WorkflowExecution(workflow_id=workflow_id))
    response = await client.service.get_workflow_execution_history(request=request)
    attr = response.history.events[0].workflow_execution_started_event_attributes
    assert attr.workflow_execution_timeout == timedelta(seconds=1000)
    assert attr.workflow_run_timeout == timedelta(seconds=500)
    assert attr.workflow_task_timeout == timedelta(seconds=30)
    assert attr.memo.fields["name"].data == b'"bob"'
