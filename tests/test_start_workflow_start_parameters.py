from datetime import timedelta, datetime

import pytest

from temporal.api.common.v1 import WorkflowExecution
from temporal.api.enums.v1 import WorkflowIdReusePolicy
from temporal.api.workflowservice.v1 import GetWorkflowExecutionHistoryRequest
from temporal.workflow import workflow_method, WorkflowClient, WorkflowOptions

TASK_QUEUE = "test_start_workflow_start_parameters_tq"
NAMESPACE = "default"


class GreetingWorkflow:
    @workflow_method(task_queue=TASK_QUEUE)
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
    options = WorkflowOptions()
    options.workflow_id = 'blah' + str(datetime.now())
    options.workflow_id_reuse_policy = WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
    options.workflow_execution_timeout = timedelta(seconds=1000)
    options.workflow_run_timeout = timedelta(seconds=500)
    options.workflow_task_timeout = timedelta(seconds=30)
    options.memo = {"name": "bob"}
    options.search_attributes = {}
    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow, workflow_options=options)
    await greeting_workflow.get_greeting()
    request = GetWorkflowExecutionHistoryRequest(namespace=NAMESPACE,
                                                 execution=WorkflowExecution(workflow_id=options.workflow_id))
    response = await client.service.get_workflow_execution_history(request=request)
    attr = response.history.events[0].workflow_execution_started_event_attributes
    assert attr.workflow_execution_timeout == timedelta(seconds=1000)
    assert attr.workflow_run_timeout == timedelta(seconds=500)
    assert attr.workflow_task_timeout == timedelta(seconds=30)
    assert attr.memo.fields["name"].data == b'"bob"'
