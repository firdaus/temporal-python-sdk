import pytest
from datetime import timedelta

from temporal.activity import Activity
from temporal.api.common.v1 import WorkflowExecution
from temporal.workflow import workflow_method, WorkflowClient, Workflow
from temporal.activity_method import activity_method

TASK_QUEUE = "test_activity_activity_attributes"
NAMESPACE = "default"
namespace = None
task_token = None
workflow_execution: WorkflowExecution = None


class GreetingActivities:
    @activity_method(task_queue=TASK_QUEUE, schedule_to_close_timeout=timedelta(seconds=1000))
    async def compose_greeting(self) -> str:
        raise NotImplementedError


class GreetingActivitiesImpl:
    activity_method_executed: bool = False

    async def compose_greeting(self):
        global namespace, task_token, workflow_execution
        namespace = Activity.get_namespace()
        task_token = Activity.get_task_token()
        workflow_execution = Activity.get_workflow_execution()


class GreetingWorkflow:
    @workflow_method(task_queue=TASK_QUEUE)
    async def get_greeting(self) -> None:
        raise NotImplementedError


class GreetingWorkflowImpl(GreetingWorkflow):

    def __init__(self):
        self.greeting_activities: GreetingActivities = Workflow.new_activity_stub(GreetingActivities)

    async def get_greeting(self):
        await self.greeting_activities.compose_greeting()


@pytest.mark.asyncio
@pytest.mark.worker_config(NAMESPACE, TASK_QUEUE, activities=[(GreetingActivitiesImpl(), "GreetingActivities")],
                           workflows=[GreetingWorkflowImpl])
async def test(worker):
    client = WorkflowClient.new_client(namespace=NAMESPACE)
    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow)
    await greeting_workflow.get_greeting()
    assert namespace == "default"
    assert task_token is not None
    assert workflow_execution is not None
    assert workflow_execution.workflow_id is not None
    assert workflow_execution.run_id is not None
