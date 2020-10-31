import pytest
from datetime import timedelta

from temporal.activity import Activity
from temporal.workflow import workflow_method, WorkflowClient, Workflow
from temporal.activity_method import activity_method, RetryParameters

TASK_QUEUE = "test_activity_heartbeat"
DOMAIN = "default"
HEARTBEAT_VALUE: str = "bb93fbab-574b-4239-9dfc-f5d03a21a84e"
captured_heartbeat_value: str = None


class GreetingActivities:
    @activity_method(task_queue=TASK_QUEUE, schedule_to_close_timeout=timedelta(seconds=1000))
    async def compose_greeting(self) -> str:
        raise NotImplementedError


class GreetingActivitiesImpl:

    def __init__(self):
        self.heartbeated = False

    async def compose_greeting(self):
        global captured_heartbeat_value
        if not self.heartbeated:
            await Activity.heartbeat(HEARTBEAT_VALUE)
            self.heartbeated = True
            raise Exception("Blah")
        else:
            captured_heartbeat_value = Activity.get_heartbeat_details()
            return captured_heartbeat_value


class GreetingWorkflow:
    @workflow_method(task_queue=TASK_QUEUE)
    async def get_greeting(self) -> None:
        raise NotImplementedError


class GreetingWorkflowImpl(GreetingWorkflow):

    def __init__(self):
        retry_parameters = RetryParameters(initial_interval_in_seconds=1, backoff_coefficient=2.0, maximum_attempts=3)
        self.greeting_activities: GreetingActivities = Workflow.new_activity_stub(GreetingActivities,
                                                                                  retry_parameters=retry_parameters)

    async def get_greeting(self):
        return await self.greeting_activities.compose_greeting()


@pytest.mark.asyncio
@pytest.mark.worker_config(DOMAIN, TASK_QUEUE, activities=[(GreetingActivitiesImpl(), "GreetingActivities")],
                           workflows=[GreetingWorkflowImpl])
async def test(worker):
    client = WorkflowClient.new_client(namespace=DOMAIN)
    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow)
    ret_value = await greeting_workflow.get_greeting()

    assert ret_value == HEARTBEAT_VALUE
    assert captured_heartbeat_value == HEARTBEAT_VALUE
