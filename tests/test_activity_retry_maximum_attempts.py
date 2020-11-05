import pytest
from datetime import timedelta

from temporal.workflow import workflow_method, WorkflowClient, Workflow
from temporal.activity_method import activity_method, RetryParameters

TASK_QUEUE = "test_activity_retry_maximum_attempts"
NAMESPACE = "default"
invoke_count = 0


class GreetingActivities:
    @activity_method(task_queue=TASK_QUEUE, schedule_to_close_timeout=timedelta(seconds=1000))
    async def compose_greeting(self) -> str:
        raise NotImplementedError


class GreetingActivitiesImpl:
    activity_method_executed: bool = False

    def compose_greeting(self):
        global invoke_count
        invoke_count += 1
        if invoke_count < 3:
            raise Exception("blah blah")
        else:
            return


class GreetingWorkflow:
    @workflow_method(task_queue=TASK_QUEUE)
    async def get_greeting(self) -> None:
        raise NotImplementedError


class GreetingWorkflowImpl(GreetingWorkflow):

    def __init__(self):
        retry_parameters = RetryParameters(backoff_coefficient=2.0, maximum_attempts=3)
        self.greeting_activities: GreetingActivities = Workflow.new_activity_stub(GreetingActivities,
                                                                                  retry_parameters=retry_parameters)

    async def get_greeting(self):
        await self.greeting_activities.compose_greeting()


@pytest.mark.asyncio
@pytest.mark.worker_config(NAMESPACE, TASK_QUEUE, activities=[(GreetingActivitiesImpl(), "GreetingActivities")],
                           workflows=[GreetingWorkflowImpl])
async def test(worker):
    client = WorkflowClient.new_client(namespace=NAMESPACE)
    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow)
    await greeting_workflow.get_greeting()
    assert invoke_count == 3
