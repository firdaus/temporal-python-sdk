import pytest
from datetime import timedelta
import asyncio

from temporal.async_activity import Async
from temporal.workflow import workflow_method, WorkflowClient, Workflow
from temporal.activity_method import activity_method


TASK_QUEUE = "test_activity_async_all_of_tq"
NAMESPACE = "default"
executed = False


class GreetingActivities:
    @activity_method(task_queue=TASK_QUEUE, schedule_to_close_timeout=timedelta(seconds=1000))
    async def compose_greeting(self, arg) -> str:
        raise NotImplementedError


class GreetingActivitiesImpl:
    activity_method_executed_counter: int = 0

    async def compose_greeting(self, arg):
        GreetingActivitiesImpl.activity_method_executed_counter += 1
        return arg


class GreetingWorkflow:
    @workflow_method(task_queue=TASK_QUEUE)
    async def get_greeting(self) -> None:
        raise NotImplementedError


class GreetingWorkflowImpl(GreetingWorkflow):

    def __init__(self):
        self.greeting_activities: GreetingActivities = Workflow.new_activity_stub(GreetingActivities)

    async def get_greeting(self):
        futures = [
            Async.function(self.greeting_activities.compose_greeting, 10),
            Async.function(self.greeting_activities.compose_greeting, 20),
            Async.function(self.greeting_activities.compose_greeting, 30)
        ]
        await Async.all_of(futures)


@pytest.mark.asyncio
@pytest.mark.worker_config(NAMESPACE, TASK_QUEUE, activities=[(GreetingActivitiesImpl(), "GreetingActivities")],
                           workflows=[GreetingWorkflowImpl])
async def test(worker):
    client = WorkflowClient.new_client(namespace=NAMESPACE)
    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow)
    await greeting_workflow.get_greeting()

    assert GreetingActivitiesImpl.activity_method_executed_counter == 3
