import time
from datetime import timedelta

import pytest

from temporal.activity import Activity
from temporal.activity_method import activity_method
from temporal.async_activity import Async
from temporal.workflow import workflow_method, WorkflowClient, Workflow

TASK_QUEUE = "test_activity_async_any_of_timer_tq"
NAMESPACE = "default"
executed = False


class GreetingActivities:
    @activity_method(task_queue=TASK_QUEUE, schedule_to_close_timeout=timedelta(seconds=1000))
    async def compose_greeting(self, arg) -> str:
        raise NotImplementedError


class GreetingActivitiesImpl:

    async def compose_greeting(self, sleep_seconds):
        Activity.do_not_complete_on_return()


class GreetingWorkflow:
    @workflow_method(task_queue=TASK_QUEUE)
    async def get_greeting(self) -> None:
        raise NotImplementedError


done = None
timer = None


class GreetingWorkflowImpl(GreetingWorkflow):

    def __init__(self):
        self.greeting_activities: GreetingActivities = Workflow.new_activity_stub(GreetingActivities)

    async def get_greeting(self):
        global done, timer
        timer = Workflow.new_timer(5)
        futures = [
            Async.function(self.greeting_activities.compose_greeting, 50),
            timer
        ]
        done, pending = await Async.any_of(futures)


@pytest.mark.asyncio
@pytest.mark.worker_config(NAMESPACE, TASK_QUEUE, activities=[(GreetingActivitiesImpl(), "GreetingActivities")],
                           workflows=[GreetingWorkflowImpl])
async def test(worker):
    client = WorkflowClient.new_client(namespace=NAMESPACE)
    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow)
    start_time = time.time()
    await greeting_workflow.get_greeting()
    end_time = time.time()
    assert 5 < end_time - start_time < 50
    assert len(done) == 1
    assert done[0] is timer
    client.close()
