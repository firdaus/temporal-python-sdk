import threading

import pytest
from datetime import timedelta
import asyncio

from temporal.activity import Activity
from temporal.async_activity import Async
from temporal.decision_loop import ActivityFuture
from temporal.workflow import workflow_method, WorkflowClient, Workflow
from temporal.activity_method import activity_method


TASK_QUEUE = "test_activity_async_sync_tq"
NAMESPACE = "default"
executed = False


def greeting_activities_thread_func(task_token, sleep_seconds):
    async def fn():
        client = WorkflowClient.new_client(namespace=NAMESPACE)
        activity_completion_client = client.new_activity_completion_client()
        print(f"Sleeping for {sleep_seconds} seconds")
        await asyncio.sleep(sleep_seconds)
        await activity_completion_client.complete(task_token, sleep_seconds)
        client.close()

    asyncio.run(fn())


class GreetingActivities:
    @activity_method(task_queue=TASK_QUEUE, schedule_to_close_timeout=timedelta(seconds=1000))
    async def compose_greeting(self, arg) -> str:
        raise NotImplementedError


class GreetingActivitiesImpl:

    def compose_greeting(self, sleep_seconds):
        Activity.do_not_complete_on_return()
        thread = threading.Thread(target=greeting_activities_thread_func, args=(Activity.get_task_token(), sleep_seconds))
        thread.start()


class GreetingWorkflow:
    @workflow_method(task_queue=TASK_QUEUE)
    async def get_greeting(self) -> None:
        raise NotImplementedError


class GreetingWorkflowImpl(GreetingWorkflow):

    def __init__(self):
        self.greeting_activities: GreetingActivities = Workflow.new_activity_stub(GreetingActivities)

    async def get_greeting(self):
        f: ActivityFuture = Async.function(self.greeting_activities.compose_greeting, 10)
        await f
        v = f.get_result()
        return v


@pytest.mark.asyncio
@pytest.mark.worker_config(NAMESPACE, TASK_QUEUE, activities=[(GreetingActivitiesImpl(), "GreetingActivities")],
                           workflows=[GreetingWorkflowImpl])
async def test(worker):
    client = WorkflowClient.new_client(namespace=NAMESPACE)
    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow)
    ret_value = await greeting_workflow.get_greeting()
    assert ret_value == 10

