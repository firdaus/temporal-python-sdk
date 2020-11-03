import asyncio
import threading
from datetime import timedelta
from time import sleep

import pytest

from temporal.activity import Activity
from temporal.workflow import workflow_method, WorkflowClient, Workflow
from temporal.activity_method import activity_method

TASK_QUEUE = "test_async_do_not_complete_on_return_complete_tq"
NAMESPACE = "default"


class GreetingActivities:
    @activity_method(task_queue=TASK_QUEUE, schedule_to_close_timeout=timedelta(seconds=1000))
    async def compose_greeting(self) -> str:
        raise NotImplementedError


def greeting_activities_thread_func(task_token):
    async def fn():
        client = WorkflowClient.new_client(namespace=NAMESPACE)
        activity_completion_client = client.new_activity_completion_client()
        sleep(10)
        return_value = "from-activity-completion-client"
        await activity_completion_client.complete(task_token, return_value)
        client.close()

    asyncio.run(fn())


class GreetingActivitiesImpl:

    def compose_greeting(self):
        Activity.do_not_complete_on_return()
        thread = threading.Thread(target=greeting_activities_thread_func, args=(Activity.get_task_token(),))
        thread.start()


class GreetingWorkflow:
    @workflow_method(task_queue=TASK_QUEUE)
    async def get_greeting(self) -> None:
        raise NotImplementedError


class GreetingWorkflowImpl(GreetingWorkflow):

    def __init__(self):
        self.greeting_activities: GreetingActivities = Workflow.new_activity_stub(GreetingActivities)

    async def get_greeting(self):
        return await self.greeting_activities.compose_greeting()


@pytest.mark.asyncio
@pytest.mark.worker_config(NAMESPACE, TASK_QUEUE, activities=[(GreetingActivitiesImpl(), "GreetingActivities")],
                           workflows=[GreetingWorkflowImpl])
async def test(worker):
    client = WorkflowClient.new_client(namespace=NAMESPACE)
    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow)
    ret_value = await greeting_workflow.get_greeting()

    assert ret_value == "from-activity-completion-client"
