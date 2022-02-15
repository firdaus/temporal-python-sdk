import asyncio
import threading
from datetime import timedelta
from time import sleep

import pytest

from temporal.activity import Activity
from temporal.exceptions import ActivityFailureException
from temporal.workflow import workflow_method, WorkflowClient, Workflow
from temporal.activity_method import activity_method, RetryParameters

TASK_QUEUE = "test_async_do_not_complete_on_return_complete_exceptionally_tq"
NAMESPACE = "default"
caught_exception = None


class GreetingException(Exception):

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class GreetingActivities:
    @activity_method(task_queue=TASK_QUEUE, schedule_to_close_timeout=timedelta(seconds=1000))
    async def compose_greeting(self) -> str:
        raise NotImplementedError


def greeting_activities_thread_func(task_token):
    async def fn():
        client = WorkflowClient.new_client(namespace=NAMESPACE)
        activity_completion_client = client.new_activity_completion_client()
        sleep(10)
        await activity_completion_client.complete_exceptionally(task_token, GreetingException("greeting error!"))
        client.close()

    asyncio.run(fn())


class GreetingActivitiesImpl:

    async def compose_greeting(self):
        Activity.do_not_complete_on_return()
        thread = threading.Thread(target=greeting_activities_thread_func, args=(Activity.get_task_token(),))
        thread.start()


class GreetingWorkflow:
    @workflow_method(task_queue=TASK_QUEUE)
    async def get_greeting(self) -> None:
        raise NotImplementedError


class GreetingWorkflowImpl(GreetingWorkflow):

    def __init__(self):
        retry_parameters = RetryParameters(maximum_attempts=1)
        self.greeting_activities: GreetingActivities = Workflow.new_activity_stub(GreetingActivities,
                                                                              retry_parameters=retry_parameters)

    async def get_greeting(self):
        try:
            return await self.greeting_activities.compose_greeting()
        except Exception as ex:
            global caught_exception
            caught_exception = ex


@pytest.mark.asyncio
@pytest.mark.worker_config(NAMESPACE, TASK_QUEUE, activities=[(GreetingActivitiesImpl(), "GreetingActivities")],
                           workflows=[GreetingWorkflowImpl])
async def test(worker):
    client = WorkflowClient.new_client(namespace=NAMESPACE)
    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow)
    await greeting_workflow.get_greeting()
    assert caught_exception is not None
    assert isinstance(caught_exception, ActivityFailureException)
    cause = caught_exception.get_cause()
    assert isinstance(cause, GreetingException)
    assert cause.__traceback__ is not None
    assert cause.args == ("greeting error!",)

