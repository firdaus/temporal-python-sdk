import uuid

import pytest
from datetime import timedelta

from temporal.api.common.v1 import WorkflowExecution
from temporal.api.enums.v1 import EventType
from temporal.api.workflowservice.v1 import GetWorkflowExecutionHistoryRequest
from temporal.workflow import workflow_method, WorkflowClient, Workflow
from temporal.activity_method import activity_method, RetryParameters, ActivityOptions

TASK_QUEUE = "test_activity_method_activity_options_from_stub"
NAMESPACE = "default"
workflow_id = "test_activity_method_activity_options_from_stub-" + str(uuid.uuid4())


class GreetingActivities:
    @activity_method(task_queue=TASK_QUEUE)
    async def compose_greeting(self) -> str:
        raise NotImplementedError


class GreetingActivitiesImpl:

    def compose_greeting(self):
        pass


class GreetingWorkflow:
    @workflow_method(task_queue=TASK_QUEUE, workflow_id=workflow_id)
    async def get_greeting(self) -> None:
        raise NotImplementedError


class GreetingWorkflowImpl(GreetingWorkflow):

    def __init__(self):
        self.greeting_activities: GreetingActivities
        self.greeting_activities = Workflow.new_activity_stub(GreetingActivities,
                                                              activity_options=ActivityOptions(
                                                                  schedule_to_close_timeout=timedelta(seconds=1000),
                                                                  schedule_to_start_timeout=timedelta(seconds=500),
                                                                  start_to_close_timeout=timedelta(seconds=800),
                                                                  heartbeat_timeout=timedelta(seconds=600)),
                                                              retry_parameters=RetryParameters(
                                                                  initial_interval=timedelta(seconds=70),
                                                                  backoff_coefficient=5.0,
                                                                  maximum_interval=timedelta(seconds=700),
                                                                  maximum_attempts=8,
                                                                  non_retryable_error_types=["DummyError"]))
    async def get_greeting(self):
        await self.greeting_activities.compose_greeting()


@pytest.mark.asyncio
@pytest.mark.worker_config(NAMESPACE, TASK_QUEUE, activities=[(GreetingActivitiesImpl(), "GreetingActivities")],
                           workflows=[GreetingWorkflowImpl])
async def test(worker):
    client = WorkflowClient.new_client(namespace=NAMESPACE)
    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow)
    await greeting_workflow.get_greeting()
    request = GetWorkflowExecutionHistoryRequest(namespace=NAMESPACE,
                                                 execution=WorkflowExecution(workflow_id=workflow_id))
    response = await client.service.get_workflow_execution_history(request=request)
    e = next(filter(lambda v: v.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, response.history.events))
    assert e.activity_task_scheduled_event_attributes.schedule_to_close_timeout == timedelta(seconds=1000)
    assert e.activity_task_scheduled_event_attributes.schedule_to_start_timeout == timedelta(seconds=500)
    assert e.activity_task_scheduled_event_attributes.start_to_close_timeout == timedelta(seconds=800)
    assert e.activity_task_scheduled_event_attributes.heartbeat_timeout == timedelta(seconds=600)
    assert e.activity_task_scheduled_event_attributes.retry_policy.initial_interval == timedelta(seconds=70)
    assert e.activity_task_scheduled_event_attributes.retry_policy.backoff_coefficient == 5.0
    assert e.activity_task_scheduled_event_attributes.retry_policy.maximum_interval == timedelta(seconds=700)
    assert e.activity_task_scheduled_event_attributes.retry_policy.maximum_attempts == 8
    assert e.activity_task_scheduled_event_attributes.retry_policy.non_retryable_error_types == ["DummyError"]
