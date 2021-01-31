import contextvars
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timedelta

from typing import List, Optional

from temporal.api.common.v1 import WorkflowExecution, ActivityType, Payloads
from temporal.api.workflowservice.v1 import PollActivityTaskQueueResponse, RecordActivityTaskHeartbeatRequest, \
    RespondActivityTaskFailedRequest, RespondActivityTaskCompletedRequest
from temporal.exception_handling import serialize_exception
from temporal.exceptions import ActivityCancelledException
from temporal.service_helpers import get_identity

current_activity_context: ContextVar['ActivityContext'] = contextvars.ContextVar("current_activity_context")


@dataclass
class ActivityTask:
    @classmethod
    def from_poll_for_activity_task_response(cls, task: PollActivityTaskQueueResponse) -> 'ActivityTask':
        activity_task: 'ActivityTask' = cls()
        activity_task.task_token = task.task_token
        activity_task.workflow_execution = task.workflow_execution
        activity_task.activity_id = task.activity_id
        activity_task.activity_type = task.activity_type
        activity_task.scheduled_time = task.scheduled_time
        activity_task.schedule_to_close_timeout = task.schedule_to_close_timeout
        activity_task.start_to_close_timeout = task.start_to_close_timeout
        activity_task.heartbeat_timeout = task.heartbeat_timeout
        activity_task.attempt = task.attempt
        activity_task.heartbeat_details = task.heartbeat_details
        activity_task.workflow_namespace = task.workflow_namespace
        return activity_task

    task_token: bytes = None
    workflow_execution: WorkflowExecution = None
    activity_id: str = None
    activity_type: ActivityType = None
    scheduled_time: datetime = None
    schedule_to_close_timeout: timedelta = None
    start_to_close_timeout: timedelta = None
    heartbeat_timeout: timedelta = None
    attempt: int = None
    heartbeat_details: Payloads = None
    workflow_namespace: str = None


async def heartbeat(client: 'WorkflowClient', task_token: bytes, details: object):
    request: RecordActivityTaskHeartbeatRequest = RecordActivityTaskHeartbeatRequest()
    request.details = client.data_converter.to_payloads([details])
    request.identity = get_identity()
    request.task_token = task_token
    response = await client.service.record_activity_task_heartbeat(request=request)
    # -----
    # if error:
    #     raise error
    # -----
    if response.cancel_requested:
        raise ActivityCancelledException()


class ActivityContext:
    client: 'WorkflowClient' = None
    activity_task: ActivityTask = None
    namespace: str = None
    do_not_complete: bool = False

    @staticmethod
    def get() -> 'ActivityContext':
        return current_activity_context.get()

    @staticmethod
    def set(context: Optional['ActivityContext']):
        current_activity_context.set(context)

    # Plan for heartbeat:
    # - We will possibly implement a non-async version of this
    # - We might standardize on a background thread (using an in-memory queue) for heartbeats
    #   in which case it doesn't matter whether it's an async method or not.
    async def heartbeat(self, details: object):
        await heartbeat(self.client, self.activity_task.task_token, details)

    def get_heartbeat_details(self) -> object:
        details: Payloads = self.activity_task.heartbeat_details
        if not self.activity_task.heartbeat_details:
            return None
        payloads: List[object] = self.client.data_converter.from_payloads(details)
        return payloads[0]

    def do_not_complete_on_return(self):
        self.do_not_complete = True


class Activity:

    @staticmethod
    def get_task_token() -> bytes:
        return ActivityContext.get().activity_task.task_token

    @staticmethod
    def get_workflow_execution() -> WorkflowExecution:
        return ActivityContext.get().activity_task.workflow_execution

    @staticmethod
    def get_namespace() -> str:
        return ActivityContext.get().namespace

    @staticmethod
    def get_heartbeat_details() -> object:
        return ActivityContext.get().get_heartbeat_details()

    @staticmethod
    async def heartbeat(details: object):
        await ActivityContext.get().heartbeat(details)

    @staticmethod
    def get_activity_task() -> ActivityTask:
        return ActivityContext.get().activity_task

    @staticmethod
    def do_not_complete_on_return():
        return ActivityContext.get().do_not_complete_on_return()


@dataclass
class ActivityCompletionClient:
    client: 'WorkflowClient'

    def heartbeat(self, task_token: bytes, details: object):
        heartbeat(self.client, task_token, details)

    async def complete(self, task_token: bytes, return_value: object):
        await complete(self.client, task_token, return_value)

    async def complete_exceptionally(self, task_token: bytes, ex: Exception):
        await complete_exceptionally(self.client, task_token, ex)


async def complete_exceptionally(client: 'WorkflowClient', task_token, ex: Exception):
    respond: RespondActivityTaskFailedRequest = RespondActivityTaskFailedRequest()
    respond.task_token = task_token
    respond.identity = get_identity()
    respond.failure = serialize_exception(ex)
    await client.service.respond_activity_task_failed(request=respond)


async def complete(client: 'WorkflowClient', task_token, return_value: object):
    respond = RespondActivityTaskCompletedRequest()
    respond.task_token = task_token
    respond.result = client.data_converter.to_payloads([return_value])
    respond.identity = get_identity()
    await client.service.respond_activity_task_completed(request=respond)


from temporal.workflow import WorkflowClient
