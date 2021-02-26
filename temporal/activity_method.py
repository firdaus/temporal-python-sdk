import inspect
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Callable, List

from temporal.api.common.v1 import RetryPolicy, ActivityType, Payloads



def get_activity_method_name(method: Callable):
    return "::".join(method.__qualname__.split(".")[-2:])


@dataclass
class RetryParameters:
    initial_interval: timedelta = None
    backoff_coefficient: float = None
    maximum_interval: timedelta = None
    maximum_attempts: int = None
    non_retryable_error_types: List[str] = field(default_factory=list)

    def to_retry_policy(self) -> RetryPolicy:
        policy = RetryPolicy()
        policy.initial_interval = self.initial_interval
        policy.backoff_coefficient = self.backoff_coefficient
        policy.maximum_interval = self.maximum_interval
        policy.maximum_attempts = self.maximum_attempts
        policy.non_retryable_error_types = self.non_retryable_error_types
        return policy


@dataclass
class ExecuteActivityParameters:
    fn: Callable = None
    activity_id: str = ""
    activity_type: ActivityType = None
    heartbeat_timeout: timedelta = None
    input: Payloads = None
    schedule_to_close_timeout: timedelta = None
    schedule_to_start_timeout: timedelta = None
    start_to_close_timeout: timedelta = None
    task_queue: str = ""
    retry_parameters: RetryParameters = None


def activity_method(func: Callable = None, name: str = "", schedule_to_close_timeout: timedelta = None,
                    schedule_to_start_timeout: timedelta = None, start_to_close_timeout: timedelta = None,
                    heartbeat_timeout: timedelta = None, task_queue: str = "", retry_parameters: RetryParameters = None):
    def wrapper(fn: Callable):
        # noinspection PyProtectedMember
        async def stub_activity_fn(self, *args):
            from .async_activity import Async
            from .decision_loop import ActivityFuture
            future: ActivityFuture = Async.function_with_self(stub_activity_fn, self, *args)
            return await future.wait_for_result()

        if not task_queue:
            raise Exception("task_queue parameter is mandatory")

        execute_parameters = ExecuteActivityParameters()
        execute_parameters.fn = fn
        execute_parameters.activity_type = ActivityType()
        execute_parameters.activity_type.name = name if name else get_activity_method_name(fn)
        execute_parameters.schedule_to_close_timeout = schedule_to_close_timeout
        execute_parameters.schedule_to_start_timeout = schedule_to_start_timeout
        execute_parameters.start_to_close_timeout = start_to_close_timeout
        execute_parameters.heartbeat_timeout = heartbeat_timeout
        execute_parameters.task_queue = task_queue
        execute_parameters.retry_parameters = retry_parameters
        # noinspection PyTypeHints
        stub_activity_fn._execute_parameters = execute_parameters  # type: ignore
        return stub_activity_fn

    if func and inspect.isfunction(func):
        raise Exception("activity_method must be called with arguments")
    else:
        return wrapper


@dataclass
class ActivityOptions:
    schedule_to_close_timeout: timedelta = None
    schedule_to_start_timeout: timedelta = None
    start_to_close_timeout: timedelta = None
    heartbeat_timeout: timedelta = None
    task_queue: str = None

    def fill_execute_activity_parameters(self, execute_parameters: ExecuteActivityParameters):
        if self.schedule_to_close_timeout is not None:
            execute_parameters.schedule_to_close_timeout = self.schedule_to_close_timeout
        if self.schedule_to_start_timeout is not None:
            execute_parameters.schedule_to_start_timeout = self.schedule_to_start_timeout
        if self.start_to_close_timeout is not None:
            execute_parameters.start_to_close_timeout = self.start_to_close_timeout
        if self.heartbeat_timeout is not None:
            execute_parameters.heartbeat_timeout = self.heartbeat_timeout
        if self.task_queue is not None:
            execute_parameters.task_queue = self.task_queue


@dataclass
class UntypedActivityStub:
    _decision_context: object = None
    _retry_parameters: RetryParameters = None
    _activity_options: ActivityOptions = None

    async def execute(self, activity_name: str, *args):
        f = await self.execute_async(activity_name, *args)
        return await f.wait_for_result()

    async def execute_async(self, activity_name: str, *args):
        from .async_activity import Async
        execute_parameters = ExecuteActivityParameters()
        execute_parameters.activity_type = ActivityType()
        execute_parameters.activity_type.name = activity_name
        return Async.call(self, execute_parameters, args)
