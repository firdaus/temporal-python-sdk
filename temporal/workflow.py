from __future__ import annotations
import datetime
import inspect
import json
import random
import uuid
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Callable, List, Type, Dict, Tuple, Any
from uuid import uuid4

from .activity_method import RetryParameters, ActivityOptions
from .api.common.v1 import WorkflowType, WorkflowExecution, SearchAttributes, Memo
from .api.enums.v1 import WorkflowIdReusePolicy, HistoryEventFilterType, EventType
from .api.history.v1 import WorkflowExecutionTerminatedEventAttributes
from .api.query.v1 import WorkflowQuery
from .api.taskqueue.v1 import TaskQueue
from .api.workflowservice.v1 import StartWorkflowExecutionRequest, GetWorkflowExecutionHistoryRequest, \
    QueryWorkflowRequest, QueryWorkflowResponse, SignalWorkflowExecutionRequest, WorkflowServiceStub

from .constants import DEFAULT_SOCKET_TIMEOUT_SECONDS
from .converter import DataConverter, DEFAULT_DATA_CONVERTER_INSTANCE, get_fn_ret_type_hints, get_fn_args_type_hints
from .errors import QueryFailedError
from .exception_handling import deserialize_exception
from .exceptions import WorkflowFailureException, ActivityFailureException, QueryRejectedException, \
    QueryFailureException, WorkflowOperationException
from .service_helpers import create_workflow_service, get_identity


class Workflow:

    @staticmethod
    def new_activity_stub(activities_cls, retry_parameters: RetryParameters = None, activity_options: ActivityOptions = None):
        from .decision_loop import ITask
        task: ITask = ITask.current()
        assert task
        cls = activities_cls()
        cls._decision_context = task.decider.decision_context
        cls._retry_parameters = retry_parameters  # type: ignore
        cls._activity_options = activity_options
        return cls

    @staticmethod
    def new_untyped_activity_stub(retry_parameters: RetryParameters = None,
                                  activity_options: ActivityOptions = None):
        from .decision_loop import ITask
        from .activity_method import UntypedActivityStub
        task: ITask = ITask.current()
        assert task
        cls = UntypedActivityStub()
        cls._decision_context = task.decider.decision_context
        cls._retry_parameters = retry_parameters  # type: ignore
        cls._activity_options = activity_options
        return cls

    @staticmethod
    async def await_till(c: Callable, timeout_seconds: int = 0) -> bool:
        from .decision_loop import ITask
        task: ITask = ITask.current()
        assert task
        return await task.await_till(c, timeout_seconds)

    @staticmethod
    async def sleep(seconds: int):
        from .decision_loop import ITask
        task: ITask = ITask.current()
        await task.decider.decision_context.schedule_timer(seconds)

    @staticmethod
    def current_time_millis() -> int:
        from .decision_loop import ITask
        task: ITask = ITask.current()
        return int(task.decider.decision_context.current_time_millis().timestamp() * 1000)

    @staticmethod
    def now() -> datetime.datetime:
        from .decision_loop import ITask
        task: ITask = ITask.current()
        return task.decider.decision_context.current_time_millis()

    @staticmethod
    def random_uuid() -> uuid.UUID:
        from .decision_loop import ITask
        task: ITask = ITask.current()
        return task.decider.decision_context.random_uuid()

    @staticmethod
    def new_random() -> random.Random:
        from .decision_loop import ITask
        task: ITask = ITask.current()
        return task.decider.decision_context.new_random()

    @staticmethod
    def get_version(change_id: str, min_supported: int, max_supported: int):
        from .decision_loop import ITask
        from .decision_loop import DecisionContext
        task: ITask = ITask.current()
        decision_context: DecisionContext = task.decider.decision_context
        return decision_context.get_version(change_id, min_supported, max_supported)

    @staticmethod
    def get_logger(name):
        from .decision_loop import ITask
        task: ITask = ITask.current()
        return task.decider.decision_context.get_logger(name)

    @staticmethod
    def get_workflow_id():
        from .decision_loop import ITask
        task: ITask = ITask.current()
        return task.decider.workflow_execution.workflow_id

    @staticmethod
    def get_run_id():
        from .decision_loop import ITask
        task: ITask = ITask.current()
        return task.decider.workflow_execution.run_id


class WorkflowStub:
    pass


@dataclass
class WorkflowExecutionContext:
    workflow_type: str
    workflow_execution: WorkflowExecution
    workflow_method_instance: WorkflowMethod


@dataclass
class WorkflowClient:
    service: WorkflowServiceStub
    namespace: str
    options: WorkflowClientOptions
    data_converter: DataConverter

    @classmethod
    def new_client(cls, host: str = "localhost", port: int = 7233, namespace: str = "",
                   options: WorkflowClientOptions = None, timeout: int = DEFAULT_SOCKET_TIMEOUT_SECONDS,
                   data_converter: DataConverter = DEFAULT_DATA_CONVERTER_INSTANCE) -> WorkflowClient:
        service = create_workflow_service(host, port, timeout=timeout)
        return cls(service=service, namespace=namespace, options=options, data_converter=data_converter)

    @classmethod
    async def start(cls, stub_fn: Callable, *args) -> WorkflowExecutionContext:
        stub = stub_fn.__self__  # type: ignore
        client = stub._workflow_client  # type: ignore
        assert client is not None
        method = stub_fn._workflow_method  # type: ignore
        assert method is not None
        options = stub._workflow_options  # type: ignore
        return await exec_workflow(client, method, args,
                             workflow_options=options, stub_instance=stub)

    def new_workflow_stub(self, cls: Type, workflow_options: WorkflowOptions = None):
        if not workflow_options:
            workflow_options = WorkflowOptions()
        attrs: Dict[str, Any] = {}
        attrs["_workflow_client"] = self
        attrs["_workflow_options"] = workflow_options
        for name, fn in inspect.getmembers(cls, inspect.isfunction):
            if hasattr(fn, "_workflow_method"):
                attrs[name] = get_workflow_stub_fn(fn._workflow_method)
            elif hasattr(fn, "_signal_method"):
                attrs[name] = get_signal_stub_fn(fn._signal_method)
            elif hasattr(fn, "_query_method"):
                attrs[name] = get_query_stub_fn(fn._query_method)
        stub_cls = type(cls.__name__, (WorkflowStub,), attrs)
        return stub_cls()

    def new_workflow_stub_from_workflow_id(self, cls: Type, workflow_id: str):
        """
        Use it to send signals or queries to a running workflow.
        Do not call workflow methods on it
        """
        stub_instance = self.new_workflow_stub(cls)
        execution = WorkflowExecution(workflow_id=workflow_id, run_id=None)
        stub_instance._execution = execution
        return stub_instance

    async def wait_for_close(self, context: WorkflowExecutionContext) -> object:
        return await self.wait_for_close_with_workflow_id(workflow_id=context.workflow_execution.workflow_id,
                                                    run_id=context.workflow_execution.run_id,
                                                    workflow_method_instance=context.workflow_method_instance)

    async def wait_for_close_with_workflow_id(self, workflow_id: str, run_id: str = None,
                                              workflow_method_instance: WorkflowMethod = None,
                                              workflow_fn: Callable = None):
        if not workflow_method_instance and workflow_fn:
            if not hasattr(workflow_fn, "_workflow_method"):
                raise Exception("workflow_fn is not a valid workflow stub function")
            workflow_method_instance = workflow_fn._workflow_method
        while True:
            history_request = create_close_history_event_request(self, workflow_id, run_id)
            history_response = await self.service.get_workflow_execution_history(request=history_request)
            if not history_response.history.events:
                continue
            history_event = history_response.history.events[0]
            if history_event.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
                completed_attributes = history_event.workflow_execution_completed_event_attributes
                type_hints = []
                if workflow_method_instance:
                    type_hints = workflow_method_instance._ret_type_hints
                payloads: List[object] = self.data_converter.from_payloads(completed_attributes.result,
                                                                           type_hints=type_hints)
                return payloads[0]
            elif history_event.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
                failed_attributes = history_event.workflow_execution_failed_event_attributes
                exception = deserialize_exception(failed_attributes.failure)
                raise exception
                """
                TODO: Do we need any of the logic here?
                if failed_attributes.reason == "WorkflowFailureException":
                    exception = deserialize_exception(failed_attributes.details)
                    if isinstance(exception, ActivityFailureException):
                        exception.set_cause()
                    workflow_execution = WorkflowExecution(workflow_id=workflow_id, run_id=run_id)
                    raise WorkflowFailureException(workflow_type=workflow_type,
                                                   execution=workflow_execution) from exception
                else:
                    details: Dict = json.loads(failed_attributes.details)
                    detail_message = details.get("detailMessage", "")
                    raise WorkflowExecutionFailedException(failed_attributes.reason, details=details,
                                                           detail_message=detail_message)
                """
            elif history_event.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
                raise WorkflowExecutionTimedOutException()
            elif history_event.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
                terminated_attributes  = history_event.workflow_execution_terminated_event_attributes
                raise WorkflowExecutionTerminatedException(reason=terminated_attributes.reason,
                                                           details=terminated_attributes.details,
                                                           identity=terminated_attributes.identity)
            elif history_event.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
                raise WorkflowExecutionCanceledException()
            else:
                raise Exception("Unexpected history close event: " + str(history_event))

    def new_activity_completion_client(self):
        return ActivityCompletionClient(self)

    def close(self):
        self.service.channel.close()


async def exec_workflow(workflow_client: WorkflowClient, wm: WorkflowMethod, args, workflow_options: WorkflowOptions = None,
                  stub_instance: object = None) -> WorkflowExecutionContext:
    start_request = create_start_workflow_request(workflow_client, wm, args, workflow_options)
    start_response = await workflow_client.service.start_workflow_execution(request=start_request)
    execution = WorkflowExecution(workflow_id=start_request.workflow_id, run_id=start_response.run_id)
    stub_instance._execution = execution  # type: ignore
    return WorkflowExecutionContext(workflow_type=wm._name, workflow_execution=execution, workflow_method_instance=wm)


async def exec_workflow_sync(workflow_client: WorkflowClient, wm: WorkflowMethod, args: List,
                       workflow_options: WorkflowOptions = None, stub_instance: object = None):
    execution_context: WorkflowExecutionContext = await exec_workflow(workflow_client, wm, args,
                                                                workflow_options=workflow_options,
                                                                stub_instance=stub_instance)
    return await workflow_client.wait_for_close(execution_context)


async def exec_signal(workflow_client: WorkflowClient, sm: SignalMethod, args, stub_instance: object = None):
    assert stub_instance._execution  # type: ignore
    request = SignalWorkflowExecutionRequest()
    request.workflow_execution = stub_instance._execution  # type: ignore
    request.signal_name = sm.name
    request.input = workflow_client.data_converter.to_payloads(args)
    request.namespace = workflow_client.namespace
    response = await workflow_client.service.signal_workflow_execution(request=request)


async def exec_query(workflow_client: WorkflowClient, qm: QueryMethod, args, stub_instance: object = None):
    assert stub_instance._execution  # type: ignore
    request = QueryWorkflowRequest()
    request.execution = stub_instance._execution  # type: ignore
    request.query = WorkflowQuery()
    request.query.query_type = qm.name
    request.query.query_args = workflow_client.data_converter.to_payloads(args)
    request.namespace = workflow_client.namespace
    response: QueryWorkflowResponse = await workflow_client.service.query_workflow(request=request)
    """
    TODO: Do we need to bring back any of this error handling
    if err:
        if isinstance(err, QueryFailedError):
            cause = deserialize_exception(err.message)
            raise QueryFailureException(query_type=qm.name, execution=stub_instance._execution) from cause
        elif isinstance(err, Exception):
            raise err
        else:
            raise Exception(err)
    """
    if response.query_rejected:
        raise QueryRejectedException(response.query_rejected.status)
    return workflow_client.data_converter.from_payloads(response.query_result, qm.ret_type_hints)[0]


def create_memo(m: Dict[str, object], data_converter: DataConverter) -> Memo:
    memo = Memo();
    for k, v in m.items():
        memo.fields[k] = data_converter.to_payload(v)
    return memo


def create_search_attributes(s: Dict[str, object], data_converter: DataConverter) -> SearchAttributes:
    search_attributes = SearchAttributes()
    for k, v in s.items():
        search_attributes.indexed_fields[k] = data_converter.to_payload(v)
    return search_attributes


def create_start_workflow_request(workflow_client: WorkflowClient, wm: WorkflowMethod,
                                  args: List, workflow_options: WorkflowOptions = None) -> StartWorkflowExecutionRequest:
    start_request = StartWorkflowExecutionRequest()
    start_request.namespace = workflow_client.namespace
    start_request.workflow_id = wm._workflow_id if wm._workflow_id else str(uuid4())
    start_request.workflow_type = WorkflowType()
    start_request.workflow_type.name = wm._name
    start_request.task_queue = TaskQueue()
    start_request.task_queue.name = wm._task_queue
    start_request.input = workflow_client.data_converter.to_payloads(args)

    start_request.workflow_execution_timeout = wm._workflow_execution_timeout
    start_request.workflow_run_timeout = wm._workflow_run_timeout
    start_request.workflow_task_timeout = wm._workflow_task_timeout

    start_request.identity = get_identity()
    start_request.workflow_id_reuse_policy = wm._workflow_id_reuse_policy
    start_request.request_id = str(uuid4())
    start_request.cron_schedule = wm._cron_schedule if wm._cron_schedule else None

    if wm._memo:
        start_request.memo = create_memo(wm._memo, workflow_client.data_converter)
    if wm._search_attributes:
        start_request.search_attributes = create_search_attributes(wm._search_attributes,
                                                                   workflow_client.data_converter)

    if workflow_options:
        if workflow_options.workflow_id:
            start_request.workflow_id = workflow_options.workflow_id
        if workflow_options.workflow_id_reuse_policy:
            start_request.workflow_id_reuse_policy = workflow_options.workflow_id_reuse_policy
        if workflow_options.workflow_run_timeout:
            start_request.workflow_run_timeout = workflow_options.workflow_run_timeout
        if workflow_options.workflow_execution_timeout:
            start_request.workflow_execution_timeout = workflow_options.workflow_execution_timeout
        if workflow_options.workflow_task_timeout:
            start_request.workflow_task_timeout = workflow_options.workflow_task_timeout
        if workflow_options.task_queue:
            start_request.task_queue = workflow_options.task_queue
        if workflow_options.cron_schedule:
            start_request.cron_schedule = workflow_options.cron_schedule
        if workflow_options.memo:
            start_request.memo = create_memo(workflow_options.memo, workflow_client.data_converter)
        if workflow_options.search_attributes:
            start_request.search_attributes = create_search_attributes(workflow_options.search_attributes,
                                                                       workflow_client.data_converter)

    return start_request


def create_close_history_event_request(workflow_client: WorkflowClient, workflow_id: str,
                                       run_id: str) -> GetWorkflowExecutionHistoryRequest:
    history_request = GetWorkflowExecutionHistoryRequest()
    history_request.namespace = workflow_client.namespace
    history_request.execution = WorkflowExecution()
    history_request.execution.workflow_id = workflow_id
    history_request.execution.run_id = run_id
    history_request.wait_for_new_event = True
    history_request.history_event_filter_type = HistoryEventFilterType.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT
    return history_request


def get_workflow_method_name(method):
    return "::".join(method.__qualname__.split(".")[-2:])


def get_workflow_stub_fn(wm: WorkflowMethod):
    async def workflow_stub_fn(self, *args):
        assert self._workflow_client is not None
        return await exec_workflow_sync(self._workflow_client, wm, args,
                                  workflow_options=self._workflow_options, stub_instance=self)

    workflow_stub_fn._workflow_method = wm  # type: ignore
    return workflow_stub_fn


def get_signal_stub_fn(sm: SignalMethod):
    async def signal_stub_fn(self, *args):
        assert self._workflow_client is not None
        return await exec_signal(self._workflow_client, sm, args, stub_instance=self)

    signal_stub_fn._signal_method = sm  # type: ignore
    return signal_stub_fn


def get_query_stub_fn(qm: QueryMethod):
    async def query_stub_fn(self, *args):
        assert self._workflow_client is not None
        return await exec_query(self._workflow_client, qm, args, stub_instance=self)

    query_stub_fn._query_method = qm  # type: ignore
    return query_stub_fn


@dataclass
class WorkflowMethod(object):
    _name: str = None
    _workflow_id: str = None
    _workflow_id_reuse_policy: WorkflowIdReusePolicy = None
    _task_queue: str = None
    _cron_schedule: str = None
    _workflow_execution_timeout: timedelta = None
    _workflow_run_timeout: timedelta = None
    _workflow_task_timeout: timedelta = None
    _memo: Dict[str, object] = None
    _search_attributes: Dict[str, object] = None
    _ret_type_hints: List[type] = None


def workflow_method(func=None,
                    name=None,
                    workflow_id=None,
                    workflow_id_reuse_policy=WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
                    workflow_execution_timeout=timedelta(seconds=7200),  # (2 hours)
                    workflow_run_timeout=timedelta(seconds=7200),  # 2 hours
                    workflow_task_timeout=timedelta(seconds=60),
                    task_queue=None,
                    memo: Dict[str, object] = None,
                    search_attributes: Dict[str, object] = None):
    def wrapper(fn):
        if not hasattr(fn, "_workflow_method"):
            fn._workflow_method = WorkflowMethod()
        fn._workflow_method._name = name if name else get_workflow_method_name(fn)
        fn._workflow_method._workflow_id = workflow_id
        fn._workflow_method._workflow_id_reuse_policy = workflow_id_reuse_policy
        fn._workflow_method._task_queue = task_queue
        fn._workflow_method._workflow_execution_timeout = workflow_execution_timeout
        fn._workflow_method._workflow_run_timeout = workflow_run_timeout
        fn._workflow_method._workflow_task_timeout = workflow_task_timeout
        fn._workflow_method._memo = memo
        fn._workflow_method._search_attributes = search_attributes
        fn._workflow_method._ret_type_hints = get_fn_ret_type_hints(fn)
        return fn

    if func and inspect.isfunction(func):
        return wrapper(func)
    else:
        return wrapper


@dataclass
class QueryMethod:
    name: str = None
    ret_type_hints: List[type] = None


def query_method(func=None, name: str = None):
    def wrapper(fn):
        fn._query_method = QueryMethod()
        fn._query_method.name = name if name else get_workflow_method_name(fn)
        fn._query_method.ret_type_hints = get_fn_ret_type_hints(fn)
        return fn

    if func and inspect.isfunction(func):
        return wrapper(func)
    else:
        return wrapper


@dataclass
class SignalMethod:
    name: str = None


def signal_method(func=None, name: str = None):
    def wrapper(fn):
        fn._signal_method = SignalMethod()
        fn._signal_method.name = name if name else get_workflow_method_name(fn)
        return fn

    if func and inspect.isfunction(func):
        return wrapper(func)
    else:
        return wrapper


def cron_schedule(value):
    def wrapper(fn):
        if not hasattr(fn, "_workflow_method"):
            fn._workflow_method = WorkflowMethod()
        fn._workflow_method._cron_schedule = value
        return fn

    return wrapper


@dataclass
class WorkflowClientOptions:
    pass


@dataclass
class RetryOptions:
    # TODO: Support for retry_options
    pass


@dataclass
class ContextPropagator:
    # TODO: Support for ContextPropagator
    pass


@dataclass
class WorkflowOptions:
    workflow_id: str = None
    workflow_id_reuse_policy: WorkflowIdReusePolicy = None
    workflow_run_timeout: timedelta = None
    workflow_execution_timeout: timedelta = None
    workflow_task_timeout: timedelta = None
    task_queue: str = None
    retry_options: RetryOptions = None
    cron_schedule: str = None
    memo: Dict[str, object] = None
    search_attributes: Dict[str, object] = None
    # TODO: What is this for?
    # context_propagators = List[ContextPropagator] = None


@dataclass
class WorkflowExecutionFailedException(Exception):
    reason: str
    details: Dict[str, Any]
    detail_message: str

    def __str__(self) -> str:
        cause = self.details.get("cause")
        if isinstance(cause, dict):
            return f"{cause['class']}: {cause['detailMessage']}"
        else:
            return f"{self.reason}: {self.detail_message}"


@dataclass
class WorkflowExecutionTimedOutException(Exception):
    pass


@dataclass
class WorkflowExecutionCanceledException(Exception):
    pass


@dataclass
class WorkflowExecutionTerminatedException(Exception):
    reason: str
    details: object
    identity: str

    def __str__(self) -> str:
        return self.reason


from .activity import ActivityCompletionClient
