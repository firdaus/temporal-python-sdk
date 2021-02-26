from __future__ import annotations

import asyncio
import contextvars
import datetime
import json
import socket
import traceback
import uuid
import random
import logging
import threading
from asyncio import CancelledError
from asyncio.events import AbstractEventLoop
from asyncio.futures import Future
from asyncio.tasks import Task
from collections import OrderedDict
from contextvars import ContextVar
from dataclasses import dataclass, field
from enum import Enum
from time import mktime
from typing import List, Dict, Optional, Any, Callable

from grpclib import GRPCError
from more_itertools import peekable  # type: ignore

from .activity_method import ExecuteActivityParameters
from .api.command.v1 import Command, CompleteWorkflowExecutionCommandAttributes, RecordMarkerCommandAttributes, \
    FailWorkflowExecutionCommandAttributes, ScheduleActivityTaskCommandAttributes, \
    CancelWorkflowExecutionCommandAttributes, StartTimerCommandAttributes
from .api.common.v1 import WorkflowType, Header, Payloads, WorkflowExecution
from .api.enums.v1 import EventType, CommandType, QueryResultType, WorkflowTaskFailedCause
from .api.history.v1 import HistoryEvent, TimerFiredEventAttributes, ActivityTaskTimedOutEventAttributes
from .api.query.v1 import WorkflowQuery
from .api.taskqueue.v1 import TaskQueue
from .api.workflowservice.v1 import PollWorkflowTaskQueueRequest, PollWorkflowTaskQueueResponse, \
    RespondWorkflowTaskCompletedRequest, RespondWorkflowTaskCompletedResponse, RespondQueryTaskCompletedRequest, \
    QueryWorkflowResponse, QueryWorkflowRequest, WorkflowServiceStub, RespondQueryTaskCompletedResponse, \
    GetWorkflowExecutionHistoryRequest
from .converter import get_fn_ret_type_hints, get_fn_args_type_hints, DataConverter

from .decisions import DecisionId, DecisionTarget
from .exception_handling import serialize_exception, deserialize_exception, failure_to_str
from .exceptions import WorkflowTypeNotFound, NonDeterministicWorkflowException, ActivityTaskFailedException, \
    ActivityTaskTimeoutException, SignalNotFound, ActivityFailureException, QueryNotFound, QueryDidNotComplete
from .retry import retry
from .service_helpers import get_identity, create_workflow_service
from .state_machines import ActivityDecisionStateMachine, DecisionStateMachine, CompleteWorkflowStateMachine, \
    TimerDecisionStateMachine, MarkerDecisionStateMachine
from .worker import Worker, StopRequestedException

logger = logging.getLogger(__name__)


# TODO: Consult implementation in the Java library
def is_decision_event(event: HistoryEvent) -> bool:
    decision_event_types = (EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
                            EventType.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
                            EventType.EVENT_TYPE_TIMER_STARTED,
                            EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
                            EventType.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
                            EventType.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
                            EventType.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
                            EventType.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED,
                            # EventType.RequestCancelActivityTaskFailed,
                            EventType.EVENT_TYPE_TIMER_CANCELED,
                            # EventType.CancelTimerFailed,
                            EventType.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
                            EventType.EVENT_TYPE_MARKER_RECORDED,
                            EventType.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED)
    return event.event_type in decision_event_types


def nano_to_milli(nano):
    return nano/(1000 * 1000)


class HistoryHelper:

    def __init__(self, events: List[HistoryEvent]):
        self.events = peekable(events)

    def has_next(self) -> bool:
        try:
            self.events.peek()
            return True
        except StopIteration:
            return False

    def next(self) -> Optional[DecisionEvents]:
        events = self.events
        if not self.has_next():
            return None
        decision_events: List[HistoryEvent] = []
        new_events: List[HistoryEvent] = []
        replay = True
        next_decision_event_id = -1
        # noinspection PyUnusedLocal
        event: HistoryEvent
        for event in events:
            event_type = event.event_type
            if event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED or not self.has_next():
                replay_current_time_milliseconds = event.event_time
                if not self.has_next():
                    replay = False
                    next_decision_event_id = event.event_id + 2
                    break
                peeked: HistoryEvent = events.peek()
                peeked_type = peeked.event_type
                if peeked_type == EventType.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT or peeked_type == \
                        EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED:
                    continue
                elif peeked_type == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
                    next(events)
                    next_decision_event_id = peeked.event_id + 1
                    break
                else:
                    raise Exception(
                        "Unexpected event after DecisionTaskStarted: {}".format(peeked))
            new_events.append(event)
        while self.has_next():
            if not is_decision_event(events.peek()):
                break
            decision_events.append(next(events))
        result = DecisionEvents(new_events, decision_events, replay,
                                replay_current_time_milliseconds, next_decision_event_id)
        logger.debug("HistoryHelper next=%s", result)
        return result


@dataclass
class DecisionEvents:
    events: List[HistoryEvent]
    decision_events: List[HistoryEvent]
    replay: bool
    replay_current_time_milliseconds: datetime.datetime
    next_decision_event_id: int
    markers: List[HistoryEvent] = field(default_factory=list)

    def __post_init__(self):
        for event in self.decision_events:
            if event.event_type == EventType.EVENT_TYPE_MARKER_RECORDED:
                self.markers.append(event)

    def get_optional_decision_event(self, event_id) -> HistoryEvent:
        index = event_id - self.next_decision_event_id
        if index < 0 or index >= len(self.decision_events):
            return None
        else:
            return self.decision_events[index]


class Status(Enum):
    CREATED = 1
    RUNNING = 2
    DONE = 3


current_task: contextvars.ContextVar = contextvars.ContextVar("current_task")


@dataclass
class ITask:
    decider: ReplayDecider = None
    task: Task = None
    status: Status = Status.CREATED
    awaited: Future = None

    def is_done(self):
        return self.status == Status.DONE

    def destroy(self):
        if self.status == Status.RUNNING:
            self.status = Status.DONE
            self.task.cancel()

    def start(self):
        pass

    async def await_till(self, c: Callable, timeout_seconds: int = 0) -> bool:
        timer_cancellation_handler: TimerCancellationHandler = None
        timer_fired = False

        def timer_callback(ex: Exception):
            nonlocal timer_fired
            if not ex:
                timer_fired = True

        if timeout_seconds:
            timer_cancellation_handler = self.decider.decision_context.create_timer(delay_seconds=timeout_seconds, callback=timer_callback)

        while not c() and not timer_fired:
            self.awaited = self.decider.event_loop.create_future()
            await self.awaited
            assert self.awaited.done()

        self.awaited = None

        if timer_fired:
            return False

        if timer_cancellation_handler:
            timer_cancellation_handler.accept(None)

        return True

    def unblock(self):
        if self.awaited:
            self.awaited.set_result(None)

    @staticmethod
    def current() -> ITask:
        return current_task.get()


@dataclass
class WorkflowMethodTask(ITask):
    task_id: str = None
    workflow_input: Payloads = None
    worker: Worker = None
    workflow_type: WorkflowType = None
    workflow_instance: object = None
    ret_value: object = None
    data_converter: DataConverter = None

    def __post_init__(self):
        logger.debug(f"[task-{self.task_id}] Created")
        self.task = asyncio.get_event_loop().create_task(self.init_workflow_instance())

    async def init_workflow_instance(self):
        current_task.set(self)
        cls, _ = self.worker.get_workflow_method(self.workflow_type.name)
        try:
            self.workflow_instance = cls()
            self.task = asyncio.get_event_loop().create_task(self.workflow_main())
        except Exception as ex:
            logger.error(
                f"Initialization of Workflow {self.workflow_type.name} failed", exc_info=1)
            self.decider.fail_workflow_execution(ex)
            self.status = Status.DONE

    async def workflow_main(self):
        logger.debug(f"[task-{self.task_id}] Running")

        if self.is_done():
            return

        current_task.set(self)

        if self.workflow_type.name not in self.worker.workflow_methods:
            self.status = Status.DONE
            ex = WorkflowTypeNotFound(self.workflow_type.name)
            logger.error(f"Workflow type not found: {self.workflow_type.name}")
            self.decider.fail_workflow_execution(ex)
            return

        cls, workflow_proc = self.worker.get_workflow_method(self.workflow_type.name)
        if self.workflow_input is None:
            workflow_input = []
        else:
            type_hints = get_fn_args_type_hints(workflow_proc)
            # We need to pop because ismethod(workflow_proc) is False because it wasn't taken from an instance
            type_hints.pop(0)
            workflow_input = self.data_converter.from_payloads(self.workflow_input, type_hints)

        self.status = Status.RUNNING
        try:
            logger.info(f"Invoking workflow {self.workflow_type.name}({str(workflow_input)[1:-1]})")
            self.ret_value = await workflow_proc(self.workflow_instance, *workflow_input)
            logger.info(
                f"Workflow {self.workflow_type.name}({str(workflow_input)[1:-1]}) returned {self.ret_value}")
            self.decider.complete_workflow_execution(self.ret_value)
        except CancelledError:
            logger.debug("Coroutine cancelled (expected)")
        except Exception as ex:
            logger.error(
                f"Workflow {self.workflow_type.name}({str(workflow_input)[1:-1]}) failed", exc_info=1)
            self.decider.fail_workflow_execution(ex)
        finally:
            self.status = Status.DONE

    def get_workflow_instance(self):
        return self.workflow_instance


@dataclass
class QueryMethodTask(ITask):
    task_id: str = None
    workflow_instance: object = None
    query_name: str = None
    query_input: Payloads = None
    exception_thrown: BaseException = None
    ret_value: object = None
    data_converter: DataConverter = None

    def start(self):
        logger.debug(f"[query-task-{self.task_id}-{self.query_name}] Created")
        self.task = asyncio.get_event_loop().create_task(self.query_main())

    async def query_main(self):
        logger.debug(f"[query-task-{self.task_id}-{self.query_name}] Running")
        current_task.set(self)

        if not self.query_name in self.workflow_instance._query_methods:
            self.status = Status.DONE
            self.exception_thrown = QueryNotFound(self.query_name)
            logger.error(f"Query not found: {self.query_name}")
            return

        query_proc = self.workflow_instance._query_methods[self.query_name]
        self.status = Status.RUNNING

        try:
            logger.info(f"Invoking query {self.query_name}")
            if not self.query_input:
                query_input = []
            else:
                hints = get_fn_args_type_hints(query_proc)
                # We need to pop because ismethod(query_proc) is False because it wasn't taken from an instance
                hints.pop(0)
                query_input = self.data_converter.from_payloads(self.query_input, hints)
            self.ret_value = await query_proc(self.workflow_instance, *query_input)
            logger.info(
                f"Query {self.query_name} returned {self.ret_value}")
        except CancelledError:
            logger.debug("Coroutine cancelled (expected)")
        except Exception as ex:
            logger.error(
                f"Query {self.query_name} failed", exc_info=1)
            self.exception_thrown = ex
        finally:
            self.status = Status.DONE


@dataclass
class SignalMethodTask(ITask):
    task_id: str = None
    workflow_instance: object = None
    signal_name: str = None
    signal_input: Payloads = None
    exception_thrown: BaseException = None
    ret_value: object = None
    data_converter: DataConverter = None

    def start(self):
        logger.debug(f"[signal-task-{self.task_id}-{self.signal_name}] Created")
        self.task = asyncio.get_event_loop().create_task(self.signal_main())

    async def signal_main(self):
        logger.debug(f"[signal-task-{self.task_id}-{self.signal_name}] Running")
        current_task.set(self)

        if not self.signal_name in self.workflow_instance._signal_methods:
            self.status = Status.DONE
            self.exception_thrown = SignalNotFound(self.signal_name)
            logger.error(f"Signal not found: {self.signal_name}")
            return

        signal_proc = self.workflow_instance._signal_methods[self.signal_name]
        self.status = Status.RUNNING

        try:
            logger.info(f"Invoking signal {self.signal_name}")
            if self.signal_input is None:
                signal_input = []
            else:
                hints = get_fn_args_type_hints(signal_proc)
                # We need to pop because ismethod(signal_proc) is False because it wasn't taken from an instance
                hints.pop(0)
                signal_input = self.data_converter.from_payloads(self.signal_input, hints)
            self.ret_value = await signal_proc(self.workflow_instance, *signal_input)
            logger.info(
                f"Signal {self.signal_name} returned {self.ret_value}")
            self.decider.complete_signal_execution(self)
        except CancelledError:
            logger.debug("Coroutine cancelled (expected)")
        except Exception as ex:
            logger.error(
                f"Signal {self.signal_name} failed", exc_info=1)
            self.exception_thrown = ex
        finally:
            self.status = Status.DONE


@dataclass
class EventLoopWrapper:
    event_loop: AbstractEventLoop = None

    def __post_init__(self):
        self.event_loop = asyncio.get_event_loop()

    async def run_event_loop_once(self):
        await asyncio.sleep(0)

    def create_future(self) -> Future[Any]:
        return self.event_loop.create_future()


class ActivityFuture:

    def __init__(self, parameters: ExecuteActivityParameters,
                 scheduled_event_id: int, future: Future[object]):
        self.parameters = parameters
        self.scheduled_event_id = scheduled_event_id
        self.future = future

    def __await__(self):
        return self.future.__await__()

    def done(self):
        return self.future.done()

    def exception(self):
        return self.future.exception()

    async def wait_for_result(self) -> object:
        try:
            await self.future
        except CancelledError as e:
            logger.debug("Coroutine cancelled (expected)")
            raise e
        except Exception as ex:
            pass
        return self.get_result()

    def get_result(self):
        ex1 = self.future.exception()
        if ex1:
            # We are relying on this behavior to serialize Exceptions:
            # e = Exception("1", "2", "3")
            # assert e.args == ('1', '2', '3')
            activity_failure = ActivityFailureException(self.scheduled_event_id,
                                                        self.parameters.activity_type.name,
                                                        self.parameters.activity_id,
                                                        failure_to_str(serialize_exception(ex1)))
            raise activity_failure
        assert self.future.done()
        data_converter, raw_result = self.future.result()
        if self.parameters.fn:
            hints = get_fn_ret_type_hints(self.parameters.fn)
        else:
            # Untyped activity stubs won't have "fn"
            hints = []
        return data_converter.from_payloads(raw_result, hints)[0]


@dataclass
class DecisionContext:
    decider: ReplayDecider
    scheduled_activities: Dict[int, Future[object]] = field(default_factory=dict)
    workflow_clock: ClockDecisionContext = None
    current_run_id: str = None

    def __post_init__(self):
        if not self.workflow_clock:
            self.workflow_clock = ClockDecisionContext(self.decider, self)

    def schedule_activity_task(self, parameters: ExecuteActivityParameters):
        attr = ScheduleActivityTaskCommandAttributes()
        attr.activity_type = parameters.activity_type
        attr.input = parameters.input
        if parameters.heartbeat_timeout:
            attr.heartbeat_timeout = parameters.heartbeat_timeout
        attr.schedule_to_close_timeout = parameters.schedule_to_close_timeout
        attr.schedule_to_start_timeout = parameters.schedule_to_start_timeout
        attr.start_to_close_timeout = parameters.start_to_close_timeout
        attr.activity_id = parameters.activity_id
        if not attr.activity_id:
            attr.activity_id = self.decider.get_and_increment_next_id()
        attr.task_queue = TaskQueue()
        attr.task_queue.name = parameters.task_queue

        if parameters.retry_parameters:
            attr.retry_policy = parameters.retry_parameters.to_retry_policy()

        scheduled_event_id = self.decider.schedule_activity_task(schedule=attr)
        future = self.decider.event_loop.create_future()
        self.scheduled_activities[scheduled_event_id] = future
        return ActivityFuture(parameters=parameters, scheduled_event_id=scheduled_event_id,
                              future=future)

    async def schedule_timer(self, seconds: int):
        future = self.decider.event_loop.create_future()

        def callback(ex: Exception):
            nonlocal future
            if ex:
                future.set_exception(ex)
            else:
                future.set_result("time-fired")

        self.decider.decision_context.create_timer(delay_seconds=seconds, callback=callback)
        await future
        assert future.done()
        exception = future.exception()
        if exception:
            raise exception
        return

    def handle_activity_task_completed(self, event: HistoryEvent):
        attr = event.activity_task_completed_event_attributes
        if self.decider.handle_activity_task_closed(attr.scheduled_event_id):
            future = self.scheduled_activities.get(attr.scheduled_event_id)
            if future:
                self.scheduled_activities.pop(attr.scheduled_event_id)
                future.set_result((self.decider.worker.client.data_converter, attr.result))
            else:
                raise NonDeterministicWorkflowException(
                    f"Trying to complete activity event {attr.scheduled_event_id} that is not in scheduled_activities")

    def handle_activity_task_failed(self, event: HistoryEvent):
        attr = event.activity_task_failed_event_attributes
        if self.decider.handle_activity_task_closed(attr.scheduled_event_id):
            future = self.scheduled_activities.get(attr.scheduled_event_id)
            if future:
                self.scheduled_activities.pop(attr.scheduled_event_id)
                # TODO: attr.reason - what should we do with it?
                ex = deserialize_exception(attr.failure)
                future.set_exception(ex)
            else:
                raise NonDeterministicWorkflowException(
                    f"Trying to complete activity event {attr.scheduled_event_id} that is not in scheduled_activities")

    def handle_activity_task_timed_out(self, event: HistoryEvent):
        attr: ActivityTaskTimedOutEventAttributes = event.activity_task_timed_out_event_attributes
        if self.decider.handle_activity_task_closed(attr.scheduled_event_id):
            future = self.scheduled_activities.get(attr.scheduled_event_id)
            if future:
                self.scheduled_activities.pop(attr.scheduled_event_id)
                # TODO: What is the replacement for timeout_type and details
                # ex = ActivityTaskTimeoutException(event.event_id, attr.timeout_type, attr.details)
                ex = ActivityTaskTimeoutException(event.event_id, None, None)
                future.set_exception(ex)
            else:
                raise NonDeterministicWorkflowException(
                    f"Trying to complete activity event {attr.scheduled_event_id} that is not in scheduled_activities")

    def create_timer(self, delay_seconds: int, callback: Callable):
        return self.workflow_clock.create_timer(delay_seconds, callback)

    def set_replay_current_time_milliseconds(self, replay_current_time_milliseconds: datetime.datetime):
        if replay_current_time_milliseconds < self.workflow_clock.current_time_millis():
            raise Exception("workflow clock moved back")
        self.workflow_clock.set_replay_current_time_milliseconds(replay_current_time_milliseconds)

    def current_time_millis(self) -> datetime.datetime:
        return self.workflow_clock.current_time_millis()

    def set_replaying(self, replaying: bool):
        self.workflow_clock.set_replaying(replaying)

    def is_replaying(self):
        return self.workflow_clock.is_replaying()

    def handle_timer_fired(self, attributes: TimerFiredEventAttributes):
        self.workflow_clock.handle_timer_fired(attributes)

    def handle_timer_canceled(self, event: HistoryEvent):
        self.workflow_clock.handle_timer_canceled(event)

    def set_current_run_id(self, run_id: str):
        self.current_run_id = run_id

    def random_uuid(self) -> uuid.UUID:
        return uuid.uuid3(uuid.UUID(self.current_run_id), str(self.decider.get_and_increment_next_id()))

    def new_random(self) -> random.Random:
        random_uuid = self.random_uuid()
        lsb = random_uuid.bytes[:8]
        generator = random.Random()
        generator.seed(lsb, version=2)
        return generator

    def record_marker(self, marker_name: str, header: Header, details: Dict[str, Payloads]) -> None:
        marker = RecordMarkerCommandAttributes()
        marker.marker_name = marker_name
        marker.header = header
        marker.details = details
        decision: Command = Command()
        decision.command_type = CommandType.COMMAND_TYPE_RECORD_MARKER
        decision.record_marker_command_attributes = marker
        next_decision_event_id = self.decider.next_decision_event_id
        decision_id = DecisionId(DecisionTarget.MARKER, next_decision_event_id)
        self.decider.add_decision(decision_id, MarkerDecisionStateMachine(id=decision_id, decision=decision))

    def get_version(self, change_id: str, min_supported: int, max_supported: int) -> int:
        return self.workflow_clock.get_version(change_id, min_supported, max_supported)

    def get_logger(self, name) -> logging.Logger:
        replay_aware_logger = logging.getLogger(name)
        make_replay_aware(replay_aware_logger)
        return replay_aware_logger


@dataclass
class ReplayDecider:
    execution_id: str
    workflow_type: WorkflowType
    worker: Worker
    workflow_task: WorkflowMethodTask = None
    tasks: List[ITask] = field(default_factory=list)
    event_loop: EventLoopWrapper = field(default_factory=EventLoopWrapper)
    completed: bool = False

    next_decision_event_id: int = 0
    id_counter: int = 0
    decision_events: DecisionEvents = None
    decisions: OrderedDict[DecisionId, DecisionStateMachine] = field(default_factory=OrderedDict)
    decision_context: DecisionContext = None
    workflow_execution: WorkflowExecution = None

    activity_id_to_scheduled_event_id: Dict[str, int] = field(default_factory=dict)

    def __post_init__(self):
        self.decision_context = DecisionContext(decider=self)

    async def decide(self, events: List[HistoryEvent]):
        helper = HistoryHelper(events)
        while helper.has_next():
            decision_events = helper.next()
            await self.process_decision_events(decision_events)
        return self.get_decisions()

    async def process_decision_events(self, decision_events: DecisionEvents):
        self.decision_context.set_replaying(decision_events.replay)
        self.decision_context.set_replay_current_time_milliseconds(decision_events.replay_current_time_milliseconds)

        self.handle_decision_task_started(decision_events)
        for event in decision_events.markers:
            if not event.marker_recorded_event_attributes.marker_name == LOCAL_ACTIVITY_MARKER_NAME:
                await self.process_event(event);
        for event in decision_events.events:
            await self.process_event(event)
        if self.completed:
            return
        await self.unblock_all()
        if decision_events.replay:
            self.notify_decision_sent()
        for event in decision_events.decision_events:
            await self.process_event(event)

    async def unblock_all(self):
        workflow_task: WorkflowMethodTask = None
        for t in self.tasks:
            if isinstance(t, SignalMethodTask):
                t.unblock()
                await self.event_loop.run_event_loop_once()
            elif isinstance(t, WorkflowMethodTask):
                workflow_task = t
        workflow_task.unblock()
        await self.event_loop.run_event_loop_once()

    async def process_event(self, event: HistoryEvent):
        event_handler = event_handlers.get(event.event_type)
        if not event_handler:
            raise Exception(f"No event handler for event type {event.event_type.name}")
        await event_handler(self, event)  # type: ignore

    async def handle_workflow_execution_started(self, event: HistoryEvent):
        start_event_attributes = event.workflow_execution_started_event_attributes
        self.decision_context.set_current_run_id(start_event_attributes.original_execution_run_id)

        self.workflow_task = WorkflowMethodTask(task_id=self.execution_id, workflow_input=start_event_attributes.input,
                                                worker=self.worker, workflow_type=self.workflow_type, decider=self,
                                                data_converter=self.worker.client.data_converter)
        await self.event_loop.run_event_loop_once()
        assert self.workflow_task.workflow_instance
        self.tasks.append(self.workflow_task)

    async def handle_workflow_execution_cancel_requested(self, event: HistoryEvent):
        self.cancel_workflow_execution()

    def notify_decision_sent(self):
        for state_machine in self.decisions.values():
            if state_machine.get_decision():
                state_machine.handle_decision_task_started_event()

    def handle_decision_task_started(self, decision_events: DecisionEvents):
        self.decision_events = decision_events
        self.next_decision_event_id = decision_events.next_decision_event_id

    def complete_workflow_execution(self, ret_value: object) -> None:
        # PORT: addAllMissingVersionMarker(false, Optional.empty());
        decision: Command = Command()
        attr: CompleteWorkflowExecutionCommandAttributes = CompleteWorkflowExecutionCommandAttributes()
        attr.result = self.worker.client.data_converter.to_payloads([ret_value])
        decision.complete_workflow_execution_command_attributes = attr
        decision.command_type = CommandType.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION
        decision_id = DecisionId(DecisionTarget.SELF, 0)
        self.add_decision(decision_id, CompleteWorkflowStateMachine(decision_id, decision))
        self.completed = True

    def fail_workflow_execution(self, exception) -> None:
        # PORT: addAllMissingVersionMarker(false, Optional.empty());
        decision = Command()
        fail_attributes = FailWorkflowExecutionCommandAttributes()
        fail_attributes.failure = serialize_exception(exception)
        decision.fail_workflow_execution_command_attributes = fail_attributes
        decision.command_type = CommandType.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION
        decision_id = DecisionId(DecisionTarget.SELF, 0)
        self.add_decision(decision_id, CompleteWorkflowStateMachine(decision_id, decision))
        self.completed = True

    def cancel_workflow_execution(self):
        logger.info("Canceling workflow: %s", self.execution_id)
        decision = Command()
        attr = CancelWorkflowExecutionCommandAttributes()
        attr.details = None
        decision.cancel_workflow_execution_command_attributes = attr
        decision.command_type = CommandType.COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION
        decision_id = DecisionId(DecisionTarget.SELF, 0)
        self.add_decision(decision_id, CompleteWorkflowStateMachine(decision_id, decision))
        self.completed = True

    def schedule_activity_task(self, schedule: ScheduleActivityTaskCommandAttributes) -> int:
        # PORT: addAllMissingVersionMarker(false, Optional.empty());
        next_decision_event_id = self.next_decision_event_id
        decision_id = DecisionId(DecisionTarget.ACTIVITY, next_decision_event_id)
        self.activity_id_to_scheduled_event_id[schedule.activity_id] = next_decision_event_id
        self.add_decision(decision_id, ActivityDecisionStateMachine(decision_id, schedule_attributes=schedule))
        return next_decision_event_id

    def complete_signal_execution(self, task: SignalMethodTask):
        task.destroy()
        self.tasks.remove(task)

    def handle_activity_task_closed(self, scheduled_event_id: int) -> bool:
        decision: DecisionStateMachine = self.get_decision(DecisionId(DecisionTarget.ACTIVITY, scheduled_event_id))
        assert decision
        decision.handle_completion_event()
        return decision.is_done()

    async def handle_activity_task_scheduled(self, event: HistoryEvent):
        decision = self.get_decision(DecisionId(DecisionTarget.ACTIVITY, event.event_id))
        decision.handle_initiated_event(event)

    async def handle_activity_task_started(self, event: HistoryEvent):
        attr = event.activity_task_started_event_attributes
        decision = self.get_decision(DecisionId(DecisionTarget.ACTIVITY, attr.scheduled_event_id))
        decision.handle_started_event(event)

    async def handle_activity_task_completed(self, event: HistoryEvent):
        self.decision_context.handle_activity_task_completed(event)

    async def handle_activity_task_failed(self, event: HistoryEvent):
        self.decision_context.handle_activity_task_failed(event)

    async def handle_activity_task_timed_out(self, event: HistoryEvent):
        self.decision_context.handle_activity_task_timed_out(event)

    async def handle_decision_task_failed(self, event: HistoryEvent):
        attr = event.workflow_task_failed_event_attributes
        if attr and attr.cause == WorkflowTaskFailedCause.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW:
            self.decision_context.set_current_run_id(attr.new_run_id)

    async def handle_workflow_execution_signaled(self, event: HistoryEvent):
        signaled_event_attributes = event.workflow_execution_signaled_event_attributes
        signal_input_payloads: Payloads = signaled_event_attributes.input
        task = SignalMethodTask(task_id=self.execution_id,
                                workflow_instance=self.workflow_task.workflow_instance,
                                signal_name=signaled_event_attributes.signal_name,
                                signal_input=signal_input_payloads,
                                decider=self,
                                data_converter=self.worker.client.data_converter)
        self.tasks.append(task)
        task.start()

    def add_decision(self, decision_id: DecisionId, decision: DecisionStateMachine):
        self.decisions[decision_id] = decision
        self.next_decision_event_id += 1

    def get_and_increment_next_id(self) -> str:
        ret_value = str(self.id_counter)
        self.id_counter += 1
        return ret_value

    def get_decision(self, decision_id: DecisionId) -> DecisionStateMachine:
        result: DecisionStateMachine = self.decisions.get(decision_id)
        if not result:
            raise NonDeterministicWorkflowException(f"Unknown {decision_id}.")
        return result

    def get_decisions(self) -> List[Command]:
        decisions = []
        for state_machine in self.decisions.values():
            d = state_machine.get_decision()
            if d:
                decisions.append(d)

        # PORT: // Include FORCE_IMMEDIATE_DECISION timer only if there are more then 100 events
        # PORT: int size = result.size();
        # PORT: if (size > MAXIMUM_DECISIONS_PER_COMPLETION &&
        # PORT:         !isCompletionEvent(result.get(MAXIMUM_DECISIONS_PER_COMPLETION - 2))) {
        # PORT:     result = result.subList(0, MAXIMUM_DECISIONS_PER_COMPLETION - 1);
        # PORT:     StartTimerDecisionAttributes attributes = new StartTimerDecisionAttributes();
        # PORT:     attributes.setStartToFireTimeoutSeconds(0);
        # PORT:     attributes.setTimerId(FORCE_IMMEDIATE_DECISION_TIMER);
        # PORT:     Decision d = new Decision();
        # PORT:     d.setStartTimerDecisionAttributes(attributes);
        # PORT:     d.setDecisionType(DecisionType.StartTimer);
        # PORT:     result.add(d);
        # PORT: }

        return decisions

    def destroy(self):
        for t in self.tasks:
            t.destroy()

    def start_timer(self, request: StartTimerCommandAttributes):
        start_event_id = self.next_decision_event_id
        decision_id = DecisionId(DecisionTarget.TIMER, start_event_id)
        self.add_decision(decision_id, TimerDecisionStateMachine(decision_id, start_timer_attributes=request))
        return start_event_id

    def cancel_timer(self, start_event_id: int, immediate_cancellation_callback: Callable):
        decision: DecisionStateMachine = self.get_decision(DecisionId(DecisionTarget.TIMER, start_event_id))
        if decision.is_done():
            return True
        if decision.cancel(immediate_cancellation_callback):
            self.next_decision_event_id += 1
        return decision.is_done()

    def handle_timer_closed(self, attributes: TimerFiredEventAttributes) -> bool:
        decision = self.get_decision(DecisionId(DecisionTarget.TIMER, attributes.started_event_id))
        decision.handle_completion_event()
        return decision.is_done()

    def handle_timer_canceled(self, event: HistoryEvent) -> bool:
        attributes = event.timer_canceled_event_attributes
        decision = self.get_decision(DecisionId(DecisionTarget.TIMER, attributes.started_event_id))
        decision.handle_cancellation_event()
        return decision.is_done()

    def handle_cancel_timer_failed(self, event: HistoryEvent) -> bool:
        started_event_id = event.event_id
        decision = self.get_decision(DecisionId(DecisionTarget.TIMER, started_event_id))
        decision.handle_cancellation_failure_event(event)
        return decision.is_done()

    async def handle_timer_started(self, event: HistoryEvent):
        decision = self.get_decision(DecisionId(DecisionTarget.TIMER, event.event_id))
        decision.handle_initiated_event(event)

    async def handle_timer_fired(self, event: HistoryEvent):
        attributes = event.timer_fired_event_attributes
        self.decision_context.handle_timer_fired(attributes)

    async def handle_marker_recorded(self, event: HistoryEvent):
        self.decision_context.workflow_clock.handle_marker_recorded(event)

    def get_optional_decision_event(self, event_id: int) -> HistoryEvent:
        return self.decision_events.get_optional_decision_event(event_id)

    async def query(self, decision_task: PollWorkflowTaskQueueResponse, query: WorkflowQuery) -> object:
        query_args: Payloads = query.query_args
        task = QueryMethodTask(task_id=self.execution_id,
                               workflow_instance=self.workflow_task.workflow_instance,
                               query_name=query.query_type,
                               query_input=query_args,
                               decider=self,
                               data_converter=self.worker.client.data_converter)
        self.tasks.append(task)
        task.start()
        await self.event_loop.run_event_loop_once()
        if task.status == Status.DONE:
            if task.exception_thrown:
                raise task.exception_thrown
            else:  # ret_value might be None, need to put it in else
                return task.ret_value
        else:
            raise QueryDidNotComplete(f"Query method {query.query_type} with args {query.query_args} did not complete")


# noinspection PyUnusedLocal
async def noop(*args):
    pass


async def on_timer_canceled(self: ReplayDecider, event: HistoryEvent):
    self.decision_context.handle_timer_canceled(event)


event_handlers = {
    EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED: ReplayDecider.handle_workflow_execution_started,
    EventType.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED: ReplayDecider.handle_workflow_execution_cancel_requested,
    EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED: noop,
    EventType.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED: noop,
    EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED: noop,  # Filtered by HistoryHelper
    EventType.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT: noop,  # TODO: check
    EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED: ReplayDecider.handle_decision_task_failed,
    EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED: ReplayDecider.handle_activity_task_scheduled,
    EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED: ReplayDecider.handle_activity_task_started,
    EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED: ReplayDecider.handle_activity_task_completed,
    EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED: ReplayDecider.handle_activity_task_failed,
    EventType.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT: ReplayDecider.handle_activity_task_timed_out,
    EventType.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED: ReplayDecider.handle_workflow_execution_signaled,
    EventType.EVENT_TYPE_TIMER_FIRED: ReplayDecider.handle_timer_fired,
    EventType.EVENT_TYPE_TIMER_STARTED: ReplayDecider.handle_timer_started,
    EventType.EVENT_TYPE_TIMER_CANCELED: on_timer_canceled,
    # TODO:
    # EventType.CancelTimerFailed: ReplayDecider.handle_cancel_timer_failed,
    EventType.EVENT_TYPE_MARKER_RECORDED: ReplayDecider.handle_marker_recorded
}


def decision_task_loop_func(worker: Worker):
    decision_task_loop = DecisionTaskLoop(worker=worker)
    decision_task_loop.start()


@dataclass
class DecisionTaskLoop:
    worker: Worker
    service: WorkflowServiceStub = None
    deciders: Dict[str, ReplayDecider] = field(default_factory=dict)

    def __post_init__(self):
        pass

    def start(self):
        asyncio.create_task(self.run())

    @retry(logger=logger)
    async def run(self):
        try:
            logger.info(f"Decision task worker started: {get_identity()}")
            # event_loop = asyncio.new_event_loop()
            # asyncio.set_event_loop(event_loop)
            self.service = self.worker.client.service
            while True:
                try:
                    if self.worker.is_stop_requested():
                        return
                    # TODO: Do we still need this
                    # self.service.set_next_timeout_cb(self.worker.raise_if_stop_requested)
                    try:
                        decision_task: PollWorkflowTaskQueueResponse = await self.poll()
                    except GRPCError as ex:
                        logger.error("poll_workflow_task_queue failed: %s", ex, exc_info=True)
                        continue
                    if not decision_task:
                        continue
                    next_page_token = decision_task.next_page_token
                    while next_page_token:
                        history_request = GetWorkflowExecutionHistoryRequest()
                        history_request.execution = decision_task.workflow_execution
                        history_request.next_page_token = next_page_token
                        history_request.namespace = self.worker.namespace
                        history_response = await self.service.get_workflow_execution_history(request=history_request)
                        decision_task.history.events.extend(history_response.history.events)
                        next_page_token = history_response.next_page_token
                    if decision_task.query:
                        try:
                            result: Payloads = await self.process_query(decision_task)
                            try:
                                await self.respond_query(decision_task.task_token, result, None)
                            except GRPCError as ex:
                                logger.error("respond_query failed with: %s", ex, exc_info=True)
                        except Exception as ex:
                            logger.error("Error processing query: %s", ex, exc_info=True)
                            try:
                                await self.respond_query(decision_task.task_token, None, str(ex))
                            except GRPCError as ex:
                                logger.error("respond_query (exception) failed with: %s", ex, exc_info=True)
                    else:
                        decisions = await self.process_task(decision_task)
                        try:
                            await self.respond_decisions(decision_task.task_token, decisions)
                        except GRPCError as ex:
                            logger.error("respond_decisions failed with: %s", ex, exc_info=True)
                except StopRequestedException:
                    return
        finally:
            self.worker.notify_thread_stopped()

    async def poll(self) -> Optional[PollWorkflowTaskQueueResponse]:
        try:
            polling_start = datetime.datetime.now()
            poll_decision_request = PollWorkflowTaskQueueRequest()
            poll_decision_request.identity = get_identity()
            poll_decision_request.task_queue = TaskQueue()
            poll_decision_request.task_queue.name = self.worker.task_queue
            poll_decision_request.namespace = self.worker.namespace
            # noinspection PyUnusedLocal
            task: PollWorkflowTaskQueueResponse
            task = await self.service.poll_workflow_task_queue(request=poll_decision_request)
            polling_end = datetime.datetime.now()
            logger.debug("PollWorkflowTaskQueue: %dms", (polling_end - polling_start).total_seconds() * 1000)
        except GRPCError as ex:
            traceback.print_exc()
            logger.error("PollWorkflowTaskQueue error: %s", ex)
            return None
        if not task.task_token:
            logger.debug("PollForWorkflowTaskQueue has no task token (expected): %s", task)
            return None
        return task

    async def process_task(self, decision_task: PollWorkflowTaskQueueResponse) -> List[Command]:
        execution_id = str(decision_task.workflow_execution)
        decider = ReplayDecider(execution_id, decision_task.workflow_type, self.worker,
                                workflow_execution=decision_task.workflow_execution)
        decisions: List[Command] = await decider.decide(decision_task.history.events)
        decider.destroy()
        return decisions

    async def process_query(self, decision_task: PollWorkflowTaskQueueResponse) -> Payloads:
        execution_id = str(decision_task.workflow_execution)
        decider = ReplayDecider(execution_id, decision_task.workflow_type, self.worker,
                                workflow_execution=decision_task.workflow_execution)
        await decider.decide(decision_task.history.events)
        try:
            result = await decider.query(decision_task, decision_task.query)
            return self.worker.client.data_converter.to_payloads([result])
        finally:
            decider.destroy()

    async def respond_query(self, task_token: bytes, result: Payloads = None, error_message: str = None):
        service = self.service
        request = RespondQueryTaskCompletedRequest()
        request.task_token = task_token
        if result:
            request.query_result = result
            request.completed_type = QueryResultType.QUERY_RESULT_TYPE_ANSWERED
        else:
            request.error_message = error_message
            request.completed_type = QueryResultType.QUERY_RESULT_TYPE_FAILED
        response: RespondQueryTaskCompletedResponse = await service.respond_query_task_completed(request=request)
        # TODO: error handling if any
        # if err:
        #    logger.error("Error invoking RespondDecisionTaskCompleted: %s", err)
        # else:
        #    logger.debug("RespondQueryTaskCompleted successful")

    async def respond_decisions(self, task_token: bytes, decisions: List[Command]):
        service = self.service
        request = RespondWorkflowTaskCompletedRequest()
        request.task_token = task_token
        request.commands = []
        request.commands.extend(decisions)
        request.identity = get_identity()
        # noinspection PyUnusedLocal
        response: RespondWorkflowTaskCompletedResponse
        response = await service.respond_workflow_task_completed(request=request)


from .clock_decision_context import ClockDecisionContext, TimerCancellationHandler, LOCAL_ACTIVITY_MARKER_NAME
from .replay_interceptor import make_replay_aware
