import logging
from dataclasses import dataclass, field
from typing import Callable, Dict, Any, Union
from datetime import datetime, tzinfo, timedelta

import pytz

from .api.command.v1 import StartTimerCommandAttributes
from .api.common.v1 import Payloads, Payload
from .api.history.v1 import TimerFiredEventAttributes, HistoryEvent, TimerCanceledEventAttributes
from .decision_loop import ReplayDecider, DecisionContext
from .exceptions import CancellationException
from .marker import MarkerHandler, MarkerInterface, MarkerResult
from .util import OpenRequestInfo
from . import DEFAULT_VERSION

logger = logging.getLogger(__name__)

SIDE_EFFECT_MARKER_NAME = "SideEffect"
MUTABLE_SIDE_EFFECT_MARKER_NAME = "MutableSideEffect"
VERSION_MARKER_NAME = "Version"
LOCAL_ACTIVITY_MARKER_NAME = "LocalActivity"


@dataclass
class ClockDecisionContext:
    decider: ReplayDecider
    decision_context: DecisionContext
    scheduled_timers: Dict[int, OpenRequestInfo] = field(default_factory=dict)
    replay_current_time_milliseconds: datetime = datetime.fromtimestamp(0, tz=pytz.UTC)
    replaying: bool = True
    version_handler: MarkerHandler = None

    def __post_init__(self):
        self.version_handler = MarkerHandler(self.decision_context, VERSION_MARKER_NAME)

    def set_replay_current_time_milliseconds(self, s: datetime):
        self.replay_current_time_milliseconds = s

    def current_time_millis(self) -> datetime:
        return self.replay_current_time_milliseconds

    def create_timer(self, delay_seconds: int, callback: Callable):
        if delay_seconds < 0:
            raise Exception("Negative delay seconds: " + str(delay_seconds))
        if delay_seconds == 0:
            callback(None)
            return None
        firing_time = (self.current_time_millis().timestamp() * 1000) + delay_seconds * 1000
        context = OpenRequestInfo(user_context=firing_time)
        timer = StartTimerCommandAttributes()
        timer.start_to_fire_timeout = timedelta(seconds=delay_seconds)
        timer.timer_id = str(self.decider.get_and_increment_next_id())
        start_event_id: int = self.decider.start_timer(timer)
        context.completion_handle = lambda ctx, e: callback(e)  # type: ignore
        self.scheduled_timers[start_event_id] = context
        return TimerCancellationHandler(start_event_id=start_event_id, clock_decision_context=self)

    def is_replaying(self):
        return self.replaying

    def set_replaying(self, replaying):
        self.replaying = replaying

    def timer_cancelled(self, start_event_id: int, reason: Exception):
        scheduled: OpenRequestInfo = self.scheduled_timers.pop(start_event_id, None)
        if not scheduled:
            return
        callback = scheduled.completion_handle
        exception = CancellationException("Cancelled by request")
        exception.init_cause(reason)
        callback(None, exception)

    def handle_timer_fired(self, attributes: TimerFiredEventAttributes):
        started_event_id: int = attributes.started_event_id
        if self.decider.handle_timer_closed(attributes):
            scheduled = self.scheduled_timers.pop(started_event_id, None)
            if scheduled:
                callback = scheduled.completion_handle
                callback(None, None)

    def handle_timer_canceled(self, event: HistoryEvent):
        attributes: TimerCanceledEventAttributes = event.timer_canceled_event_attributes
        started_event_id: int = attributes.started_event_id
        if self.decider.handle_timer_canceled(event):
            self.timer_cancelled(started_event_id, None)

    def get_version(self, change_id: str, min_supported: int, max_supported: int) -> int:
        def func():
            d: Dict[str, Payloads] = {"VERSION": self.decider.worker.client.data_converter.to_payloads([max_supported])}
            return d

        result: Dict[str, Payloads] = self.version_handler.handle(change_id, func)
        if result is None:
            result = {"VERSION": self.decider.worker.client.data_converter.to_payloads([DEFAULT_VERSION])}
            self.version_handler.set_data(change_id, result)
            self.version_handler.mark_replayed(change_id)  # so that we don't ever emit a MarkerRecorded for this

        version: int = self.decider.worker.client.data_converter.from_payloads(result["VERSION"])[0]  # type: ignore
        self.validate_version(change_id, version, min_supported, max_supported)
        return version

    def validate_version(self, change_id: str, version: int, min_supported: int, max_supported: int):
        if version < min_supported or version > max_supported:
            raise Exception(f"Version {version} of changeID {change_id} is not supported. "
                            f"Supported version is between {min_supported} and {max_supported}.")

    def handle_marker_recorded(self, event: HistoryEvent):
        """
        Will be executed more than once for the same event.
        """
        attributes = event.marker_recorded_event_attributes
        name: str = attributes.marker_name
        if SIDE_EFFECT_MARKER_NAME == name:
            # TODO
            # sideEffectResults.put(event.getEventId(), attributes.getDetails());
            pass
        elif LOCAL_ACTIVITY_MARKER_NAME == name:
            # TODO
            # handleLocalActivityMarker(attributes);
            pass
        elif VERSION_MARKER_NAME == name:
            marker_data = MarkerInterface.from_event_attributes(attributes)
            change_id: str = marker_data.get_id()
            data: Dict[str, Payloads] = marker_data.get_data()
            self.version_handler.mutable_marker_results[change_id] = MarkerResult(data=data)
        elif MUTABLE_SIDE_EFFECT_MARKER_NAME != name:
            # TODO
            # if (log.isWarnEnabled()) {
            #       log.warn("Unexpected marker: " + event);
            # }
            pass


@dataclass
class TimerCancellationHandler:
    start_event_id: int
    clock_decision_context: ClockDecisionContext

    def accept(self, reason: Exception):
        self.clock_decision_context.decider.cancel_timer(self.start_event_id,
                                                         lambda: self.clock_decision_context.timer_cancelled(
                                                             self.start_event_id, reason))
