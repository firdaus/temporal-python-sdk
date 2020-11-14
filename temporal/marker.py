from __future__ import annotations

from dataclasses import dataclass, field
from dataclasses_json import dataclass_json, LetterCase

from typing import Dict

from temporal.api.common.v1 import Header, Payload, Payloads
from temporal.api.enums.v1 import EventType
from temporal.api.history.v1 import MarkerRecordedEventAttributes, HistoryEvent

from .decision_loop import DecisionContext

MUTABLE_MARKER_HEADER_KEY = "MutableMarkerHeader"


class MarkerInterface:
    @staticmethod
    def from_event_attributes(attributes: MarkerRecordedEventAttributes) -> MarkerInterface:
        if attributes.header and attributes.header.fields and MUTABLE_MARKER_HEADER_KEY in attributes.header.fields:
            buffer: bytes = attributes.header.fields.get(MUTABLE_MARKER_HEADER_KEY).data
            header = MarkerHeader.from_json(str(buffer, "utf-8"))  # type: ignore
            return MarkerData(header=header, data=attributes.details)
        else:
            # return PlainMarkerData.from_json(str(attributes.details, "utf-8"))
            raise Exception("PlainMarkerData is not supported")

    def get_id(self) -> str:
        raise NotImplementedError()

    def get_access_count(self) -> int:
        raise NotImplementedError()

    def get_data(self) -> Dict[str, Payloads]:
        raise NotImplementedError()


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class MarkerHeader:
    id: str = None
    event_id: int = None
    access_count: int = 0


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class MarkerData(MarkerInterface):
    header: MarkerHeader = None
    data: Dict[str, Payloads] = None

    @staticmethod
    def create(id: str, event_id: int, data: Dict[str, Payloads], access_count: int) -> MarkerData:
        header = MarkerHeader(id=id, event_id=event_id, access_count=access_count)
        return MarkerData(header=header, data=data)

    def get_header(self) -> Header:
        header_bytes = self.header.to_json().encode("utf-8")  # type: ignore
        header = Header()
        header.fields[MUTABLE_MARKER_HEADER_KEY] = Payload(data=header_bytes)
        return header

    def get_access_count(self) -> int:
        return self.header.access_count

    def get_data(self) -> Dict[str, Payloads]:
        return self.data

    def get_id(self) -> str:
        return self.header.id


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class MarkerResult:
    data: Dict[str, Payloads] = None
    access_count: int = 0
    replayed: bool = False


@dataclass
class MarkerHandler:
    decision_context: DecisionContext
    marker_name: str
    mutable_marker_results: Dict[str, MarkerResult] = field(default_factory=dict)

    def record_mutable_marker(self, id: str, event_id: int, data: Dict[str, Payloads], access_count: int):
        marker = MarkerData.create(id=id, event_id=event_id, data=data, access_count=access_count)
        if id in self.mutable_marker_results:
            self.mutable_marker_results[id].replayed = True
        else:
            self.mutable_marker_results[id] = MarkerResult(data=data)
        self.decision_context.record_marker(self.marker_name, marker.get_header(), data)

    # Sets data without creating a decision - used when DEFAULT_VERSION is the implicit current version
    def set_data(self, id, data: Dict[str, Payloads]):
        self.mutable_marker_results[id] = MarkerResult(data=data)

    def mark_replayed(self, id):
        self.mutable_marker_results[id].replayed = True

    def handle(self, id: str, func) -> Dict[str, Payloads]:
        event_id = self.decision_context.decider.next_decision_event_id
        result: MarkerResult = self.mutable_marker_results.get(id)
        if result or self.decision_context.is_replaying():
            if result:
                if self.decision_context.is_replaying() and not result.replayed:
                    # Need to insert marker to ensure that event_id is incremented
                    self.record_mutable_marker(id, event_id, result.data, 0)
                return result.data
            else:
                return None
        else:
            to_store = func()
            if to_store:
                data = to_store
                self.record_mutable_marker(id, event_id, data, 0)
                return to_store
            else:
                # TODO: Should this ever happen? - at least for version it will never happen
                return None

    # This method is currently not being used - after adopting the version logic from the
    # Golang client
    def get_marker_data_from_history(self, event_id: int, marker_id: str, expected_access_count: int) -> \
            Dict[str, Payloads]:
        event: HistoryEvent = self.decision_context.decider.get_optional_decision_event(event_id)
        if not event or event.event_type != EventType.EVENT_TYPE_MARKER_RECORDED:
            return None

        attributes: MarkerRecordedEventAttributes = event.marker_recorded_event_attributes
        name = attributes.marker_name
        if self.marker_name != name:
            return None

        marker_data = MarkerInterface.from_event_attributes(attributes)
        if marker_id != marker_data.get_id() or marker_data.get_access_count() > expected_access_count:
            return None

        return marker_data.get_data()


# @dataclass_json(letter_case=LetterCase.CAMEL)
# @dataclass
# class PlainMarkerData(MarkerInterface):
#     id: str = None
#     event_id: int = None
#     data: bytes = None
#     access_count: int = 0
#
#     def get_access_count(self):
#         return self.access_count
#
#     def get_data(self):
#         return self.data
#
#     def get_id(self) -> str:
#         return self.id
#
