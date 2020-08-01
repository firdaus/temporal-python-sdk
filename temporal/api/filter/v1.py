# Generated by the protocol buffer compiler.  DO NOT EDIT!
# sources: temporal/api/filter/v1/message.proto
# plugin: python-betterproto
from dataclasses import dataclass

import betterproto

from temporal.api.enums import v1 as v1enums


@dataclass
class WorkflowExecutionFilter(betterproto.Message):
    workflow_id: str = betterproto.string_field(1)
    run_id: str = betterproto.string_field(2)


@dataclass
class WorkflowTypeFilter(betterproto.Message):
    name: str = betterproto.string_field(1)


@dataclass
class StartTimeFilter(betterproto.Message):
    earliest_time: int = betterproto.int64_field(1)
    latest_time: int = betterproto.int64_field(2)


@dataclass
class StatusFilter(betterproto.Message):
    status: v1enums.WorkflowExecutionStatus = betterproto.enum_field(1)