import pickle
from dataclasses import dataclass

import pytest
from datetime import timedelta

from temporal.api.common.v1 import Payload
from temporal.conversions import METADATA_ENCODING_KEY
from temporal.converter import DataConverter
from temporal.workflow import workflow_method, WorkflowClient, Workflow
from temporal.activity_method import activity_method

TASK_QUEUE = "test_typed_data_converter"
NAMESPACE = "default"


@dataclass
class ActivityArgType1:
    pass


@dataclass
class ActivityArgType2:
    pass


@dataclass
class ActivityRetType:
    pass


@dataclass
class WorkflowArgType1:
    pass


@dataclass
class WorkflowArgType2:
    pass


@dataclass
class WorkflowRetType:
    pass


ACTIVITY_ARG_TYPE_1 = ActivityArgType1()
ACTIVITY_ARG_TYPE_2 = ActivityArgType2()
ACTIVITY_RET_TYPE = ActivityRetType()
WORKFLOW_ARG_TYPE_1 = WorkflowArgType1()
WORKFLOW_ARG_TYPE_2 = WorkflowArgType2()
WORKFLOW_RET_TYPE = WorkflowRetType()


class GreetingActivities:
    @activity_method(task_queue=TASK_QUEUE, schedule_to_close_timeout=timedelta(seconds=1000))
    async def compose_greeting(self, arg1: ActivityArgType1, arg2: ActivityArgType2) -> ActivityRetType:
        raise NotImplementedError


class GreetingActivitiesImpl:

    async def compose_greeting(self, arg1: ActivityArgType1, arg2: ActivityArgType2) -> ActivityRetType:
        return ACTIVITY_RET_TYPE


class GreetingWorkflow:
    @workflow_method(task_queue=TASK_QUEUE)
    async def get_greeting(self, arg1: WorkflowArgType1, arg2: WorkflowArgType2) -> WorkflowRetType:
        raise NotImplementedError


class GreetingWorkflowImpl(GreetingWorkflow):

    def __init__(self):
        self.greeting_activities: GreetingActivities = Workflow.new_activity_stub(GreetingActivities)

    async def get_greeting(self, arg1: WorkflowArgType1, arg2: WorkflowArgType2) -> WorkflowRetType:
        ret_value: ActivityRetType = await self.greeting_activities.compose_greeting(ACTIVITY_ARG_TYPE_1,
                                                                                     ACTIVITY_ARG_TYPE_2)
        return WORKFLOW_RET_TYPE


deserialized = []


class PickleDataConverter(DataConverter):

    def to_payload(self, arg: object) -> Payload:
        payload = Payload()
        payload.metadata = {METADATA_ENCODING_KEY: b"PYTHON_PICKLE"}
        payload.data = pickle.dumps(arg)
        return payload

    def from_payload(self, payload: Payload, type_hint: type = None) -> object:
        obj = pickle.loads(payload.data)
        deserialized.append((type_hint, obj))
        return obj


@pytest.mark.asyncio
@pytest.mark.worker_config(NAMESPACE, TASK_QUEUE, activities=[(GreetingActivitiesImpl(), "GreetingActivities")],
                           workflows=[GreetingWorkflowImpl], data_converter=PickleDataConverter())
async def test(worker):
    client = WorkflowClient.new_client(namespace=NAMESPACE, data_converter=PickleDataConverter())
    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow)
    await greeting_workflow.get_greeting(WORKFLOW_ARG_TYPE_1, WORKFLOW_ARG_TYPE_2)
    for t, obj in deserialized:
        assert isinstance(obj, t)
    assert deserialized == [
        # Invoking workflow
        (WorkflowArgType1, WORKFLOW_ARG_TYPE_1),
        (WorkflowArgType2, WORKFLOW_ARG_TYPE_2),
        # Invoking activity
        (ActivityArgType1, ACTIVITY_ARG_TYPE_1),
        (ActivityArgType2, ACTIVITY_ARG_TYPE_2),
        # Replaying workflow with activity result
        (WorkflowArgType1, WORKFLOW_ARG_TYPE_1),
        (WorkflowArgType2, WORKFLOW_ARG_TYPE_2),
        (ActivityRetType, ACTIVITY_RET_TYPE),
        # Processing workflow return value
        (WorkflowRetType, WORKFLOW_RET_TYPE)
    ]
