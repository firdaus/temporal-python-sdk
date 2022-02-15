import pickle
from dataclasses import dataclass

import pytest
from datetime import timedelta

from temporal.api.common.v1 import Payload
from temporal.conversions import METADATA_ENCODING_KEY
from temporal.converter import DataConverter
from temporal.workflow import workflow_method, WorkflowClient, Workflow
from temporal.activity_method import activity_method

TASK_QUEUE = "test_data_converter"
NAMESPACE = "default"


@dataclass
class Greeting:
    name: str
    age: int


class GreetingActivities:
    @activity_method(task_queue=TASK_QUEUE, schedule_to_close_timeout=timedelta(seconds=1000))
    async def compose_greeting(self, name: str, age: int) -> Greeting:
        raise NotImplementedError


class GreetingActivitiesImpl:

    async def compose_greeting(self, name: str, age: int) -> Greeting:
        return Greeting(name, age)


class GreetingWorkflow:
    @workflow_method(task_queue=TASK_QUEUE)
    async def get_greeting(self) -> Greeting:
        raise NotImplementedError


class GreetingWorkflowImpl(GreetingWorkflow):

    def __init__(self):
        self.greeting_activities: GreetingActivities = Workflow.new_activity_stub(GreetingActivities)

    async def get_greeting(self) -> Greeting:
        return await self.greeting_activities.compose_greeting("Bob", 20)


class PickleDataConverter(DataConverter):

    def to_payload(self, arg: object) -> Payload:
        payload = Payload()
        payload.metadata = {METADATA_ENCODING_KEY: b"PYTHON_PICKLE"}
        payload.data = pickle.dumps(arg)
        return payload

    def from_payload(self, payload: Payload, type_hint: type = None) -> object:
        obj = pickle.loads(payload.data)
        return obj


@pytest.mark.asyncio
@pytest.mark.worker_config(NAMESPACE, TASK_QUEUE, activities=[(GreetingActivitiesImpl(), "GreetingActivities")],
                           workflows=[GreetingWorkflowImpl], data_converter=PickleDataConverter())
async def test(worker):
    client = WorkflowClient.new_client(namespace=NAMESPACE, data_converter=PickleDataConverter())
    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow)
    ret_value = await greeting_workflow.get_greeting()

    assert ret_value == Greeting("Bob", 20)
    client.close()
