import asyncio
import pickle
from dataclasses import dataclass

import pytest

from temporal.api.common.v1 import Payload
from temporal.conversions import METADATA_ENCODING_KEY
from temporal.converter import DataConverter
from temporal.workflow import workflow_method, WorkflowClient, Workflow, query_method, signal_method

TASK_QUEUE = "test_typed_data_converter_query_signal"
NAMESPACE = "default"
workflow_started = False


@dataclass
class QueryArgType1:
    pass


@dataclass
class QueryArgType2:
    pass


@dataclass
class QueryRetType:
    pass


@dataclass
class SignalArgType1:
    pass


@dataclass
class SignalArgType2:
    pass


QUERY_ARG_TYPE_1 = QueryArgType1()
QUERY_ARG_TYPE_2 = QueryArgType2()
QUERY_RET_TYPE = QueryRetType()
SIGNAL_ARG_TYPE_1 = SignalArgType1()
SIGNAL_ARG_TYPE_2 = SignalArgType2()


class GreetingWorkflow:

    @query_method
    async def get_status(self, arg1: QueryArgType1, arg2: QueryArgType2) -> QueryRetType:
        raise NotImplementedError

    @signal_method
    async def push_status(self, arg1: SignalArgType1, arg2: SignalArgType2):
        raise NotImplementedError

    @workflow_method(task_queue=TASK_QUEUE)
    async def get_greeting(self) -> None:
        raise NotImplementedError


class GreetingWorkflowImpl(GreetingWorkflow):

    @query_method
    async def get_status(self, arg1: QueryArgType1, arg2: QueryArgType2) -> QueryRetType:
        return QUERY_RET_TYPE

    @signal_method
    async def push_status(self, arg1: SignalArgType1, arg2: SignalArgType2):
        pass

    async def get_greeting(self):
        global workflow_started
        workflow_started = True
        await Workflow.sleep(10)


deserialized = []


class PickleDataConverter(DataConverter):

    def to_payload(self, arg: object) -> Payload:
        payload = Payload()
        payload.metadata = {METADATA_ENCODING_KEY: b"PYTHON_PICKLE"}
        payload.data = pickle.dumps(arg)
        return payload

    def from_payload(self, payload: Payload, type_hint: type = None) -> object:
        obj = pickle.loads(payload.data)
        if type_hint:
            deserialized.append((type_hint, obj))
        return obj


@pytest.mark.asyncio
@pytest.mark.worker_config(NAMESPACE, TASK_QUEUE, activities=[], workflows=[GreetingWorkflowImpl],
                           data_converter=PickleDataConverter())
async def test(worker):
    global workflow_started
    client = WorkflowClient.new_client(namespace=NAMESPACE, data_converter=PickleDataConverter())
    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow)
    context = await WorkflowClient.start(greeting_workflow.get_greeting)
    while not workflow_started:
        await asyncio.sleep(2)
    greeting_workflow = client.new_workflow_stub_from_workflow_id(GreetingWorkflow,
                                                                  workflow_id=context.workflow_execution.workflow_id)
    await greeting_workflow.push_status(SIGNAL_ARG_TYPE_1, SIGNAL_ARG_TYPE_2)
    await greeting_workflow.get_status(QUERY_ARG_TYPE_1, QUERY_ARG_TYPE_2)
    await client.wait_for_close(context)
    assert deserialized == [
        # Signal invocation
        (SignalArgType1, SIGNAL_ARG_TYPE_1),
        (SignalArgType2, SIGNAL_ARG_TYPE_2),
        # Replay signal to invoke query
        (SignalArgType1, SIGNAL_ARG_TYPE_1),
        (SignalArgType2, SIGNAL_ARG_TYPE_2),
        (QueryArgType1, QUERY_ARG_TYPE_1),
        (QueryArgType2, QUERY_ARG_TYPE_2),
        # Query return type being deserialized
        (QueryRetType, QUERY_RET_TYPE),
        # Workflow replayed after sleep is over
        (SignalArgType1, SIGNAL_ARG_TYPE_1),
        (SignalArgType2, SIGNAL_ARG_TYPE_2)
    ]
