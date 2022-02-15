import asyncio
from asyncio.events import AbstractEventLoop
from typing import Optional

import pytest

from temporal.converter import DEFAULT_DATA_CONVERTER_INSTANCE
from temporal.workerfactory import WorkerFactory
from temporal.workflow import WorkflowClient
from . import cleanup_worker

loop: Optional[AbstractEventLoop] = None


@pytest.fixture
def event_loop():
    global loop
    if not loop:
        loop = asyncio.get_event_loop()
    yield loop


@pytest.fixture
async def worker(request):
    marker = request.node.get_closest_marker("worker_config")
    namespace = marker.args[0]
    task_queue = marker.args[1]
    activities = marker.kwargs.get("activities", [])
    workflows = marker.kwargs.get("workflows", [])
    data_converter = marker.kwargs.get("data_converter", DEFAULT_DATA_CONVERTER_INSTANCE)

    client: WorkflowClient = WorkflowClient.new_client("localhost", 7233, data_converter=data_converter)
    factory = WorkerFactory(client, namespace)
    worker_instance = factory.new_worker(task_queue)
    for a_instance, a_cls in activities:
        worker_instance.register_activities_implementation(a_instance, a_cls)
    for w in workflows:
        worker_instance.register_workflow_implementation_type(w)
    factory.start()

    yield worker_instance

    asyncio.create_task(cleanup_worker(client, worker_instance))
