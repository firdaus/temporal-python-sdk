import pytest

from temporal.workerfactory import WorkerFactory
from . import cleanup_worker


@pytest.fixture
async def worker(request):
    marker = request.node.get_closest_marker("worker_config")
    namespace = marker.args[0]
    task_queue = marker.args[1]
    activities = marker.kwargs.get("activities", [])
    workflows = marker.kwargs.get("workflows", [])

    factory = WorkerFactory("localhost", 7233, namespace)
    worker_instance = factory.new_worker(task_queue)
    for a_instance, a_cls in activities:
        worker_instance.register_activities_implementation(a_instance, a_cls)
    for w in workflows:
        worker_instance.register_workflow_implementation_type(w)
    factory.start()

    yield worker_instance

    await cleanup_worker(worker_instance)
