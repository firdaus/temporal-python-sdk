import pytest

from temporal.workerfactory import WorkerFactory


@pytest.fixture
def worker(request):
    marker = request.node.get_closest_marker("worker_config")
    domain = marker.args[0]
    task_queue = marker.args[1]
    activities = marker.kwargs.get("activities", [])
    workflows = marker.kwargs.get("workflows", [])

    factory = WorkerFactory("localhost", 7233, domain)
    worker_instance = factory.new_worker(task_queue)
    for a_instance, a_cls in activities:
        worker_instance.register_activities_implementation(a_instance, a_cls)
    for w in workflows:
        worker_instance.register_workflow_implementation_type(w)
    factory.start()

    yield worker_instance

    worker_instance.stop(background=False)
