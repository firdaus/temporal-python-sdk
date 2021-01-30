import logging

from temporal.worker import Worker
from temporal.workflow import WorkflowClient

logging.basicConfig(level=logging.INFO)


def setup_module():
    pass


def teardown_module():
    pass


async def cleanup_worker(client: WorkflowClient, worker: Worker):
    await worker.stop(background=False)
    client.close()
