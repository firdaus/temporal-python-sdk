import asyncio
import logging

from temporal.worker import Worker
from temporal.workflow import WorkflowClient

logging.basicConfig(level=logging.INFO)

workers_to_cleanup = []


def setup_module():
    pass


async def monitor_teardown():
    from .conftest import loop
    while True:
        running_tasks = asyncio.all_tasks(loop)
        print(f"running_tasks={len(running_tasks)}")
        await asyncio.sleep(5)


def teardown_module():
    from .conftest import loop
    pending = asyncio.all_tasks(loop)
    print("Waiting for workers to cleanup....")
    loop.create_task(monitor_teardown())
    loop.run_until_complete(asyncio.gather(*pending))


async def cleanup_worker(client: WorkflowClient, worker: Worker):
    workers_to_cleanup.append(worker)
    await worker.stop(background=False)
    client.close()
