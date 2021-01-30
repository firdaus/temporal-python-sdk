import logging
import os
from typing import List

from temporal.worker import Worker

logging.basicConfig(level=logging.INFO)

_workers: List[Worker] = []


def setup_module():
    pass


def teardown_module():
    pass


async def cleanup_worker(worker: Worker):
    global _workers
    background = os.getenv("BACKGROUND_CLEANUP", "False") == "True"
    await worker.stop(background=background)
    _workers.append(worker)
