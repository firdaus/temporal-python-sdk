import logging
import time
from typing import List

from temporal.worker import Worker

logging.basicConfig(level=logging.INFO)

_workers: List[Worker] = []


def setup_module():
    pass


def teardown_module():
    global _workers
    for worker in _workers:
        worker.stop(background=True)
    print(f"Workers to cleanup: {len(_workers)}")
    for n, worker in enumerate(_workers):
        print(f"Stopping worker: {n + 1}")
        while not worker.is_stopped():
            time.sleep(5)


def cleanup_worker(worker: Worker):
    global _workers
    _workers.append(worker)
