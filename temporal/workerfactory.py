import asyncio
import logging
import signal
from dataclasses import dataclass, field
from typing import List

from temporal.api.workflowservice.v1 import WorkflowServiceStub
from temporal.worker import Worker, WorkerOptions
from temporal.workflow import WorkflowClient

logger = logging.getLogger(__name__)


@dataclass
class WorkerFactoryOptions:
    pass


@dataclass
class WorkerFactory:
    client: WorkflowClient
    namespace: str = None
    options: WorkerFactoryOptions = None
    workers: List[Worker] = field(default_factory=list)

    def new_worker(self, task_queue: str, worker_options: WorkerOptions = None) -> Worker:
        worker = Worker(client=self.client, namespace=self.namespace, task_queue=task_queue, options=worker_options)
        self.workers.append(worker)
        return worker

    def ask_exit(self, signame):
        logger.info(f"Exit signal received: {signame}. Requesting all workers to stop.")
        for worker in self.workers:
            worker.stop_requested = True

    async def start_async(self):
        loop = asyncio.get_event_loop()
        for signame in {"SIGINT", "SIGTERM"}:
            loop.add_signal_handler(
                getattr(signal, signame),
                self.ask_exit,
                signame,
            )

        worker_coros = [worker.start_async() for worker in self.workers]
        await asyncio.gather(*worker_coros)

    def start(self):
        for worker in self.workers:
            worker.start()

    async def stop(self):
        for worker in self.workers:
            await worker.stop()
