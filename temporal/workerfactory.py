import asyncio
import logging
import signal
from dataclasses import dataclass, field
from typing import List

from temporal.api.workflowservice.v1 import WorkflowServiceStub
from temporal.worker import Worker, WorkerOptions
from temporal.workflow import WorkflowClient

logger = logging.getLogger(__name__)

exit_count = 0

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

    def ask_exit(self, signame, loop):
        global exit_count
        exit_count += 1
        
        if exit_count > 1:
            loop.stop()
            return

        logger.debug(f"Exit signal received: {signame}")
        logger.info(f"Requesting all workers to stop. Try again to immediately terminate.")

        for worker in self.workers:
            worker.stop_async()

    async def start_async(self):
        loop = asyncio.get_event_loop()
        for signame in {"SIGINT", "SIGTERM"}:
            loop.add_signal_handler(
                getattr(signal, signame),
                self.ask_exit,
                signame,
                loop
            )

        worker_coros = [worker.start_async() for worker in self.workers]
        await asyncio.gather(*worker_coros)

    def start(self):
        for worker in self.workers:
            worker.start()

    async def stop(self):
        for worker in self.workers:
            await worker.stop()
