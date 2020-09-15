from dataclasses import dataclass, field
from typing import List

from temporal.worker import Worker, WorkerOptions


@dataclass
class WorkerFactoryOptions:
    pass


@dataclass
class WorkerFactory:
    host: str = None
    port: int = None
    namespace: str = None
    options: WorkerFactoryOptions = None
    workers: List[Worker] = field(default_factory=list)

    def new_worker(self, task_queue: str, worker_options: WorkerOptions = None) -> Worker:
        worker = Worker(host=self.host, port=self.port, namespace=self.namespace, task_queue=task_queue, options=worker_options)
        self.workers.append(worker)
        return worker

    def start(self):
        for worker in self.workers:
            worker.start()
