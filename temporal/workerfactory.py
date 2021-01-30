from dataclasses import dataclass, field
from typing import List

from temporal.api.workflowservice.v1 import WorkflowServiceStub
from temporal.worker import Worker, WorkerOptions
from temporal.workflow import WorkflowClient


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

    def start(self):
        for worker in self.workers:
            worker.start()
