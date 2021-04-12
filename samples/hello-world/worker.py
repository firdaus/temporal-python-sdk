import asyncio
import logging

from temporal.workerfactory import WorkerFactory
from temporal.workflow import WorkflowClient

from workflow import GreetingActivities, GreetingWorkflow

log_console_format = (
    "%(asctime)s | %(levelname)s | %(filename)s:%(funcName)s:%(lineno)d | %(message)s"
)
logging.basicConfig(level=logging.INFO, format=log_console_format)

TASK_QUEUE = "hello-world"
NAMESPACE = "default"


async def main():
    client = WorkflowClient.new_client()

    factory = WorkerFactory(client, NAMESPACE)
    worker = factory.new_worker(TASK_QUEUE)

    worker.register_activities_implementation(GreetingActivities())
    worker.register_workflow_implementation_type(GreetingWorkflow)

    await factory.start_async()


if __name__ == "__main__":
    asyncio.run(main())
