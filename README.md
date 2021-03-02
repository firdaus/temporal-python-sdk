# Unofficial Python SDK for the Temporal Workflow Engine

## Status

This should be considered EXPERIMENTAL at the moment. At the moment, all I can say is that the [test cases](https://gist.github.com/firdaus/4ec442f2c626122ad0c8d379a7ffd8bc) currently pass. I have not tested this for any real world use cases yet. 

## Installation

```
pip install temporal-python-sdk
```

## Hello World

```python
import asyncio
import logging
from datetime import timedelta

from temporal.activity_method import activity_method
from temporal.workerfactory import WorkerFactory
from temporal.workflow import workflow_method, Workflow, WorkflowClient

logging.basicConfig(level=logging.INFO)

TASK_QUEUE = "HelloActivity-python-tq"
NAMESPACE = "default"

# Activities Interface
class GreetingActivities:
    @activity_method(task_queue=TASK_QUEUE, schedule_to_close_timeout=timedelta(seconds=1000))
    async def compose_greeting(self, greeting: str, name: str) -> str:
        raise NotImplementedError


# Activities Implementation
class GreetingActivitiesImpl:
    async def compose_greeting(self, greeting: str, name: str):
        return greeting + " " + name + "!"


# Workflow Interface
class GreetingWorkflow:
    @workflow_method(task_queue=TASK_QUEUE)
    async def get_greeting(self, name: str) -> str:
        raise NotImplementedError


# Workflow Implementation
class GreetingWorkflowImpl(GreetingWorkflow):

    def __init__(self):
        self.greeting_activities: GreetingActivities = Workflow.new_activity_stub(GreetingActivities)
        pass

    async def get_greeting(self, name):
        return await self.greeting_activities.compose_greeting("Hello", name)


async def client_main():
    client = WorkflowClient.new_client(namespace=NAMESPACE)

    factory = WorkerFactory(client, NAMESPACE)
    worker = factory.new_worker(TASK_QUEUE)
    worker.register_activities_implementation(GreetingActivitiesImpl(), "GreetingActivities")
    worker.register_workflow_implementation_type(GreetingWorkflowImpl)
    factory.start()

    greeting_workflow: GreetingWorkflow = client.new_workflow_stub(GreetingWorkflow)
    result = await greeting_workflow.get_greeting("Python")
    print(result)
    print("Stopping workers.....")
    await worker.stop()
    print("Workers stopped......")

if __name__ == '__main__':
    asyncio.run(client_main())
```

## Roadmap 

1.0
- [x]  Workflow argument passing and return values
- [x]  Activity invocation
- [x]  Activity heartbeat and Activity.getHeartbeatDetails()
- [x]  doNotCompleteOnReturn
- [x]  ActivityCompletionClient
    - [x]  complete
    - [x]  complete_exceptionally
- [x]  Activity get_namespace(), get_task_token() get_workflow_execution()
- [x]  Activity Retry
- [x]  Activity Failure Exceptions
- [x] workflow_execution_timeout / workflow_run_timeout / workflow_task_timeout
- [x] Workflow exceptions
- [x]  Cron workflows
- [x]  Workflow static methods:
    - [x]  await_till()
    - [x]  sleep()
    - [x]  current_time_millis()
    - [x]  now()
    - [x]  random_uuid()
    - [x]  new_random()
    - [x]  get_workflow_id()
    - [x]  get_run_id()
    - [x]  get_version()
    - [x]  get_logger()
- [x]  Activity invocation parameters
- [x]  Query method
- [x]  Signal methods
- [x]  Workflow start parameters - workflow_id etc...
- [x]  Workflow client - starting workflows synchronously
- [x]  Workflow client - starting workflows asynchronously (WorkflowClient.start)
- [x]  Get workflow result after async execution (client.wait_for_close)
- [x]  Workflow client - invoking signals
- [x]  Workflow client - invoking queries

1.1
- [x] ActivityStub and Workflow.newUntypedActivityStub
- [x] Remove threading, use coroutines for everything all concurrency
- [x] Classes as arguments and return values to/from activity and workflow methods (DataConverter)
    - [x] Type hints for DataConverter
- [ ] WorkflowStub and WorkflowClient.newUntypedWorkflowStub
- [ ] Parallel activity execution (STATUS: there's a working but not finalized API).
- [ ] Custom workflow ids through start() and new_workflow_stub()
- [ ] ContinueAsNew ![](https://img.shields.io/badge/PRs-welcome-informational)
- [ ] Compatibility with Java client ![](https://img.shields.io/badge/PRs-welcome-informational)
- [ ] Compatibility with Golang client ![](https://img.shields.io/badge/PRs-welcome-informational)

2.0
- [ ] Upgrade python-betterproto ![](https://img.shields.io/badge/PRs-welcome-informational)
- [ ] Sticky workflows ![](https://img.shields.io/badge/PRs-welcome-informational)

Post 2.0:
- [ ] sideEffect/mutableSideEffect ![](https://img.shields.io/badge/PRs-welcome-informational)
- [ ] Local activity ![](https://img.shields.io/badge/PRs-welcome-informational)
- [ ] Timers ![](https://img.shields.io/badge/PRs-welcome-informational)
- [ ] Cancellation Scopes ![](https://img.shields.io/badge/PRs-welcome-informational)
- [ ] Child Workflows ![](https://img.shields.io/badge/PRs-welcome-informational)
- [ ] Explicit activity ids for activity invocations ![](https://img.shields.io/badge/PRs-welcome-informational)
