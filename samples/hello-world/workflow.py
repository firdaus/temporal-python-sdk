import time
from datetime import timedelta

from temporal.activity_method import activity_method
from temporal.workflow import workflow_method, Workflow

TASK_QUEUE = "hello-world"

class GreetingActivities:
    @activity_method(
        task_queue=TASK_QUEUE,
        start_to_close_timeout=timedelta(seconds=10)
    )
    async def compose_greeting(self, greeting: str, name: str):
        return greeting + " " + name + "!"

class GreetingWorkflow:
    def __init__(self):
        self.greeting_activities: GreetingActivities = Workflow.new_activity_stub(
            GreetingActivities
        )
        pass

    @workflow_method(
        task_queue=TASK_QUEUE,
    )
    async def get_greeting(self, name):
        return await self.greeting_activities.compose_greeting("Hello", name)

