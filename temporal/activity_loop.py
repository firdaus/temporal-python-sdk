import datetime
import logging
import json
import inspect
from typing import List

from grpclib import GRPCError

from temporal.activity import ActivityContext, ActivityTask, complete_exceptionally, complete
from temporal.api.taskqueue.v1 import TaskQueue, TaskQueueMetadata
from temporal.converter import get_fn_args_type_hints
from temporal.retry import retry
from temporal.service_helpers import get_identity
from temporal.worker import Worker, StopRequestedException
from temporal.api.workflowservice.v1 import WorkflowServiceStub as WorkflowService, PollActivityTaskQueueRequest, \
    PollActivityTaskQueueResponse

logger = logging.getLogger(__name__)


@retry(logger=logger)
async def activity_task_loop_func(worker: Worker):
    service: WorkflowService = worker.client.service
    logger.info(f"Activity task worker started: {get_identity()}")
    try:
        while True:
            if worker.is_stop_requested():
                return
            try:
                polling_start = datetime.datetime.now()
                polling_request: PollActivityTaskQueueRequest = PollActivityTaskQueueRequest()
                polling_request.task_queue_metadata = TaskQueueMetadata()
                polling_request.task_queue_metadata.max_tasks_per_second = 200000
                polling_request.namespace = worker.namespace
                polling_request.identity = get_identity()
                polling_request.task_queue = TaskQueue()
                polling_request.task_queue.name = worker.task_queue
                task: PollActivityTaskQueueResponse
                task = await service.poll_activity_task_queue(request=polling_request)
                polling_end = datetime.datetime.now()
                logger.debug("PollActivityTaskQueue: %dms", (polling_end - polling_start).total_seconds() * 1000)
            except StopRequestedException:
                return
            except GRPCError as ex:
                logger.error("Error invoking poll_activity_task_queue: %s", ex, exc_info=True)
                continue
            task_token = task.task_token
            if not task_token:
                logger.debug("PollActivityTaskQueue has no task_token (expected): %s", task)
                continue

            logger.info(f"Request for activity: {task.activity_type.name}")
            fn = worker.activities.get(task.activity_type.name)
            if not fn:
                logger.error("Activity type not found: " + task.activity_type.name)
                continue

            args: List[object] = worker.client.data_converter.from_payloads(task.input,
                                                                            get_fn_args_type_hints(fn))

            process_start = datetime.datetime.now()
            activity_context = ActivityContext()
            activity_context.client = worker.client
            activity_context.activity_task = ActivityTask.from_poll_for_activity_task_response(task)
            activity_context.namespace = worker.namespace
            try:
                ActivityContext.set(activity_context)
                if inspect.iscoroutinefunction(fn):
                    return_value = await fn(*args)
                else:
                    raise Exception(f"Activity method {fn.__module__}.{fn.__qualname__} should be a coroutine")
                if activity_context.do_not_complete:
                    logger.info(f"Not completing activity {task.activity_type.name}({str(args)[1:-1]})")
                    continue

                logger.info(
                    f"Activity {task.activity_type.name}({str(args)[1:-1]}) returned {return_value}")

                try:
                    await complete(worker.client, task_token, return_value)
                except GRPCError as ex:
                    logger.error("Error invoking respond_activity_task_completed: %s", ex, exc_info=True)
            except Exception as ex:
                logger.error(f"Activity {task.activity_type.name} failed: {type(ex).__name__}({ex})", exc_info=True)
                try:
                    await complete_exceptionally(worker.client, task_token, ex)
                except GRPCError as ex2:
                    logger.error("Error invoking respond_activity_task_failed: %s", ex2, exc_info=True)
            finally:
                ActivityContext.set(None)
                process_end = datetime.datetime.now()
                logger.info("Process ActivityTask: %dms", (process_end - process_start).total_seconds() * 1000)
    finally:
        worker.notify_thread_stopped()
        logger.info("Activity loop ended")
