import copy
from asyncio import Future
from typing import List, Union

from temporal.activity_method import ExecuteActivityParameters
from temporal.decision_loop import ActivityFuture


class Async:
    @staticmethod
    def function(method, *args) -> ActivityFuture:
        return Async.function_with_self(method, method.__self__, *args)

    @staticmethod
    def function_with_self(method, self, *args):
        assert self._decision_context
        assert method._execute_parameters
        parameters: ExecuteActivityParameters = copy.deepcopy(method._execute_parameters)
        return Async.call(self, parameters, args)

    @staticmethod
    def call(self, parameters, args: List[object]):
        if hasattr(self, "_activity_options") and self._activity_options:
            self._activity_options.fill_execute_activity_parameters(parameters)
        if self._retry_parameters:
            parameters.retry_parameters = self._retry_parameters
        from temporal.decision_loop import DecisionContext
        decision_context: DecisionContext = self._decision_context
        parameters.input = decision_context.decider.worker.client.data_converter.to_payloads(args)
        return decision_context.schedule_activity_task(parameters=parameters)

    @staticmethod
    async def any_of(futures: List[Union[ActivityFuture, Future]], timeout_seconds=0):
        done, pending = [], []

        def condition():
            done[:] = []
            pending[:] = []
            for f in futures:
                if f.done():
                    done.append(f)
                else:
                    pending.append(f)
            if done:
                return True
            else:
                return False

        await Workflow.await_till(condition, timeout_seconds=timeout_seconds)
        return done, pending

    @staticmethod
    async def all_of(futures: List[Union[ActivityFuture, Future]], timeout_seconds=0):

        def condition():
            for f in futures:
                if not f.done():
                    return False
            return True

        await Workflow.await_till(condition, timeout_seconds=timeout_seconds)


from temporal.workflow import Workflow
