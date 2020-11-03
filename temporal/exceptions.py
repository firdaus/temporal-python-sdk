from dataclasses import dataclass

from temporal.api.common.v1 import WorkflowExecution
from temporal.api.enums.v1 import TimeoutType, WorkflowExecutionStatus
from temporal.api.failure.v1 import Failure
from temporal.exception_handling import deserialize_exception, str_to_failure


class IllegalStateException(BaseException):
    pass


class IllegalArgumentException(BaseException):
    pass


class WorkflowTypeNotFound(Exception):
    pass


class NonDeterministicWorkflowException(BaseException):
    pass


class ActivityTaskFailedException(Exception):

    def __init__(self, reason: str, cause: Exception) -> None:
        super().__init__(reason)
        self.reason = reason
        self.cause = cause


class ActivityTaskTimeoutException(Exception):

    def __init__(self, event_id: int, timeout_type: TimeoutType, details: bytes, *args: object) -> None:
        super().__init__(*args)
        self.details = details
        self.timeout_type = timeout_type
        self.event_id = event_id


class SignalNotFound(Exception):
    pass


class QueryNotFound(Exception):
    pass


class QueryDidNotComplete(Exception):
    pass

class CancellationException(Exception):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cause = None

    def init_cause(self, cause):
        self.cause = cause


class ActivityCancelledException(Exception):
    pass


class WorkflowOperationException(Exception):
    def __init__(self, event_id: int):
        self.event_id = event_id


class ActivityException(WorkflowOperationException):
    def __init__(self, event_id: int, activity_type: str, activity_id: str):
        super().__init__(event_id=event_id)
        self.activity_type = activity_type
        self.activity_id = activity_id


class ActivityFailureException(ActivityException):
    """
    cause should be the result of failure_to_str()
    Note: Don't change cause to be of type Failure, it needs to be an "str" to make it easier to handle serialization
    of Exceptions.
    """
    def __init__(self, event_id: int, activity_type: str, activity_id: str, cause: str):
        super().__init__(event_id, activity_type, activity_id)
        self.cause: str = cause
        self.attempt: int = None
        self.backoff: int = 0

    # def set_cause(self):
    #     if self.cause:
    #         cause_ex = deserialize_exception(self.cause)
    #         self.__cause__ = cause_ex

    def get_cause(self):
        if self.cause:
            f: Failure = str_to_failure(self.cause)
            return deserialize_exception(f)
        else:
            return None


@dataclass
class WorkflowException(Exception):
    workflow_type: str = None
    execution: WorkflowExecution = None

    def __str__(self):
        return f'{type(self).__name__}: WorkflowType="{self.workflow_type}", ' \
               f'WorkflowID="{self.execution.workflow_id}", RunID="{self.execution.run_id} '


@dataclass
class WorkflowFailureException(WorkflowException):
    decision_task_completed_event_id: int = None


@dataclass
class QueryFailureException(Exception):
    query_type: str = None
    execution: WorkflowExecution = None

    def __str__(self):
        return f'{type(self).__name__}: QueryType="{self.query_type}", ' \
               f'WorkflowID="{self.execution.workflow_id}", RunID="{self.execution.run_id} '



class QueryRejectedException(Exception):
    close_status: WorkflowExecutionStatus

    def __init__(self, close_status: WorkflowExecutionStatus):
        self.close_status = close_status
