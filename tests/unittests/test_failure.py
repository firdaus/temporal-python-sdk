import traceback

from temporal.api.failure.v1 import Failure
from temporal.exception_handling import failure_to_str, str_to_failure, serialize_exception, deserialize_exception
from temporal.exceptions import ActivityTaskTimeoutException


def test_serialize_exception_without_traceback():
    e: Exception = Exception()
    assert e.__traceback__ is None
    f: Failure = serialize_exception(e)
    assert isinstance(f, Failure)
    s = failure_to_str(f)
    assert isinstance(s, str)
    f2: Failure = str_to_failure(s)
    assert isinstance(f2, Failure)
    assert f.to_dict() == f2.to_dict()
    e2 = deserialize_exception(f2)
    assert isinstance(e2, Exception)
    assert e2.__traceback__ is None


def test_deserialize_exception_with_traceback():
    try:
        raise Exception("blah")
    except Exception as ex:
        e = ex
    assert e.__traceback__ is not None
    f: Failure = serialize_exception(e)
    assert isinstance(f, Failure)
    s = failure_to_str(f)
    assert isinstance(s, str)
    f2: Failure = str_to_failure(s)
    assert isinstance(f2, Failure)
    assert f.to_dict() == f2.to_dict()
    e2 = deserialize_exception(f2)
    assert isinstance(e2, Exception)
    assert e2.__traceback__ is not None
    assert traceback.format_tb(e.__traceback__) == traceback.format_tb(e2.__traceback__)


# TODO: Add tests to ensure that other exception types are serializable as well
def test_serialize_deserialize_activity_task_timeout_exception():
    e1 = ActivityTaskTimeoutException(None, None, None)
    f = serialize_exception(e1)
    e2 = deserialize_exception(f)
    assert isinstance(e2, ActivityTaskTimeoutException)
