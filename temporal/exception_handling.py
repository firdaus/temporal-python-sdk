import json
import logging
import traceback
from typing import Optional

import tblib  # type: ignore

from temporal.api.failure.v1 import Failure, ApplicationFailureInfo

THIS_SOURCE = "PythonSDK"

logger = logging.getLogger(__name__)


class ExternalException(Exception):
    def __init__(self, details):
        super().__init__(details)

    @property
    def details(self):
        return self.args[0]


def exception_class_fqn(o):
    # Copied from: https://stackoverflow.com/a/2020083
    module = o.__class__.__module__
    # if module is None or module == str.__class__.__module__:
    #     return o.__class__.__name__  # Avoid reporting __builtin__
    # else:
    return module + '.' + o.__class__.__name__


def import_class_from_string(path):
    # Taken from: https://stackoverflow.com/a/30042585
    from importlib import import_module
    module_path, _, class_name = path.rpartition('.')
    mod = import_module(module_path)
    klass = getattr(mod, class_name)
    return klass


def serialize_exception(ex: BaseException) -> Failure:
    failure = Failure()
    failure.message = str(ex)
    failure.source = THIS_SOURCE
    # TODO: support chaining?
    # failure.cause = ???
    exception_cls_name: str = exception_class_fqn(ex)
    failure.application_failure_info = ApplicationFailureInfo()
    failure.application_failure_info.type = exception_class_fqn(ex)
    tb = "".join(traceback.format_exception(type(ex), ex, ex.__traceback__))
    failure.stack_trace = json.dumps({
        "class": exception_cls_name,
        "args": ex.args,
        "traceback": tb,
    })
    return failure


"""
TODO: Need unit testing for this
"""
def deserialize_exception(details: Failure) -> Exception:
    """
    TODO: Support built-in types like Exception
    """
    exception: Optional[Exception] = None
    source = details.source
    exception_cls_name: str = details.application_failure_info.type

    if source == THIS_SOURCE and exception_cls_name:
        details_dict = json.loads(details.stack_trace)
        try:
            klass = import_class_from_string(exception_cls_name)
            exception = klass(*details_dict["args"])
            traceback_string: str = details_dict["traceback"]
            # when complete_exceptionally() is invoked, the Exception has no
            # traceback, so don't try to deserialize the traceback if there is none
            if len(traceback_string.split("\n")) > 2:
                t = tblib.Traceback.from_string(traceback_string)
                assert exception is not None
                exception.with_traceback(t.as_traceback())
        except Exception as e:
            exception = None
            logger.error("Failed to deserialize exception (details=%s) cause=%r", details_dict, e)

    if not exception:
        # TODO: Better to deserialize details
        return ExternalException(details)
    else:
        return exception


def failure_to_str(f: Failure) -> str:
    return f.to_json()


def str_to_failure(s: str) -> Failure:
    f = Failure()
    return f.from_json(s)
