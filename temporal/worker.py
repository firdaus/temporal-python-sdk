import asyncio
from dataclasses import dataclass, field
from typing import Callable, Dict, Tuple
import inspect
import logging

from temporal.constants import DEFAULT_SOCKET_TIMEOUT_SECONDS
from temporal.conversions import camel_to_snake, snake_to_camel, snake_to_title

from .workflow import WorkflowMethod, SignalMethod, QueryMethod, WorkflowClient

logger = logging.getLogger(__name__)


@dataclass
class WorkerOptions:
    pass


def _find_interface_class(impl_cls) -> type:
    hierarchy = list(inspect.getmro(impl_cls))
    hierarchy.reverse()
    hierarchy.pop(0)  # remove object
    for cls in hierarchy:
        for method_name, fn in inspect.getmembers(cls, predicate=inspect.isfunction):
            # first class with a "_workflow_method" is considered the interface
            if hasattr(fn, "_workflow_method"):
                return cls
    return impl_cls


def _find_metadata_field(cls, metadata_field, method_name):
    for c in inspect.getmro(cls):
        if not hasattr(c, method_name):
            continue
        m = getattr(c, method_name)
        if not hasattr(m, metadata_field):
            continue
        return getattr(m, metadata_field)
    return None


def _get_wm(cls: type, method_name: str) -> WorkflowMethod:
    metadata_field = "_workflow_method"
    return _find_metadata_field(cls, metadata_field, method_name)


def _get_sm(cls: type, method_name: str) -> SignalMethod:
    metadata_field = "_signal_method"
    return _find_metadata_field(cls, metadata_field, method_name)


def _get_qm(cls: type, method_name: str) -> QueryMethod:
    metadata_field = "_query_method"
    return _find_metadata_field(cls, metadata_field, method_name)


@dataclass
class Worker:
    client: WorkflowClient
    namespace: str = None
    task_queue: str = None
    options: WorkerOptions = None
    activities: Dict[str, Callable] = field(default_factory=dict)
    workflow_methods: Dict[str, Tuple[type, Callable]] = field(default_factory=dict)
    threads_started: int = 0
    threads_stopped: int = 0
    stop_requested: bool = False
    timeout: int = DEFAULT_SOCKET_TIMEOUT_SECONDS
    num_activity_tasks = 1
    num_worker_tasks = 1

    def register_activities_implementation(self, activities_instance: object, activities_cls_name: str = None):
        cls_name = activities_cls_name if activities_cls_name else type(activities_instance).__name__
        for method_name, fn in inspect.getmembers(activities_instance, predicate=inspect.ismethod):
            if method_name.startswith("_"):
                continue
            self.activities[f'{cls_name}::{camel_to_snake(method_name)}'] = fn
            self.activities[f'{cls_name}::{snake_to_camel(method_name)}'] = fn
            self.activities[f'{cls_name}::{snake_to_title(method_name)}'] = fn

    def register_workflow_implementation_type(self, impl_cls: type, workflow_cls_name: str = None):
        cls_name = workflow_cls_name if workflow_cls_name else _find_interface_class(impl_cls).__name__
        if not hasattr(impl_cls, "_signal_methods"):
            impl_cls._signal_methods = {}  # type: ignore
        if not hasattr(impl_cls, "_query_methods"):
            impl_cls._query_methods = {}  # type: ignore
        for method_name, fn in inspect.getmembers(impl_cls, predicate=inspect.isfunction):
            wm: WorkflowMethod = _get_wm(impl_cls, method_name)
            if wm:
                impl_fn = getattr(impl_cls, method_name)
                self.workflow_methods[wm._name] = (impl_cls, impl_fn)
                if "::" in wm._name:
                    _, method_name = wm._name.split("::")
                    self.workflow_methods[f'{cls_name}::{camel_to_snake(method_name)}'] = (impl_cls, impl_fn)
                    self.workflow_methods[f'{cls_name}::{snake_to_camel(method_name)}'] = (impl_cls, impl_fn)
                continue
            sm: SignalMethod = _get_sm(impl_cls, method_name)
            if sm:
                impl_fn = getattr(impl_cls, method_name)
                impl_cls._signal_methods[sm.name] = impl_fn  # type: ignore
                if "::" in sm.name:
                    _, method_name = sm.name.split("::")
                    impl_cls._signal_methods[f'{cls_name}::{camel_to_snake(method_name)}'] = impl_fn  # type: ignore
                    impl_cls._signal_methods[f'{cls_name}::{snake_to_camel(method_name)}'] = impl_fn  # type: ignore
                continue
            qm: QueryMethod = _get_qm(impl_cls, method_name)
            if qm:
                impl_fn = getattr(impl_cls, method_name)
                impl_cls._query_methods[qm.name] = impl_fn  # type: ignore
                if "::" in qm.name:
                    _, method_name = qm.name.split("::")
                    impl_cls._query_methods[f'{cls_name}::{camel_to_snake(method_name)}'] = impl_fn  # type: ignore
                    impl_cls._query_methods[f'{cls_name}::{snake_to_camel(method_name)}'] = impl_fn  # type: ignore

    def start(self):
        from .activity_loop import activity_task_loop_func
        from .decision_loop import decision_task_loop_func
        self.threads_stopped = 0
        self.threads_started = 0
        self.stop_requested = False
        if self.activities:
            for i in range(0, self.num_activity_tasks):
                asyncio.create_task(activity_task_loop_func(self))
                self.threads_started += 1
        if self.workflow_methods:
            for i in range(0, self.num_worker_tasks):
                decision_task_loop_func(self)
                self.threads_started += 1

    async def stop(self, background=False):
        self.stop_requested = True
        if background:
            return
        else:
            while self.threads_stopped != self.threads_started:
                await asyncio.sleep(5)

    def is_stopped(self):
        return self.threads_stopped == self.threads_started

    def is_stop_requested(self):
        return self.stop_requested

    def notify_thread_stopped(self):
        self.threads_stopped += 1

    def get_workflow_method(self, workflow_type_name: str) -> Tuple[type, Callable]:
        return self.workflow_methods[workflow_type_name]

    def set_timeout(self, timeout):
        self.timeout = timeout

    def get_timeout(self):
        return self.timeout

    def raise_if_stop_requested(self):
        if self.is_stop_requested():
            raise StopRequestedException()


class StopRequestedException(Exception):
    pass
