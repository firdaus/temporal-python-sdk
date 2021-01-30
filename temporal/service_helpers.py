import os
import socket

from grpclib.client import Channel

from temporal.api.workflowservice.v1 import WorkflowServiceStub


def create_workflow_service(host: str, port: int, timeout: float) -> WorkflowServiceStub:
    channel = Channel(host=host, port=port)
    return WorkflowServiceStub(channel, timeout=timeout)


def get_identity():
    return "%d@%s" % (os.getpid(), socket.gethostname())

