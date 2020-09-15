import os
import socket

from grpclib.client import Channel

from temporal.api.workflowservice.v1 import WorkflowServiceStub as WorkflowService


def create_workflow_service(host: str, port: int, timeout: float) -> WorkflowService:
    channel = Channel(host=host, port=port)
    return WorkflowService(channel, timeout=timeout)


def get_identity():
    return "%d@%s" % (os.getpid(), socket.gethostname())

