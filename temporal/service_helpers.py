import os
import socket
import ssl
from typing import List

from dataclasses import dataclass, field
from grpclib.client import Channel

from temporal.api.workflowservice.v1 import WorkflowServiceStub


@dataclass
class TLSOptions:
    alpn_protocols: List = field(default_factory=lambda: ['h2'])
    ca_cert: str = None
    client_cert: str = None
    client_key: str = None
    ciphers: str = "ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20"
    npn_protocols: List = field(default_factory=lambda: ['h2'])


def create_secure_context(tls_options: TLSOptions) -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS)
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.load_cert_chain(str(tls_options.client_cert), str(tls_options.client_key))
    ctx.load_verify_locations(str(tls_options.ca_cert))
    ctx.options |= ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1
    ctx.set_ciphers(tls_options.ciphers)
    ctx.set_alpn_protocols(tls_options.alpn_protocols)
    try:
        ctx.set_npn_protocols(tls_options.npn_protocols)
    except NotImplementedError:
        pass
    return ctx


def create_workflow_service(host: str, port: int, timeout: float, tls_options: TLSOptions) -> WorkflowServiceStub:
    ssl_context = create_secure_context(tls_options) if tls_options is not None else None
    channel = Channel(host=host, port=port, ssl=ssl_context)
    return WorkflowServiceStub(channel, timeout=timeout)


def get_identity():
    return "%d@%s" % (os.getpid(), socket.gethostname())

