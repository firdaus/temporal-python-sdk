from temporal.api.common.v1 import Payloads
from temporal.converter import DefaultDataConverter


def test_no_payloads():
    converter = DefaultDataConverter()
    p = Payloads(payloads=None)
    assert converter.from_payloads(p) == [None]
