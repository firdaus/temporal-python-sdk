from typing import Union, List, Iterable

from temporal.api.common.v1 import Payload, Payloads
from temporal.conversions import ENCODINGS, METADATA_ENCODING_KEY, DECODINGS


class DataConverter:
    def to_payload(self, arg: object) -> Payload:
        raise NotImplementedError()

    def from_payload(self, payload: Payload) -> object:
        raise NotImplementedError()

    def to_payloads(self, args: Union[object, List[object]]) -> Payloads:
        payloads: Payloads = Payloads()
        payloads.payloads = []
        if isinstance(args, (str, bytes)) or not isinstance(args, Iterable):
            args = [args]
        for arg in args:
            payloads.payloads.append(self.to_payload(arg))
        return payloads

    def from_payloads(self, payloads: Payloads) -> List[object]:
        args: List[object] = []
        for payload in payloads.payloads:
            args.append(self.from_payload(payload))
        return args

    @staticmethod
    def get_default():
        return DefaultDataConverter()


class DefaultDataConverter(DataConverter):
    def to_payload(self, arg: object) -> Payload:
        for fn in ENCODINGS:
            payload = fn(arg)
            if payload is not None:
                return payload
        raise Exception(f"Object cannot be encoded: {arg}")

    def from_payload(self, payload: Payload) -> object:
        encoding: bytes = payload.metadata[METADATA_ENCODING_KEY]
        decoding = DECODINGS.get(encoding)
        if not decoding:
            raise Exception(f"Unsupported encoding: {str(encoding, 'utf-8')}")
        return decoding(payload)


DEFAULT_DATA_CONVERTER_INSTANCE = DefaultDataConverter()
