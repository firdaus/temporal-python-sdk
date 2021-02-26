import inspect
from typing import Union, List, Iterable, Callable

from temporal.api.common.v1 import Payload, Payloads
from temporal.conversions import ENCODINGS, METADATA_ENCODING_KEY, DECODINGS


def get_fn_args_type_hints(fn: Callable) -> List[type]:
    spec = inspect.getfullargspec(fn)
    args = []
    for s in spec.args:
        args.append(spec.annotations.get(s))
    if inspect.ismethod(fn):
        # Remove "self
        args.pop(0)
    return args


def get_fn_ret_type_hints(fn: Callable) -> List[type]:
    spec = inspect.getfullargspec(fn)
    return [spec.annotations.get("return")]


class DataConverter:
    def to_payload(self, arg: object) -> Payload:
        raise NotImplementedError()

    def from_payload(self, payload: Payload, type_hint: type = None) -> object:
        raise NotImplementedError()

    def to_payloads(self, args: Union[object, List[object]]) -> Payloads:
        payloads: Payloads = Payloads()
        payloads.payloads = []
        if isinstance(args, (str, bytes)) or not isinstance(args, Iterable):
            args = [args]
        for arg in args:
            payloads.payloads.append(self.to_payload(arg))
        return payloads

    def from_payloads(self, payloads: Payloads, type_hints: List[type] = []) -> List[object]:
        args: List[object] = []
        for i, payload in enumerate(payloads.payloads):
            try:
                type_hint = type_hints[i]
            except IndexError as ex:
                type_hint = None
            args.append(self.from_payload(payload, type_hint))
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

    def from_payload(self, payload: Payload, type_hint: type = None) -> object:
        encoding: bytes = payload.metadata[METADATA_ENCODING_KEY]
        decoding = DECODINGS.get(encoding)
        if not decoding:
            raise Exception(f"Unsupported encoding: {str(encoding, 'utf-8')}")
        return decoding(payload)


DEFAULT_DATA_CONVERTER_INSTANCE = DefaultDataConverter()
