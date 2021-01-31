import json
import re
from typing import List, Optional, Union, Iterable

from temporal.api.common.v1 import Payload, Payloads


def camel_to_snake(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def snake_to_camel(snake_str):
    components = snake_str.split('_')
    # We capitalize the first letter of each component except the first one
    # with the 'title' method and join them together.
    return components[0] + ''.join(x.title() for x in components[1:])


def snake_to_title(snake_str):
    components = snake_str.split('_')
    return ''.join(x.title() for x in components)


METADATA_ENCODING_KEY = "encoding"

METADATA_ENCODING_NULL_NAME = "binary/null"
METADATA_ENCODING_NULL = METADATA_ENCODING_NULL_NAME.encode("utf-8")
METADATA_ENCODING_RAW_NAME = "binary/plain"
METADATA_ENCODING_RAW = METADATA_ENCODING_RAW_NAME.encode("utf-8")
METADATA_ENCODING_JSON_NAME = "json/plain"
METADATA_ENCODING_JSON = METADATA_ENCODING_JSON_NAME.encode("utf-8")

# TODO: Implement encode/decode for these:
METADATA_ENCODING_PROTOBUF_JSON_NAME = "json/protobuf"
METADATA_ENCODING_PROTOBUF_JSON = METADATA_ENCODING_PROTOBUF_JSON_NAME.encode("utf-8")
METADATA_ENCODING_PROTOBUF_NAME = "binary/protobuf"
METADATA_ENCODING_PROTOBUF = METADATA_ENCODING_PROTOBUF_NAME.encode('utf-8')


def encode_null(value: object) -> Optional[Payload]:
    if value is None:
        p: Payload = Payload()
        p.metadata = {METADATA_ENCODING_KEY: METADATA_ENCODING_NULL}
        p.data = bytes()
        return p
    else:
        return None


# noinspection PyUnusedLocal
def decode_null(payload: Payload) -> object:
    return None


def encode_binary(value: object) -> Optional[Payload]:
    if isinstance(value, bytes):
        p: Payload = Payload()
        p.metadata = {METADATA_ENCODING_KEY: METADATA_ENCODING_RAW}
        p.data = value
        return p
    else:
        return None


def decode_binary(payload: Payload) -> object:
    return payload.data


def encode_json_string(value: object) -> Payload:
    # TODO:
    # mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    # mapper.registerModule(new JavaTimeModule());
    # mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    p: Payload = Payload()
    p.metadata = {METADATA_ENCODING_KEY: METADATA_ENCODING_JSON}
    p.data = json.dumps(value).encode("utf-8")
    return p


def decode_json_string(payload: Payload) -> object:
    # TODO:
    # mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    # mapper.registerModule(new JavaTimeModule());
    # mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    b = str(payload.data, "utf-8")
    return json.loads(b)


ENCODINGS = [
    encode_null,
    encode_binary,
    encode_json_string
]


DECODINGS = {
    METADATA_ENCODING_NULL: decode_null,
    METADATA_ENCODING_RAW: decode_binary,
    METADATA_ENCODING_JSON: decode_json_string
}

