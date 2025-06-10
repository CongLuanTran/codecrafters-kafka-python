from typing import Iterator, Self
from app.builtins import CompactArray, CompactString
from app.request import KafkaRequestBody
from app.response import KafkaResponseBody


class ApiVersion:
    def __init__(
        self,
        api_key: int,
        min_version: int,
        max_version: int,
        tag_buffer: bytes,
    ):
        self.api_key = api_key
        self.min_version = min_version
        self.max_version = max_version
        self.tag_buffer = tag_buffer

    def __bytes__(self):
        b = bytearray()
        b.extend(self.api_key.to_bytes(2, byteorder="big", signed=True))
        b.extend(self.min_version.to_bytes(2, byteorder="big", signed=True))
        b.extend(self.max_version.to_bytes(2, byteorder="big", signed=True))
        b.extend(self.tag_buffer)
        return bytes(b)


API_VERSIONS = CompactArray(
    [
        ApiVersion(
            api_key=18,
            min_version=0,
            max_version=4,
            tag_buffer=bytes([0x00]),
        ),
        ApiVersion(
            api_key=75,
            min_version=0,
            max_version=0,
            tag_buffer=bytes([0x00]),
        ),
    ]
)


class ApiVersionsRequest(KafkaRequestBody):
    def __init__(
        self,
        client_id: CompactString,
        client_software_version: CompactString,
        tag_buffer: bytes,
    ):
        self.client_id = client_id
        self.client_software_version = client_software_version
        self.tag_buffer = tag_buffer

    @classmethod
    def deserialize(cls, it: Iterator) -> Self:
        client_id = CompactString.deserialize(it)
        client_software_version = CompactString.deserialize(it)
        tag_buffer = next(it)  # placeholder for tag buffer
        return cls(client_id, client_software_version, tag_buffer)


class ApiVersionsResponse(KafkaResponseBody):
    def __init__(self, request_api_version: int):
        if 0 <= request_api_version <= 4:
            self.error_code = 0
            self.api_versions = API_VERSIONS
        else:
            self.error_code = 35
            self.api_versions = CompactArray(None)
        self.throttle_time = 0
        self.tag_buffer = bytes([0x00])

    def __bytes__(self):
        b = bytearray()
        b.extend(self.error_code.to_bytes(2, byteorder="big", signed=True))
        b.extend(bytes(self.api_versions))
        b.extend(self.throttle_time.to_bytes(4, byteorder="big", signed=True))
        b.extend(self.tag_buffer)
        return bytes(b)

    def size(self):
        return len(bytes(self))
