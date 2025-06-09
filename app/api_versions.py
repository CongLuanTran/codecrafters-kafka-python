from typing import Iterator
from app.builtins import CompactArray, CompactString
from app.response import KafkaResponseBody


class ApiVersion:
    def __init__(self, api_key, min_version, max_version, tag_buffer):
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
        )
    ]
)


class ApiVersionsRequest:
    def __init__(self, it: Iterator):
        self.client_id = CompactString.deserialize(it)
        self.client_software_version = CompactString.deserialize(it)
        self.tag_buffer = next(it)  # placeholder for tag buffer

    @property
    def client_id(self) -> CompactString:
        """The client_id property."""
        return self._client_id

    @client_id.setter
    def client_id(self, value):
        self._client_id = value

    @property
    def client_software_version(self) -> CompactString:
        """The client_softwaere_version property."""
        return self._client_softwaere_version

    @client_software_version.setter
    def client_software_version(self, value):
        self._client_softwaere_version = value

    @property
    def tag_buffer(self) -> CompactString:
        """The tag_buffer property."""
        return self._tag_buffer

    @tag_buffer.setter
    def tag_buffer(self, value):
        self._tag_buffer = value


class ApiVersionsResponse(KafkaResponseBody):
    def __init__(self, request_api_version):
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

    @property
    def error_code(self) -> int:
        """The error_code property."""
        return self._error_code

    @error_code.setter
    def error_code(self, value):
        self._error_code = value

    @property
    def api_versions(self) -> CompactArray:
        """The api_versions property."""
        return self._api_versions

    @api_versions.setter
    def api_versions(self, value):
        self._api_versions = value
