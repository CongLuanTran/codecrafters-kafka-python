from itertools import islice
from typing import Iterator

from app.api_versions import ApiVersionsRequest


class KafkaRequestHeader:
    def __init__(self, it: Iterator):
        self.api_key = bytes(islice(it, 2))
        self.api_version = bytes(islice(it, 2))
        self.correlation_id = bytes(islice(it, 4))

        length = bytes(islice(it, 2))
        length = int.from_bytes(length, byteorder="big", signed=True)
        self.client_id = None if length < 1 else bytes(islice(it, length))
        self.tag_buffer = next(it)  # placeholder for tag buffer which is just 0x0 now

    @property
    def api_key(self):
        """The api_key property."""
        return self._api_key

    @api_key.setter
    def api_key(self, value: bytes):
        self._api_key = int.from_bytes(value, byteorder="big", signed=True)

    @property
    def api_version(self):
        """The api_version property."""
        return self._api_version

    @api_version.setter
    def api_version(self, value: bytes):
        self._api_version = int.from_bytes(value, byteorder="big", signed=True)

    @property
    def correlation_id(self):
        """The correlation_id property."""
        return self._correlation_id

    @correlation_id.setter
    def correlation_id(self, value: bytes):
        self._correlation_id = int.from_bytes(value, byteorder="big", signed=True)

    @property
    def client_id(self):
        """The client_id property."""
        return self._client_id

    @client_id.setter
    def client_id(self, value: bytes | None):
        self._client_id = None if not value else value.decode("utf-8")

    @property
    def tag_buffer(self):
        """The tag_buffer property."""
        return self._tag_buffer

    @tag_buffer.setter
    def tag_buffer(self, value: bytes):
        self._tag_buffer = value


class KafkaRequestBody:
    def __init__(self):
        pass

    def __bytes__(self):
        pass


class KafkaRequest:
    def __init__(self, bytes: bytes):
        it = iter(bytes)
        self.header = KafkaRequestHeader(it)
        match self.header.api_key:
            case 18:
                self.body = ApiVersionsRequest(it)
            case _:
                self.body = KafkaRequestBody()

    @property
    def header(self) -> KafkaRequestHeader:
        """The header property."""
        return self._header

    @header.setter
    def header(self, value):
        self._header = value

    @property
    def body(self) -> KafkaRequestBody:
        """The body property."""
        return self._body

    @body.setter
    def body(self, value):
        self._body = value
