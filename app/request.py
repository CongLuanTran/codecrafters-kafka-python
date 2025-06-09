from itertools import islice
from typing import Iterator, Self

from app.api_versions import ApiVersionsRequest


class KafkaRequestHeader:
    def __init__(
        self,
        api_key: int,
        api_version: int,
        correlation_id: int,
        client_id: str | None,
        tag_buffer: bytes,
    ):
        self.api_key = api_key
        self.api_version = api_version
        self.correlation_id = correlation_id
        self.client_id = client_id
        self.tag_buffer = tag_buffer

    @classmethod
    def deserialize(cls, it: Iterator) -> Self:
        api_key = int.from_bytes(bytes(islice(it, 2)), byteorder="big", signed=True)
        api_version = int.from_bytes(bytes(islice(it, 2)), byteorder="big", signed=True)
        correlation_id = int.from_bytes(
            bytes(islice(it, 4)), byteorder="big", signed=True
        )

        length = bytes(islice(it, 2))
        length = int.from_bytes(length, byteorder="big", signed=True)
        client_id = None if length < 1 else bytes(islice(it, length)).decode()
        tag_buffer = next(it)
        return cls(api_key, api_version, correlation_id, client_id, tag_buffer)

    @property
    def api_key(self):
        """The api_key property."""
        return self._api_key

    @api_key.setter
    def api_key(self, value: int):
        self._api_key = value

    @property
    def api_version(self):
        """The api_version property."""
        return self._api_version

    @api_version.setter
    def api_version(self, value: int):
        self._api_version = value

    @property
    def correlation_id(self):
        """The correlation_id property."""
        return self._correlation_id

    @correlation_id.setter
    def correlation_id(self, value: int):
        self._correlation_id = value

    @property
    def client_id(self):
        """The client_id property."""
        return self._client_id

    @client_id.setter
    def client_id(self, value: str | None):
        self._client_id = value

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
    def __init__(self, header: KafkaRequestHeader, body: KafkaRequestBody):
        self.header = header
        self.body = body

    @classmethod
    def deserialize(cls, it: Iterator):
        header = KafkaRequestHeader.deserialize(it)
        match header.api_key:
            case 18:
                body = ApiVersionsRequest.deserialize(it)
            case _:
                body = KafkaRequestBody()
        return cls(header, body)

    @property
    def header(self):
        """The header property."""
        return self._header

    @header.setter
    def header(self, value: KafkaRequestHeader):
        self._header = value

    @property
    def body(self):
        """The body property."""
        return self._body

    @body.setter
    def body(self, value: KafkaRequestBody):
        self._body = value
