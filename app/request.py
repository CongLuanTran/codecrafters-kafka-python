from abc import ABC, abstractmethod
from collections.abc import Iterator
from itertools import islice
from typing import Self, final


@final
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
    def deserialize(cls, it: Iterator[int]) -> Self:
        api_key = int.from_bytes(bytes(islice(it, 2)), byteorder="big", signed=True)
        api_version = int.from_bytes(bytes(islice(it, 2)), byteorder="big", signed=True)
        correlation_id = int.from_bytes(
            bytes(islice(it, 4)), byteorder="big", signed=True
        )

        length = bytes(islice(it, 2))
        length = int.from_bytes(length, byteorder="big", signed=True)
        client_id = None if length < 1 else bytes(islice(it, length)).decode()
        tag_buffer = bytes(next(it))
        return cls(api_key, api_version, correlation_id, client_id, tag_buffer)


class KafkaRequestBody(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    @classmethod
    def deserialize(cls, it: Iterator[int]) -> Self:
        pass


@final
class KafkaRequest:
    def __init__(self, header: KafkaRequestHeader, body: KafkaRequestBody):
        self.header = header
        self.body = body
