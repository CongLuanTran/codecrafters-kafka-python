from itertools import islice
from typing import Self, final, override
from collections.abc import Iterator
from app.builtins import CompactArray
from app.request import KafkaRequestBody
from app.response import KafkaResponseBody


@final
class DescribeTopicPartitionsRequest(KafkaRequestBody):
    def __init__(
        self,
        topics: CompactArray,
        response_partition_limit: int,
        cursor: bytes,
        tag_buffer: bytes,
    ):
        self.topics = topics
        self.response_partition_limit = response_partition_limit
        self.cursor = cursor
        self.tag_buffer = tag_buffer

    @override
    @classmethod
    def deserialize(cls, it: Iterator[int]) -> Self:
        topics = CompactArray.deserialize(it)
        response_partition_limit = int.from_bytes(
            bytes(islice(it, 4)), byteorder="big", signed=True
        )
        cursor = bytes(next(it))
        tag_buffer = bytes(next(it))
        return cls(topics, response_partition_limit, cursor, tag_buffer)


@final
class DescribeTopicPartitionsResponse(KafkaResponseBody):
    def __init__(
        self,
        throttle_time: int,
        topics: CompactArray,
        next_cursor: bytes,
        tag_buffer: bytes,
    ):
        self.throttle_time = throttle_time
        self.topics = topics
        self.next_cursor = next_cursor
        self.tag_buffer = tag_buffer

    @override
    def __bytes__(self):
        b = bytearray()
        b.extend(self.throttle_time.to_bytes(4, byteorder="big", signed=True))
        b.extend(bytes(self.topics))
        b.extend(self.next_cursor)
        b.extend(self.tag_buffer)
        return bytes(b)

    @override
    def size(self) -> int:
        return len(bytes(self))
