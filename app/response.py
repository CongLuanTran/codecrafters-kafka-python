from abc import ABC, abstractmethod


class KafkaResponseHeader:
    def __init__(self, correlation_id: int):
        self.correlation_id = correlation_id

    def __bytes__(self) -> bytes:
        return self.correlation_id.to_bytes(4, "big", signed=True)

    def size(self):
        return len(bytes(self))


class KafkaResponseBody(ABC):
    @abstractmethod
    def __bytes__(self) -> bytes:
        pass

    @abstractmethod
    def size(self) -> int:
        pass


class KafkaResponse:
    def __init__(self, header: KafkaResponseHeader, body: KafkaResponseBody | None):
        self.header = header
        self.body = body

    def __bytes__(self):
        return bytes(self.header) + (bytes(self.body) if self.body else b"")

    def size(self):
        total_size = self.header.size() + (self.body.size() if self.body else 0)
        return total_size.to_bytes(4, "big", signed=True)
