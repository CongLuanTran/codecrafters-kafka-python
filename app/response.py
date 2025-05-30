class ResponseHeader:
    def __init__(self, correlation_id: int):
        self.correlation_id = correlation_id

    def size(self) -> bytes:
        header_size = len(bytes(self))
        return header_size.to_bytes(4, byteorder="big", signed=True)

    def __bytes__(self) -> bytes:
        return self.correlation_id.to_bytes(4, byteorder="big", signed=True)

    @property
    def correlation_id(self):
        """The correlation_id property."""
        return self._correlation_id

    @correlation_id.setter
    def correlation_id(self, value):
        self._correlation_id = value
