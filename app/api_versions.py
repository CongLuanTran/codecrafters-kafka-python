from app.response import KafkaResponseBody


class ApiVersionsResponse(KafkaResponseBody):
    def __init__(self):
        self.error_code = 35

    def __bytes__(self):
        return self.error_code.to_bytes(2, byteorder="big", signed=True)

    def size(self):
        return len(bytes(self))

    @property
    def error_code(self):
        """The error_code property."""
        return self._error_code

    @error_code.setter
    def error_code(self, value):
        self._error_code = value
