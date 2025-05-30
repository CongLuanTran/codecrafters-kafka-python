class RequestHeader:
    def __init__(self, bytes: bytes):
        self.api_key = bytes[:2]
        self.api_version = bytes[2:4]
        self.correlation_id = bytes[4:8]

        length = bytes[8:10]
        length = int.from_bytes(length, byteorder="big", signed=True)
        self.client_id = None if length < 1 else bytes[10 : 10 + length]

        self.tag_buffer = bytes[10 + length :]

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
