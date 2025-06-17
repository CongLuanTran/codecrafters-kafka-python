from itertools import islice
from typing import Any, Self, final, override
from collections.abc import Iterator


@final
class UnsignedVarint:
    def __init__(self, value: int):
        self.value = value

    def __int__(self) -> int:
        return self.value

    def __bytes__(self) -> bytes:
        result = bytearray()
        n = self.value
        while True:
            byte = n & 0x7F
            n >>= 7
            if n:
                result.append(byte | 0x80)
            else:
                result.append(byte)
                break
        return bytes(result)

    @classmethod
    def deserialize(cls, it: Iterator[int]) -> Self:
        result = 0
        shift = 0
        for byte in it:
            result |= (byte & 0x7F) << shift
            if byte & 0x80 == 0:
                return cls(result)
            shift += 7
        raise ValueError("Invalid UnsignedVarint: not enough bytes to deserialize")


@final
class CompactString:
    def __init__(self, value: str | None):
        self.value = value

    @override
    def __str__(self):
        return self.value or ""

    def __bytes__(self) -> bytes:
        result = bytearray()
        if self.value is None:
            result.extend(bytes(UnsignedVarint(0)))
        else:
            encoded = self.value.encode()
            length = len(encoded) + 1
            result.extend(bytes(UnsignedVarint(length)))
            result.extend(encoded)
        return bytes(result)

    @classmethod
    def deserialize(cls, it: Iterator[int]) -> Self:
        length = int(UnsignedVarint.deserialize(it))
        if length == 0:
            return cls(None)
        length = length - 1
        result = bytes(islice(it, length))
        if len(result) != length:
            raise ValueError("Invalid CompactString: not enough bytes to deserialize")
        return cls(result.decode())


@final
class CompactArray:
    def __init__(self, value: list[Any] | None):
        self.value = value

    def __bytes__(self) -> bytes:
        result = bytearray()
        if self.value is None:
            result.extend(bytes(UnsignedVarint(0)))
        else:
            length = len(self.value) + 1
            result.extend(bytes(UnsignedVarint(length)))
            for item in self.value:
                result.extend(bytes(item))
        return bytes(result)

    @classmethod
    def deserialize(cls, it: Iterator[int]) -> Self:
        length = int(UnsignedVarint.deserialize(it))
        if length == 0:
            return cls(None)
        length = length - 1
        result = []
        while len(result) < length:
            result.append(CompactString.deserialize(it))
        return cls(result)
