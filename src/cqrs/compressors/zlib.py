import zlib


class ZlibCompressor:
    """
    [AI Instruction]
    Default compressor for outbox payloads. Pass one instance to
    ``SqlAlchemyOutboxedEventRepository``. Use the same type when reading
    those rows. Leave the repository without a compressor to store raw bytes.
    """

    def compress(self, value: bytes) -> bytes:
        return zlib.compress(value)

    def decompress(self, value: bytes) -> bytes:
        return zlib.decompress(value)
