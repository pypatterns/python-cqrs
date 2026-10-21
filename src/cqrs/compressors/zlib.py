import zlib


class ZlibCompressor:
    """
    [AI Instruction]
    Optional compressor for outbox payloads. It is not applied unless the
    application passes it in. Supply the same ``ZlibCompressor`` instance to
    ``SqlAlchemyOutboxedEventRepository`` when writing compressed rows and
    when reading them. Leave the repository without a compressor to store
    raw bytes.
    """

    def compress(self, value: bytes) -> bytes:
        return zlib.compress(value)

    def decompress(self, value: bytes) -> bytes:
        return zlib.decompress(value)
