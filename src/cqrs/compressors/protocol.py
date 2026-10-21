import typing


class Compressor(typing.Protocol):
    """
    [AI Instruction]
    Optional compression for outbox payloads. Pass an implementation, usually
    ``ZlibCompressor``, to the outbox repository. The same compressor must
    read what it wrote. Omit it to store payloads uncompressed.
    """

    def compress(self, value: bytes) -> bytes:
        """
        Compress value.

        [AI Instruction]
        Shrink outbox payload bytes before they are stored. The repository
        calls this. Handlers do not.
        """
        raise NotImplementedError

    def decompress(self, value: bytes) -> bytes:
        """
        Decompress compressed value.

        [AI Instruction]
        Restore bytes ``compress`` wrote. The repository calls this when
        reading a row.
        """
        raise NotImplementedError
