"""In-memory broker that records messages sent by the mediators.

[AI Instruction]
Import ``CapturingMessageBroker`` from ``cqrs.testing``. Pass it to
``bootstrap`` as ``message_broker``, or let ``test_harness`` create one.
``fail_next`` raises on the following ``send_message`` and does not record
that message.
"""

from cqrs.message_brokers.protocol import Message, MessageBroker


class CapturingMessageBroker(MessageBroker):
    """
    Drop-in broker that keeps every delivered message.

    ``fail_next`` schedules an exception for the next ``send_message``.
    The failed send is not appended. An optional ``inner`` broker is called
    after the message is accepted, so a real adapter can sit behind the capture.
    """

    def __init__(self, inner: MessageBroker | None = None) -> None:
        self._inner = inner
        self._messages: list[Message] = []
        self._failures: list[BaseException] = []

    @property
    def messages(self) -> list[Message]:
        return self._messages

    def fail_next(self, error: BaseException) -> None:
        """Raise ``error`` from the next ``send_message``."""

        self._failures.append(error)

    async def send_message(self, message: Message) -> None:
        if self._failures:
            raise self._failures.pop(0)
        if self._inner is not None:
            await self._inner.send_message(message)
        self._messages.append(message)


__all__ = ("CapturingMessageBroker",)
