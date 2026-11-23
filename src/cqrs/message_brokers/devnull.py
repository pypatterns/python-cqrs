"""
Broker that records a message and does not deliver it.

``MESSAGE_BUS`` is deprecated in favor of ``cqrs.transport.InMemoryTransport``.
Do not read ``MESSAGE_BUS`` when a subscriber must see the message. The list
and ``DevnullMessageBroker`` stay for existing callers.
"""

import logging

from cqrs.message_brokers import protocol

logger = logging.getLogger("cqrs")

MESSAGE_BUS = []


class DevnullMessageBroker(protocol.MessageBroker):
    """
    Drop a published message.

    ``MESSAGE_BUS`` is deprecated. Use ``InMemoryTransport`` when the test or
    the local process needs the message to reach a subscriber.
    """

    async def send_message(self, message: protocol.Message) -> None:
        MESSAGE_BUS.append(message)
        logger.warning(f"Event {message} will be skip")
