from collections.abc import AsyncIterator

import pytest

from cqrs.transport import InMemoryTransport


@pytest.fixture
async def transport() -> AsyncIterator[InMemoryTransport]:
    """In-memory backend for the shared transport contract."""

    broker = InMemoryTransport()
    yield broker
    await broker.close()
