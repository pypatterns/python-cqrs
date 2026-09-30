import typing

if typing.TYPE_CHECKING:
    import aio_pika


class KafkaProducer(typing.Protocol):
    async def produce(
        self,
        topic: typing.Text,
        message: typing.Any,
        headers: dict[str, str] | None = None,
    ) -> None: ...


class AMQPPublisher(typing.Protocol):
    async def publish(
        self,
        message: "aio_pika.abc.AbstractMessage",
        queue_name: str,
        exchange_name: str,
    ) -> None: ...


class AMQPConsumer(typing.Protocol):
    async def consume(
        self,
        handler: typing.Callable[
            ["aio_pika.abc.AbstractIncomingMessage"],
            typing.Awaitable[None],
        ],
        queue_name: str,
    ) -> None: ...
