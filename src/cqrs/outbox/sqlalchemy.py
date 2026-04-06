import datetime
import logging
import typing

import dotenv
import orjson
import cqrs
import uuid
from cqrs import compressors
from cqrs.outbox import map, repository

try:
    import sqlalchemy

    from sqlalchemy import func
    from sqlalchemy.orm import Mapped, mapped_column, DeclarativeMeta, registry
    from sqlalchemy.ext.asyncio import session as sql_session
    from sqlalchemy.dialects import postgresql
except ImportError:
    raise ImportError(
        "You are trying to use SQLAlchemy outbox implementation, "
        "but 'sqlalchemy' is not installed. "
        "Please install it using: pip install python-cqrs[sqlalchemy]",
    ) from None


Base = registry().generate_base()

logger = logging.getLogger(__name__)

dotenv.load_dotenv()

DEFAULT_OUTBOX_TABLE_NAME = "outbox"

MAX_FLUSH_COUNTER_VALUE = 5


class BinaryUUID(sqlalchemy.TypeDecorator):
    """Stores the UUID as a native UUID in Postgres and as BINARY(16) in other databases (MySQL)."""

    impl = sqlalchemy.BINARY(16)
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(postgresql.UUID())
        else:
            return dialect.type_descriptor(sqlalchemy.BINARY(16))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        if isinstance(value, str):
            value = uuid.UUID(value)
        if dialect.name == "postgresql":
            return value  # asyncpg works with uuid.UUID
        if isinstance(value, uuid.UUID):
            return value.bytes  # For MySQL return 16 bytes
        return value

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        if dialect.name == "postgresql":
            return value  # asyncpg return uuid.UUID
        if isinstance(value, bytes):
            return uuid.UUID(bytes=value)  # From MySQL got bytes, make UUID
        return value


class OutboxModel(Base):
    __tablename__ = DEFAULT_OUTBOX_TABLE_NAME

    __table_args__ = (
        sqlalchemy.UniqueConstraint(
            "event_id_bin",
            "event_name",
            name="event_id_unique_index",
        ),
    )
    id: Mapped[int] = mapped_column(
        sqlalchemy.BigInteger,
        sqlalchemy.Identity(),
        primary_key=True,
        nullable=False,
        autoincrement=True,
        comment="Identity",
    )
    event_id: Mapped[uuid.UUID] = mapped_column(
        BinaryUUID,
        nullable=False,
        comment="Event idempotency id",
    )
    event_id_bin: Mapped[bytes] = mapped_column(
        sqlalchemy.BINARY(16),
        nullable=False,
        comment="Event idempotency id in 16 bit presentation",
    )
    event_status: Mapped[repository.EventStatus] = mapped_column(
        sqlalchemy.Enum(repository.EventStatus),
        nullable=False,
        default=repository.EventStatus.NEW,
        comment="Event producing status",
    )
    flush_counter: Mapped[int] = mapped_column(
        sqlalchemy.SmallInteger,
        nullable=False,
        default=0,
        comment="Event producing flush counter",
    )
    event_name: Mapped[typing.Text] = mapped_column(
        sqlalchemy.String(255),
        nullable=False,
        comment="Event name",
    )
    topic: Mapped[typing.Text] = mapped_column(
        sqlalchemy.String(255),
        nullable=False,
        comment="Event topic",
        default="",
    )
    created_at: Mapped[datetime.datetime] = mapped_column(
        sqlalchemy.DateTime,
        nullable=False,
        server_default=func.now(),
        comment="Event creation timestamp",
    )
    payload: Mapped[bytes] = mapped_column(
        sqlalchemy.LargeBinary,
        nullable=False,
        comment="Event payload",
    )

    def row_to_dict(self) -> typing.Dict[typing.Text, typing.Any]:
        return {column.name: getattr(self, column.name) for column in self.__table__.columns}

    @classmethod
    def get_batch_query(
        cls,
        size: int,
        topic: typing.Optional[typing.Text] = None,
    ) -> sqlalchemy.Select:
        return (
            sqlalchemy.select(cls)
            .select_from(cls)
            .where(
                sqlalchemy.and_(
                    cls.event_status.in_(
                        [
                            repository.EventStatus.NEW,
                            repository.EventStatus.NOT_PRODUCED,
                        ],
                    ),
                    cls.flush_counter < MAX_FLUSH_COUNTER_VALUE,
                    (cls.topic == topic) if topic is not None else sqlalchemy.true(),
                ),
            )
            .order_by(cls.status_sorting_case().asc())
            .order_by(cls.id.asc())
            .limit(size)
        )

    @classmethod
    def update_status_query(
        cls,
        outboxed_event_id: int,
        status: repository.EventStatus,
    ) -> sqlalchemy.Update:
        values = {
            "event_status": status,
            "flush_counter": cls.flush_counter,
        }
        if status == repository.EventStatus.NOT_PRODUCED:
            values["flush_counter"] += 1

        return sqlalchemy.update(cls).where(cls.id == outboxed_event_id).values(**values)

    @classmethod
    def status_sorting_case(cls) -> sqlalchemy.Case:
        return sqlalchemy.case(
            {
                repository.EventStatus.NEW: 1,
                repository.EventStatus.NOT_PRODUCED: 2,
                repository.EventStatus.PRODUCED: 3,
            },
            value=cls.event_status,
            else_=4,
        )


class SqlAlchemyOutboxedEventRepository(repository.OutboxedEventRepository):
    def __init__(
        self,
        session: sql_session.AsyncSession,
        compressor: typing.Optional[compressors.Compressor] = None,
    ):
        self.session = session
        self._compressor = compressor

    def add(
        self,
        event: cqrs.INotificationEvent,
    ) -> None:
        registered_event = map.OutboxedEventMap.get(event.event_name)
        if registered_event is None:
            raise TypeError(f"Unknown event name for {event.event_name}")

        if type(event) is not registered_event:
            raise TypeError(
                f"Event type {type(event)} does not match registered event type {registered_event}",
            )

        bytes_payload = orjson.dumps(event.to_dict())
        if self._compressor is not None:
            bytes_payload = self._compressor.compress(bytes_payload)

        self.session.add(
            OutboxModel(
                event_id=event.event_id,
                event_id_bin=event.event_id.bytes,
                event_name=event.event_name,
                created_at=event.event_timestamp,
                payload=bytes_payload,
                topic=event.topic,
            ),
        )

    def _process_events(self, model: OutboxModel) -> typing.Optional[repository.OutboxedEvent]:
        event_dict = model.row_to_dict()

        event_model = map.OutboxedEventMap.get(event_dict["event_name"])
        if event_model is None:
            return None

        if self._compressor is not None:
            event_dict["payload"] = self._compressor.decompress(event_dict["payload"])
        event_payload_dict = orjson.loads(event_dict["payload"])

        # Use from_dict interface method for validation and type conversion
        # This works through the interface without exposing implementation details
        return repository.OutboxedEvent(
            id=event_dict["id"],
            topic=event_dict["topic"],
            status=event_dict["event_status"],
            event=event_model.from_dict(**event_payload_dict),
        )

    async def get_many(
        self,
        batch_size: int = 100,
        topic: typing.Optional[typing.Text] = None,
    ) -> typing.List[repository.OutboxedEvent]:
        events: typing.Sequence[OutboxModel] = (
            (await self.session.execute(OutboxModel.get_batch_query(batch_size, topic))).scalars().all()
        )

        result = []
        for event in events:
            outboxed_event = self._process_events(event)
            if outboxed_event is None:
                logger.warning(f"Unknown event name for {event.event_name}")
                continue
            result.append(outboxed_event)

        return result

    async def update_status(
        self,
        outboxed_event_id: int,
        new_status: repository.EventStatus,
    ) -> None:
        await self.session.execute(
            statement=OutboxModel.update_status_query(outboxed_event_id, new_status),
        )

    async def commit(self):
        await self.session.commit()

    async def rollback(self):
        await self.session.rollback()


def rebind_outbox_model(
    model: typing.Any,
    new_base: DeclarativeMeta,
    table_name: typing.Optional[typing.Text] = None,
):
    model.__bases__ = (new_base,)
    model.__table__.name = table_name or model.__table__.name
    new_base.metadata._add_table(
        model.__table__.name,
        model.__table__.schema,
        model.__table__,
    )
