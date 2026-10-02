import datetime
import logging
import typing

import dotenv
import cqrs
import uuid
from cqrs import compressors
from cqrs.outbox import map, repository
from cqrs.serializers.json import JsonEventSerializer
from cqrs.serializers.protocol import EventCodec

try:
    import sqlalchemy

    from sqlalchemy import func
    from sqlalchemy.orm import Mapped, mapped_column, DeclarativeMeta, registry
    from sqlalchemy.ext.asyncio import session as sql_session

    from cqrs import sqlalchemy_types
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


# Deprecated name of cqrs.sqlalchemy_types.UUIDBinary — kept importable for
# backward compatibility (e.g. already generated Alembic migrations).
# Prefer cqrs.sqlalchemy_types.UUIDBinary in new code.
BinaryUUID = sqlalchemy_types.UUIDBinary


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
        sqlalchemy_types.UUIDBinary,
        nullable=False,
        comment="Event idempotency id",
    )
    event_id_bin: Mapped[bytes] = mapped_column(
        sqlalchemy_types.Binary16,
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
        sqlalchemy_types.PayloadBinary,
        nullable=False,
        comment="Event payload",
    )

    def row_to_dict(self) -> typing.Dict[typing.Text, typing.Any]:
        return {column.name: getattr(self, column.name) for column in self.__table__.columns}

    @classmethod
    def get_batch_query(
        cls,
        size: int,
        topic: typing.Text | None = None,
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
        # Conditions instead of a `value=` mapping: only a comparison with the
        # column binds the statuses through the Enum type. As plain mapping keys
        # they are sent as enum values ("new"), which PostgreSQL rejects because
        # its `eventstatus` type holds member names ("NEW").
        return sqlalchemy.case(
            (cls.event_status == repository.EventStatus.NEW, 1),
            (cls.event_status == repository.EventStatus.NOT_PRODUCED, 2),
            (cls.event_status == repository.EventStatus.PRODUCED, 3),
            else_=4,
        )


class SqlAlchemyOutboxedEventRepository(repository.OutboxedEventRepository):
    def __init__(
        self,
        session: sql_session.AsyncSession,
        compressor: compressors.Compressor | None = None,
        *,
        serializer: EventCodec | None = None,
        event_map: map.OutboxedEventMap | None = None,
    ):
        self.session = session
        self._compressor = compressor
        self._serializer = serializer or JsonEventSerializer()
        self._event_map = event_map if event_map is not None else map.OutboxedEventMap

    def _resolve_codec(self, event_name: str) -> EventCodec:
        return self._event_map.get_serializer(event_name) or self._serializer

    def add(
        self,
        event: cqrs.INotificationEvent,
    ) -> None:
        registered_event = self._event_map.get(event.event_name)
        if registered_event is None:
            raise TypeError(f"Unknown event name for {event.event_name}")

        event_type = type(event)
        registered_origin = typing.get_origin(registered_event) or registered_event
        # Pydantic Model[T] creates a distinct class; dataclass generics do not.
        # Accept exact match, or origin match when registered is a GenericAlias and
        # the instance type is exactly that origin (DCNotificationEvent[T] case).
        if event_type is registered_event:
            pass
        elif typing.get_origin(registered_event) is not None and event_type is registered_origin:
            pass
        elif isinstance(registered_event, type) and issubclass(event_type, registered_event):
            pass
        else:
            raise TypeError(
                f"Event type {event_type} does not match registered event type {registered_event}",
            )

        bytes_payload = self._resolve_codec(event.event_name).serialize(event)
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

    def _process_events(self, model: OutboxModel) -> repository.OutboxedEvent | None:
        event_dict = model.row_to_dict()
        event_name = event_dict["event_name"]

        event_model = self._event_map.get(event_name)
        if event_model is None:
            logger.warning(f"Unknown event name for {event_name}")
            return None

        codec = self._resolve_codec(event_name)
        payload = event_dict["payload"]
        try:
            if self._compressor is not None:
                payload = self._compressor.decompress(payload)
            event = codec.deserialize(payload, event_model)
        except Exception as error:
            logger.warning(
                "Failed to deserialize outbox event %s (id=%s, codec=%s): %s",
                event_name,
                event_dict["id"],
                type(codec).__name__,
                error,
            )
            return None

        return repository.OutboxedEvent(
            id=event_dict["id"],
            topic=event_dict["topic"],
            status=event_dict["event_status"],
            event=event,
            payload_bytes=payload,
            content_type=codec.content_type_for(event),
        )

    async def get_many(
        self,
        batch_size: int = 100,
        topic: typing.Text | None = None,
    ) -> typing.List[repository.OutboxedEvent]:
        events: typing.Sequence[OutboxModel] = (
            (await self.session.execute(OutboxModel.get_batch_query(batch_size, topic))).scalars().all()
        )

        result = []
        for event in events:
            outboxed_event = self._process_events(event)
            if outboxed_event is None:
                # Same budget as broker publish failures: after MAX_FLUSH_COUNTER_VALUE
                # the row leaves the selectable set and stops filling the batch.
                await self.update_status(
                    event.id,
                    repository.EventStatus.NOT_PRODUCED,
                )
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
    table_name: typing.Text | None = None,
):
    model.__bases__ = (new_base,)
    model.__table__.name = table_name or model.__table__.name
    new_base.metadata._add_table(
        model.__table__.name,
        model.__table__.schema,
        model.__table__,
    )
