"""DDL of the outbox model under different dialects.

The outbox model is built from the dialect-aware types of
`cqrs.sqlalchemy_types`, so the same model has to render as a native type of
every supported DBMS. The MySQL part of the suite also pins the backward
compatibility invariant: the DDL produced for MySQL must stay exactly the one
produced before the types were introduced.
"""

import typing
import uuid

import pytest
import sqlalchemy
from sqlalchemy import func
from sqlalchemy.dialects import oracle
from sqlalchemy.schema import CreateTable

from cqrs.outbox import repository
from cqrs.outbox.sqlalchemy import OutboxModel, SqlAlchemyOutboxedEventRepository
from cqrs.sqlalchemy_types import (
    Binary16,
    DialectAwareType,
    DialectTypeHandler,
    UUIDBinary,
)


def dialect_by_name(name: typing.Text) -> typing.Any:
    """Returns a driverless dialect instance usable for DDL compilation."""
    return sqlalchemy.create_mock_engine(f"{name}://", executor=None).dialect


def compile_outbox_ddl(dialect: typing.Any) -> typing.Text:
    return str(CreateTable(OutboxModel.__table__).compile(dialect=dialect))


def legacy_outbox_table() -> sqlalchemy.Table:
    """The outbox table as it was declared before `cqrs.sqlalchemy_types`.

    `event_id` was a `TypeDecorator` with `BINARY(16)` as its MySQL impl and
    `event_id_bin` was a plain `sqlalchemy.BINARY(16)`.
    """
    return sqlalchemy.Table(
        "outbox",
        sqlalchemy.MetaData(),
        sqlalchemy.Column(
            "id",
            sqlalchemy.BigInteger,
            sqlalchemy.Identity(),
            primary_key=True,
            nullable=False,
            autoincrement=True,
            comment="Identity",
        ),
        sqlalchemy.Column(
            "event_id",
            sqlalchemy.BINARY(16),
            nullable=False,
            comment="Event idempotency id",
        ),
        sqlalchemy.Column(
            "event_id_bin",
            sqlalchemy.BINARY(16),
            nullable=False,
            comment="Event idempotency id in 16 bit presentation",
        ),
        sqlalchemy.Column(
            "event_status",
            sqlalchemy.Enum(repository.EventStatus),
            nullable=False,
            comment="Event producing status",
        ),
        sqlalchemy.Column(
            "flush_counter",
            sqlalchemy.SmallInteger,
            nullable=False,
            comment="Event producing flush counter",
        ),
        sqlalchemy.Column(
            "event_name",
            sqlalchemy.String(255),
            nullable=False,
            comment="Event name",
        ),
        sqlalchemy.Column(
            "topic",
            sqlalchemy.String(255),
            nullable=False,
            comment="Event topic",
        ),
        sqlalchemy.Column(
            "created_at",
            sqlalchemy.DateTime,
            nullable=False,
            server_default=func.now(),
            comment="Event creation timestamp",
        ),
        sqlalchemy.Column(
            "payload",
            sqlalchemy.LargeBinary,
            nullable=False,
            comment="Event payload",
        ),
        sqlalchemy.UniqueConstraint(
            "event_id_bin",
            "event_name",
            name="event_id_unique_index",
        ),
    )


class TestPostgresqlDDL:
    """PostgreSQL has no BINARY type, this is exactly what issue #85 was about."""

    def test_binary_columns_are_native_postgresql_types(self):
        ddl = compile_outbox_ddl(dialect_by_name("postgresql"))

        assert "event_id UUID NOT NULL" in ddl
        assert "event_id_bin BYTEA NOT NULL" in ddl

    def test_no_binary_type_in_ddl(self):
        ddl = compile_outbox_ddl(dialect_by_name("postgresql"))

        assert "BINARY" not in ddl

    def test_unique_constraint_is_kept(self):
        ddl = compile_outbox_ddl(dialect_by_name("postgresql"))

        assert "CONSTRAINT event_id_unique_index UNIQUE (event_id_bin, event_name)" in ddl


class TestMysqlDDL:
    def test_binary_columns_are_binary16(self):
        ddl = compile_outbox_ddl(dialect_by_name("mysql"))

        assert "event_id BINARY(16) NOT NULL" in ddl
        assert "event_id_bin BINARY(16) NOT NULL" in ddl

    def test_unique_constraint_is_kept(self):
        ddl = compile_outbox_ddl(dialect_by_name("mysql"))

        assert "CONSTRAINT event_id_unique_index UNIQUE (event_id_bin, event_name)" in ddl

    def test_ddl_is_unchanged_against_legacy_model(self):
        """Backward compatibility: existing MySQL installations need no migration."""
        dialect = dialect_by_name("mysql")

        assert compile_outbox_ddl(dialect) == str(
            CreateTable(legacy_outbox_table()).compile(dialect=dialect),
        )

    def test_mariadb_matches_mysql(self):
        assert "event_id_bin BINARY(16) NOT NULL" in compile_outbox_ddl(
            dialect_by_name("mariadb"),
        )


class TestSqliteDDL:
    """Any dialect the library knows nothing about falls back to LargeBinary(16)."""

    def test_binary_columns_are_blobs(self):
        ddl = compile_outbox_ddl(dialect_by_name("sqlite"))

        assert "event_id BLOB NOT NULL" in ddl
        assert "event_id_bin BLOB NOT NULL" in ddl
        assert "BINARY" not in ddl


class TestRegisterDialect:
    @pytest.fixture(autouse=True)
    def restore_registries(self):
        """`register_dialect` mutates a class level registry, undo it afterwards."""
        saved = {
            Binary16: dict(Binary16._handlers),
            UUIDBinary: dict(UUIDBinary._handlers),
        }
        yield
        for type_, handlers in saved.items():
            type_._handlers = handlers

    def test_custom_dialect_type_is_used_in_ddl(self):
        Binary16.register_dialect(
            "oracle",
            DialectTypeHandler(type_factory=lambda dialect: oracle.RAW(16)),
        )
        UUIDBinary.register_dialect(
            "oracle",
            DialectTypeHandler(
                type_factory=lambda dialect: oracle.RAW(16),
                bind=lambda value: value.bytes,
                result=lambda value: uuid.UUID(bytes=value),
            ),
        )

        ddl = compile_outbox_ddl(oracle.base.OracleDialect())

        assert "event_id RAW(16) NOT NULL" in ddl
        assert "event_id_bin RAW(16) NOT NULL" in ddl

    def test_unknown_dialect_falls_back_to_default_handler(self):
        assert Binary16.get_handler("oracle") is Binary16.default_handler

    def test_registration_does_not_leak_between_types(self):
        class CustomBinary(DialectAwareType):
            default_handler = DialectTypeHandler(
                type_factory=lambda dialect: sqlalchemy.LargeBinary(16),
            )

        handler = DialectTypeHandler(type_factory=lambda dialect: oracle.RAW(16))
        CustomBinary.register_dialect("oracle", handler)

        assert CustomBinary.get_handler("oracle") is handler
        assert Binary16.get_handler("oracle") is Binary16.default_handler
        assert UUIDBinary.get_handler("oracle") is UUIDBinary.default_handler


class TestBatchQueryStatusLiterals:
    """The `eventstatus` enum stores member names, so queries must bind names."""

    @pytest.mark.parametrize("dialect_name", ["postgresql", "mysql", "sqlite"])
    def test_statuses_are_bound_as_member_names(self, dialect_name):
        query = OutboxModel.get_batch_query(10)

        sql = str(
            query.compile(
                dialect=dialect_by_name(dialect_name),
                compile_kwargs={"literal_binds": True},
            ),
        )

        for name in ("NEW", "NOT_PRODUCED", "PRODUCED"):
            assert f"'{name}'" in sql
        for value in ("'new'", "'not_produced'", "'produced'"):
            assert value not in sql


class TestBatchQuerySkipLocked:
    """``lock=True`` is the row lock the repository turns on for capable dialects."""

    @pytest.mark.parametrize("dialect_name", ["postgresql", "mysql", "mariadb"])
    def test_lock_compiles_to_for_update_skip_locked(self, dialect_name):
        query = OutboxModel.get_batch_query(10, lock=True)

        sql = str(query.compile(dialect=dialect_by_name(dialect_name)))

        assert "FOR UPDATE SKIP LOCKED" in sql

    @pytest.mark.parametrize("dialect_name", ["postgresql", "mysql", "mariadb", "sqlite"])
    def test_unlocked_query_omits_the_clause(self, dialect_name):
        query = OutboxModel.get_batch_query(10, lock=False)

        sql = str(query.compile(dialect=dialect_by_name(dialect_name)))

        assert "FOR UPDATE" not in sql
        assert "SKIP LOCKED" not in sql


class _Dialect:
    def __init__(
        self,
        name: str,
        server_version_info: tuple[int, ...] | None,
        *,
        is_mariadb: bool = False,
    ):
        self.name = name
        self.server_version_info = server_version_info
        self.is_mariadb = is_mariadb


class _Result:
    def scalars(self) -> "_Result":
        return self

    def all(self) -> list[object]:
        return []


class _Session:
    """Stand-in for the bind dialect and the async connection that initializes it."""

    def __init__(
        self,
        dialect: _Dialect,
        on_connect: typing.Callable[[_Dialect], None] | None = None,
    ):
        self.dialect = dialect
        self.on_connect = on_connect
        self.connected = False
        self.statement: typing.Any = None

    def get_bind(self) -> "_Session":
        return self

    async def connection(self) -> None:
        self.connected = True
        if self.on_connect is not None:
            self.on_connect(self.dialect)

    async def execute(self, statement: typing.Any) -> _Result:
        self.statement = statement
        return _Result()


def _query_locks(statement: typing.Any) -> bool:
    sql = str(statement.compile(dialect=dialect_by_name("postgresql")))
    return "FOR UPDATE SKIP LOCKED" in sql


def _repository(session: _Session) -> SqlAlchemyOutboxedEventRepository:
    return SqlAlchemyOutboxedEventRepository(typing.cast(typing.Any, session))


class TestSkipLockedDialectVersions:
    """Version gate for FOR UPDATE SKIP LOCKED. No live server."""

    @pytest.mark.parametrize(
        ("name", "version", "is_mariadb", "locked"),
        [
            ("mysql", (5, 7), False, False),
            ("mysql", (8, 0), False, True),
            ("mariadb", (10, 5), False, False),
            ("mariadb", (10, 6), False, True),
            ("mysql", (10, 5), True, False),
            ("mysql", (10, 6), True, True),
            ("postgresql", None, False, True),
            ("sqlite", (3, 40, 1), False, False),
        ],
    )
    async def test_known_version_selects_the_lock(self, name, version, is_mariadb, locked):
        session = _Session(_Dialect(name, version, is_mariadb=is_mariadb))
        repository = _repository(session)

        await repository.get_many()

        assert _query_locks(session.statement) is locked
        assert session.connected is False

    async def test_mysql_8_locks_once_the_connection_initializes_the_dialect(self):
        def initialize(dialect: _Dialect) -> None:
            dialect.server_version_info = (8, 0)

        session = _Session(_Dialect("mysql", None), on_connect=initialize)
        repository = _repository(session)

        await repository.get_many()

        assert session.connected is True
        assert _query_locks(session.statement) is True

    async def test_mariadb_flag_is_read_after_the_dialect_is_initialized(self):
        """``(10, 5) >= (8, 0)`` must not lock a MariaDB that still looks like MySQL."""

        def initialize(dialect: _Dialect) -> None:
            dialect.server_version_info = (10, 5)
            dialect.is_mariadb = True

        session = _Session(_Dialect("mysql", None), on_connect=initialize)
        repository = _repository(session)

        await repository.get_many()

        assert session.connected is True
        assert _query_locks(session.statement) is False

    async def test_unknown_version_after_connect_does_not_lock(self):
        session = _Session(_Dialect("mysql", None))
        repository = _repository(session)

        await repository.get_many()

        assert session.connected is True
        assert _query_locks(session.statement) is False

    async def test_postgresql_locks_without_waiting_for_a_version(self):
        session = _Session(_Dialect("postgresql", None))
        repository = _repository(session)

        await repository.get_many()

        assert session.connected is False
        assert _query_locks(session.statement) is True


class TestValueConversion:
    """Values must travel to and from the database in the dialect representation."""

    def test_uuid_binary_binds_bytes_for_mysql_and_uuid_for_postgresql(self):
        type_ = UUIDBinary()
        value = uuid.uuid4()

        assert type_.process_bind_param(value, dialect_by_name("mysql")) == value.bytes
        assert type_.process_bind_param(value, dialect_by_name("postgresql")) == value

    def test_uuid_binary_reads_uuid_back(self):
        type_ = UUIDBinary()
        value = uuid.uuid4()

        assert type_.process_result_value(value.bytes, dialect_by_name("mysql")) == value
        assert type_.process_result_value(value, dialect_by_name("postgresql")) == value

    def test_binary16_accepts_both_bytes_and_uuid(self):
        type_ = Binary16()
        value = uuid.uuid4()

        for dialect_name in ("mysql", "postgresql", "sqlite"):
            dialect = dialect_by_name(dialect_name)

            assert type_.process_bind_param(value, dialect) == value.bytes
            assert type_.process_bind_param(value.bytes, dialect) == value.bytes

    def test_none_is_passed_through(self):
        dialect = dialect_by_name("postgresql")

        assert UUIDBinary().process_bind_param(None, dialect) is None
        assert UUIDBinary().process_result_value(None, dialect) is None
        assert Binary16().process_bind_param(None, dialect) is None
