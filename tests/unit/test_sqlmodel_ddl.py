"""SQLModel models render the same DDL as the SQLAlchemy models."""

import importlib.util

import pytest
import sqlalchemy
from sqlalchemy.schema import CreateTable

from cqrs.outbox.sqlalchemy import OutboxModel as SaOutboxModel
from cqrs.outbox.sqlmodel import Base as SqlModelBase
from cqrs.outbox.sqlmodel import OutboxModel as SqlModelOutboxModel
from cqrs.saga.storage.sqlalchemy import SagaExecutionModel as SaSagaExecutionModel
from cqrs.saga.storage.sqlalchemy import SagaLogModel as SaSagaLogModel
from cqrs.saga.storage.sqlmodel import Base as SagaSqlModelBase
from cqrs.saga.storage.sqlmodel import SagaExecutionModel as SqlModelSagaExecutionModel
from cqrs.saga.storage.sqlmodel import SagaLogModel as SqlModelSagaLogModel

pytestmark = pytest.mark.sqlmodel


def _ddl(table: sqlalchemy.Table, dialect_name: str) -> str:
    dialect = sqlalchemy.create_mock_engine(f"{dialect_name}://", executor=None).dialect
    return str(CreateTable(table).compile(dialect=dialect))


@pytest.mark.parametrize("dialect_name", ["postgresql", "mysql", "sqlite"])
@pytest.mark.parametrize(
    ("sa_model", "sqlmodel_model"),
    [
        (SaOutboxModel, SqlModelOutboxModel),
        (SaSagaExecutionModel, SqlModelSagaExecutionModel),
        (SaSagaLogModel, SqlModelSagaLogModel),
    ],
)
def test_sqlmodel_ddl_matches_sqlalchemy(
    dialect_name: str,
    sa_model: type,
    sqlmodel_model: type,
) -> None:
    assert _ddl(sa_model.__table__, dialect_name) == _ddl(sqlmodel_model.__table__, dialect_name)


def test_sqlmodel_tables_share_one_metadata() -> None:
    assert SqlModelBase.metadata is SagaSqlModelBase.metadata
    names = set(SqlModelBase.metadata.tables)
    assert SqlModelOutboxModel.__tablename__ in names
    assert SqlModelSagaExecutionModel.__tablename__ in names
    assert SqlModelSagaLogModel.__tablename__ in names


def test_sqlmodel_metadata_is_not_the_default_sqlmodel_metadata() -> None:
    from sqlmodel import SQLModel

    assert SqlModelBase.metadata is not SQLModel.metadata
    assert SqlModelOutboxModel.__tablename__ not in SQLModel.metadata.tables


def test_import_error_message_mentions_extra() -> None:
    for module_name in (
        "cqrs.sqlmodel_base",
        "cqrs.outbox.sqlmodel",
        "cqrs.saga.storage.sqlmodel",
    ):
        spec = importlib.util.find_spec(module_name)
        assert spec is not None and spec.origin is not None
        text = open(spec.origin, encoding="utf-8").read()
        assert "python-cqrs[sqlmodel]" in text
