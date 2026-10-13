"""Declarative base shared by the optional SQLModel outbox and saga models."""

import typing

try:
    from sqlalchemy.ext.asyncio import AsyncSession as SqlAlchemyAsyncSession
    from sqlalchemy.orm import registry
    from sqlmodel import SQLModel
    from sqlmodel.ext.asyncio.session import AsyncSession
except ImportError:
    raise ImportError(
        "You are trying to use the SQLModel integration, "
        "but 'sqlmodel' is not installed. "
        "Please install it using: pip install python-cqrs[sqlmodel]",
    ) from None

cqrs_registry = registry()


class CqrsSQLModel(SQLModel, registry=cqrs_registry):
    """Base for library SQLModel tables.

    Tables live on this class's metadata, not on the application's
    ``SQLModel.metadata``. Call ``Base.metadata.create_all`` the same way as
    with the SQLAlchemy models, or rebind the outbox model onto your own base.
    """


class SqlAlchemyExecuteSession:
    """SQLModel session whose ``execute`` is the SQLAlchemy implementation.

    ``sqlmodel.AsyncSession.execute`` is deprecated in favor of ``exec`` and
    warns on every call. The outbox and saga storage use Core ``execute``
    because they need ``Result.rowcount`` and ``scalars()`` unchanged from the
    SQLAlchemy repositories.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def __getattr__(self, name: str) -> typing.Any:
        return getattr(self._session, name)

    async def execute(self, statement: typing.Any, params: typing.Any = None, **kwargs: typing.Any) -> typing.Any:
        return await SqlAlchemyAsyncSession.execute(self._session, statement, params, **kwargs)
