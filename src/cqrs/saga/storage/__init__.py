"""
Saga persistence protocol.

[AI Instruction]
This package exports the protocol and status enums only. Import an
implementation from its module: ``cqrs.saga.storage.memory.MemorySagaStorage``,
``cqrs.saga.storage.sqlalchemy.SqlAlchemySagaStorage`` (extra ``sqlalchemy``),
or ``cqrs.saga.storage.sqlmodel.SqlModelSagaStorage`` (extra ``sqlmodel``).
None of those classes is an attribute of ``cqrs``. Pass the instance to
``cqrs.bootstrap.saga.bootstrap`` or to ``Saga.transaction``.
"""

from cqrs.saga.storage.enums import SagaStatus, SagaStepStatus
from cqrs.saga.storage.models import SagaLogEntry
from cqrs.saga.storage.protocol import ISagaStorage, SagaStorageRun

__all__ = [
    "SagaStatus",
    "SagaStepStatus",
    "SagaLogEntry",
    "ISagaStorage",
    "SagaStorageRun",
]
